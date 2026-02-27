package util

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type LogLevel int

const (
	DebugLevel LogLevel = iota
	InfoLevel
	WarnLevel
	ErrorLevel
)

type LogFields map[string]interface{}

type logEvent struct {
	Timestamp string                 `json:"ts"`
	Level     string                 `json:"level"`
	Service   string                 `json:"service,omitempty"`
	Caller    string                 `json:"caller,omitempty"`
	Message   string                 `json:"msg"`
	Fields    map[string]interface{} `json:"fields,omitempty"`
}

type asyncLogger struct {
	queue         chan *logEvent
	stop          chan struct{}
	done          chan struct{}
	flushInterval time.Duration
	batchSize     int

	fileMu sync.Mutex
	file   *os.File
	writer *bufio.Writer
}

const (
	defaultQueueSize     = 8192
	defaultBatchSize     = 256
	defaultFlushInterval = 100 * time.Millisecond
	defaultServiceName   = "radic"
)

var (
	currentLevel  atomic.Int32
	consoleLevel  atomic.Int32
	droppedEvents atomic.Uint64
	serviceName   atomic.Value // string

	loggerOnce   sync.Once
	loggerState  *asyncLogger
	loggerClosed atomic.Bool
)

func init() {
	currentLevel.Store(int32(InfoLevel))
	consoleLevel.Store(int32(WarnLevel))
	serviceName.Store(defaultServiceName)

	if level, ok := parseLogLevel(os.Getenv("RADIC_LOG_LEVEL")); ok {
		currentLevel.Store(int32(level))
	} else if level, ok := parseLogLevel(os.Getenv("LOG_LEVEL")); ok {
		currentLevel.Store(int32(level))
	}
	if level, ok := parseLogLevel(os.Getenv("RADIC_CONSOLE_LEVEL")); ok {
		consoleLevel.Store(int32(level))
	}
	startLogger()
}

func parseLogLevel(raw string) (LogLevel, bool) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "debug":
		return DebugLevel, true
	case "info", "":
		return InfoLevel, true
	case "warn", "warning":
		return WarnLevel, true
	case "error":
		return ErrorLevel, true
	default:
		return InfoLevel, false
	}
}

func parsePositiveInt(raw string, fallback int) int {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

func startLogger() {
	loggerOnce.Do(func() {
		queueSize := parsePositiveInt(os.Getenv("RADIC_LOG_QUEUE_SIZE"), defaultQueueSize)
		batchSize := parsePositiveInt(os.Getenv("RADIC_LOG_BATCH_SIZE"), defaultBatchSize)
		flushMs := parsePositiveInt(os.Getenv("RADIC_LOG_FLUSH_MS"), int(defaultFlushInterval/time.Millisecond))
		flushInterval := time.Duration(flushMs) * time.Millisecond

		loggerState = &asyncLogger{
			queue:         make(chan *logEvent, queueSize),
			stop:          make(chan struct{}),
			done:          make(chan struct{}),
			flushInterval: flushInterval,
			batchSize:     batchSize,
		}
		go loggerState.run()
	})
}

func SetServiceName(name string) {
	name = strings.TrimSpace(name)
	if name == "" {
		return
	}
	serviceName.Store(name)
}

func SetLogLevel(level LogLevel) {
	currentLevel.Store(int32(level))
}

func InitLogger(logPath string) error {
	startLogger()

	if loggerClosed.Load() {
		return nil
	}

	if level, ok := parseLogLevel(os.Getenv("RADIC_LOG_LEVEL")); ok {
		currentLevel.Store(int32(level))
	} else if level, ok := parseLogLevel(os.Getenv("LOG_LEVEL")); ok {
		currentLevel.Store(int32(level))
	}
	if level, ok := parseLogLevel(os.Getenv("RADIC_CONSOLE_LEVEL")); ok {
		consoleLevel.Store(int32(level))
	}

	var nextFile *os.File
	var err error
	if strings.TrimSpace(logPath) != "" {
		if dir := filepath.Dir(logPath); dir != "" && dir != "." {
			if mkErr := os.MkdirAll(dir, 0o755); mkErr != nil {
				return fmt.Errorf("failed to create log dir: %w", mkErr)
			}
		}
		nextFile, err = os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o666)
		if err != nil {
			return fmt.Errorf("failed to open log file: %w", err)
		}
	}

	state := loggerState
	if state == nil {
		if nextFile != nil {
			_ = nextFile.Close()
		}
		return nil
	}
	state.fileMu.Lock()
	oldFile := state.file
	state.file = nextFile
	if nextFile != nil {
		state.writer = bufio.NewWriterSize(nextFile, 256*1024)
	} else {
		state.writer = nil
	}
	state.fileMu.Unlock()
	if oldFile != nil {
		_ = oldFile.Close()
	}
	return nil
}

func CloseLogger() {
	state := loggerState
	if state == nil {
		return
	}
	if loggerClosed.CompareAndSwap(false, true) {
		close(state.stop)
		<-state.done
	}
	state.fileMu.Lock()
	if state.writer != nil {
		_ = state.writer.Flush()
	}
	if state.file != nil {
		_ = state.file.Close()
	}
	state.writer = nil
	state.file = nil
	state.fileMu.Unlock()
}

func (l *asyncLogger) run() {
	defer close(l.done)
	ticker := time.NewTicker(l.flushInterval)
	defer ticker.Stop()

	batch := make([]*logEvent, 0, l.batchSize)
	flushBatch := func() {
		if len(batch) == 0 {
			return
		}
		for _, event := range batch {
			l.writeEvent(event)
		}
		l.flushFile()
		batch = batch[:0]
	}

	for {
		select {
		case event := <-l.queue:
			if event == nil {
				continue
			}
			batch = append(batch, event)
			if len(batch) >= l.batchSize {
				flushBatch()
			}
		case <-ticker.C:
			flushBatch()
		case <-l.stop:
			for {
				select {
				case event := <-l.queue:
					if event != nil {
						batch = append(batch, event)
					}
				default:
					flushBatch()
					return
				}
			}
		}
	}
}

func (l *asyncLogger) flushFile() {
	l.fileMu.Lock()
	defer l.fileMu.Unlock()
	if l.writer != nil {
		_ = l.writer.Flush()
	}
}

func (l *asyncLogger) writeEvent(event *logEvent) {
	if event == nil {
		return
	}
	line, err := json.Marshal(event)
	if err != nil {
		line = []byte(fmt.Sprintf(`{"ts":"%s","level":"error","msg":"marshal log event failed","fields":{"error":%q}}`,
			time.Now().Format(time.RFC3339Nano), err.Error()))
	}
	line = append(line, '\n')

	if logLevelFromString(event.Level) >= LogLevel(consoleLevel.Load()) {
		if event.Level == "error" || event.Level == "warn" {
			_, _ = os.Stderr.Write(line)
		} else {
			_, _ = os.Stdout.Write(line)
		}
	}

	l.fileMu.Lock()
	if l.writer != nil {
		_, _ = l.writer.Write(line)
	}
	l.fileMu.Unlock()
}

func copyFields(fields LogFields) map[string]interface{} {
	if len(fields) == 0 {
		return nil
	}
	cloned := make(map[string]interface{}, len(fields))
	for key, value := range fields {
		cloned[key] = value
	}
	return cloned
}

func logLevelString(level LogLevel) string {
	switch level {
	case DebugLevel:
		return "debug"
	case InfoLevel:
		return "info"
	case WarnLevel:
		return "warn"
	case ErrorLevel:
		return "error"
	default:
		return "info"
	}
}

func logLevelFromString(level string) LogLevel {
	switch strings.ToLower(level) {
	case "debug":
		return DebugLevel
	case "info":
		return InfoLevel
	case "warn", "warning":
		return WarnLevel
	case "error":
		return ErrorLevel
	default:
		return InfoLevel
	}
}

func enqueueEvent(level LogLevel, event *logEvent) {
	state := loggerState
	if state == nil || event == nil {
		return
	}
	if loggerClosed.Load() {
		return
	}

	if level >= ErrorLevel {
		select {
		case state.queue <- event:
			return
		default:
		}
		timer := time.NewTimer(20 * time.Millisecond)
		defer timer.Stop()
		select {
		case state.queue <- event:
		case <-timer.C:
			droppedEvents.Add(1)
			state.writeEvent(event)
		}
		return
	}

	select {
	case state.queue <- event:
	default:
		droppedEvents.Add(1)
	}
}

func buildEvent(level LogLevel, message string, fields LogFields, callerSkip int) *logEvent {
	_, file, line, ok := runtime.Caller(callerSkip)
	caller := "?:0"
	if ok {
		caller = fmt.Sprintf("%s:%d", filepath.Base(file), line)
	}
	service := defaultServiceName
	if value, ok := serviceName.Load().(string); ok && strings.TrimSpace(value) != "" {
		service = value
	}
	return &logEvent{
		Timestamp: time.Now().Format(time.RFC3339Nano),
		Level:     logLevelString(level),
		Service:   service,
		Caller:    caller,
		Message:   message,
		Fields:    copyFields(fields),
	}
}

func logWithFields(level LogLevel, message string, fields LogFields, callerSkip int) {
	if level < LogLevel(currentLevel.Load()) {
		return
	}
	enqueueEvent(level, buildEvent(level, message, fields, callerSkip))
}

func logPrint(level LogLevel, format string, args ...interface{}) {
	logWithFields(level, fmt.Sprintf(format, args...), nil, 3)
}

func LogDebugWithFields(message string, fields LogFields) {
	logWithFields(DebugLevel, message, fields, 2)
}

func LogInfoWithFields(message string, fields LogFields) {
	logWithFields(InfoLevel, message, fields, 2)
}

func LogWarnWithFields(message string, fields LogFields) {
	logWithFields(WarnLevel, message, fields, 2)
}

func LogErrorWithFields(message string, fields LogFields) {
	logWithFields(ErrorLevel, message, fields, 2)
}

func LogDebug(format string, args ...interface{}) {
	logPrint(DebugLevel, format, args...)
}

func LogInfo(format string, args ...interface{}) {
	logPrint(InfoLevel, format, args...)
}

func LogWarn(format string, args ...interface{}) {
	logPrint(WarnLevel, format, args...)
}

func LogError(format string, args ...interface{}) {
	logPrint(ErrorLevel, format, args...)
}
