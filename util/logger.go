package util

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

// 日志级别定义
type LogLevel int

const (
	DebugLevel LogLevel = iota
	InfoLevel
	WarnLevel
	ErrorLevel
)

// ANSI 颜色代码
const (
	colorRed     = "\033[31m"
	colorYellow  = "\033[33m"
	colorBlue    = "\033[34m"
	colorMagenta = "\033[35m"
	colorReset   = "\033[0m"
)

var (
	logFile *os.File
)

// InitLogger 初始化日志系统
// logPath: 日志文件路径。如果为空，则只输出到控制台。
func InitLogger(logPath string) error {
	if logPath == "" {
		return nil
	}

	// 打开文件，追加模式 | 创建 | 只写
	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return fmt.Errorf("failed to open log file: %w", err)
	}

	logFile = file
	return nil
}

// CloseLogger 关闭日志文件资源
func CloseLogger() {
	if logFile != nil {
		logFile.Close()
	}
}

// logPrint 统一处理日志输出
// 实现了“双写”逻辑：控制台输出彩色日志，文件输出纯文本日志
func logPrint(level LogLevel, levelStr string, color string, format string, args ...interface{}) {
	// 获取调用位置。skip=2 是为了跳过 logPrint 和外部包装函数(如 LogInfo)
	_, file, line, ok := runtime.Caller(2)
	fileName := "???"
	if ok {
		fileName = filepath.Base(file)
	}

	// 时间戳
	timestamp := time.Now().Format("2006-01-02 15:04:05")

	// 格式化消息内容
	msg := fmt.Sprintf(format, args...)

	// 1. 控制台输出 (带颜色)
	// 格式: [颜色][时间] [级别] [文件名:行号] 消息[重置颜色]
	fmt.Printf("%s[%s] [%s] [%s:%d] %s%s\n",
		color, timestamp, levelStr, fileName, line, msg, colorReset)

	// 2. 文件输出 (纯文本，无颜色码)
	if logFile != nil {
		// 格式: [时间] [级别] [文件名:行号] 消息
		plainMsg := fmt.Sprintf("[%s] [%s] [%s:%d] %s\n",
			timestamp, levelStr, fileName, line, msg)
		logFile.WriteString(plainMsg)
	}
}

// LogDebug 打印调试信息 (紫色)
func LogDebug(format string, args ...interface{}) {
	logPrint(DebugLevel, "DEBUG", colorMagenta, format, args...)
}

// LogInfo 打印普通信息 (蓝色)
func LogInfo(format string, args ...interface{}) {
	logPrint(InfoLevel, "INFO ", colorBlue, format, args...)
}

// LogWarn 打印警告信息 (黄色)
func LogWarn(format string, args ...interface{}) {
	logPrint(WarnLevel, "WARN ", colorYellow, format, args...)
}

// LogError 打印错误信息 (红色)
func LogError(format string, args ...interface{}) {
	logPrint(ErrorLevel, "ERROR", colorRed, format, args...)
}