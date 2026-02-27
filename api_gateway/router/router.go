package router

import (
	"context"
	"my_radic/api_gateway/config"
	"my_radic/api_gateway/infra"
	"my_radic/api_gateway/model"
	"my_radic/api_gateway/response"
	"my_radic/api_gateway/rpc"
	"my_radic/index_service"
	"my_radic/types"
	"my_radic/util"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"sync/atomic"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	jsoniter "github.com/json-iterator/go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/protobuf/proto"
)

var ready atomic.Bool
var accessLogSeq atomic.Uint64

var json = jsoniter.ConfigCompatibleWithStandardLibrary

var (
	accessLogSampleEvery  = envPositiveInt("RADIC_ACCESS_LOG_SAMPLE_EVERY", 100)
	accessLogSlowMs       = envPositiveInt("RADIC_ACCESS_LOG_SLOW_MS", 500)
	accessLogQueryMaxLen  = envPositiveInt("RADIC_ACCESS_LOG_QUERY_MAX_LEN", 128)
	accessLogIncludeQuery = envBool("RADIC_ACCESS_LOG_INCLUDE_QUERY", false)
)

var (
	requestCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests.",
		},
		[]string{"method", "path", "status"},
	)
	requestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request latency distributions.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "path"},
	)
	searchCacheHitTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "search_cache_hit_total",
			Help: "Total number of search cache hits.",
		},
	)
	searchCacheMissTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "search_cache_miss_total",
			Help: "Total number of search cache misses.",
		},
	)
	searchCacheErrorTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "search_cache_error_total",
			Help: "Total number of search cache errors.",
		},
	)
)

func init() {
	for _, collector := range []prometheus.Collector{
		requestCounter,
		requestDuration,
		searchCacheHitTotal,
		searchCacheMissTotal,
		searchCacheErrorTotal,
	} {
		if err := prometheus.Register(collector); err != nil {
			if _, ok := err.(prometheus.AlreadyRegisteredError); ok {
				continue
			}
			util.LogError("prometheus.Register failed: %v", err)
		}
	}
}

func PrometheusMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		duration := time.Since(start).Seconds()

		status := strconv.Itoa(c.Writer.Status())
		path := c.FullPath()
		if path == "" {
			path = "unknown"
		}

		requestCounter.WithLabelValues(c.Request.Method, path, status).Inc()
		requestDuration.WithLabelValues(c.Request.Method, path).Observe(duration)
	}
}

// NewRouter 构造并返回 API Gateway 的 Gin 引擎
func envPositiveInt(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func envBool(key string, fallback bool) bool {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	switch strings.ToLower(raw) {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		return fallback
	}
}

func shouldSampleAccessLog(status int, latency time.Duration) bool {
	if status >= http.StatusBadRequest {
		return true
	}
	if latency >= time.Duration(accessLogSlowMs)*time.Millisecond {
		return true
	}
	if accessLogSampleEvery <= 1 {
		return true
	}
	return accessLogSeq.Add(1)%uint64(accessLogSampleEvery) == 0
}

func AccessLogMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		rawPath := c.Request.URL.Path
		if rawPath == "/healthz" || rawPath == "/readyz" || rawPath == "/metrics" {
			c.Next()
			return
		}

		start := time.Now()
		c.Next()
		latency := time.Since(start)
		status := c.Writer.Status()
		if !shouldSampleAccessLog(status, latency) {
			return
		}

		path := c.FullPath()
		if path == "" {
			path = rawPath
		}

		fields := util.LogFields{
			"method":     c.Request.Method,
			"path":       path,
			"status":     status,
			"latency_ms": float64(latency.Microseconds()) / 1000.0,
			"client_ip":  c.ClientIP(),
		}
		if accessLogIncludeQuery {
			if query := c.Request.URL.RawQuery; query != "" {
				if len(query) > accessLogQueryMaxLen {
					query = query[:accessLogQueryMaxLen] + "..."
				}
				fields["query"] = query
			}
		}

		switch {
		case status >= http.StatusInternalServerError:
			util.LogErrorWithFields("http_access", fields)
		case status >= http.StatusBadRequest || latency >= time.Duration(accessLogSlowMs)*time.Millisecond:
			util.LogWarnWithFields("http_access", fields)
		default:
			util.LogInfoWithFields("http_access", fields)
		}
	}
}

func NewRouter() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()

	r.Use(gin.Recovery(), PrometheusMiddleware(), AccessLogMiddleware())
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	r.GET("/article/:id", getArticleDetail)
	r.POST("/add_doc", addDocHandler)
	r.GET("/search", searchWebSocketHandler)
	r.GET("/search_http", searchHTTPHandler)
	r.GET("/healthz", func(c *gin.Context) {
		c.Status(200)
	})
	r.GET("/readyz", func(c *gin.Context) {
		if IsReady() {
			c.Status(200)
			return
		}
		c.Status(503)
	})
	return r
}

func SetReady(v bool) {
	ready.Store(v)
}

func IsReady() bool {
	return ready.Load()
}

type AddDocRequest struct {
	Title   string `json:"title" binding:"required"`
	Content string `json:"content" binding:"required"`
	Url     string `json:"url"`
}

func getArticleDetail(c *gin.Context) {
	idStr := c.Param("id")
	id, _ := strconv.ParseUint(idStr, 10, 64)

	var article model.Article
	if err := infra.DB.First(&article, id).Error; err != nil {
		response.Fail(c, http.StatusNotFound, "Article not found")
		return
	}
	response.OK(c, article)
}

func addDocHandler(c *gin.Context) {
	var req AddDocRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Fail(c, http.StatusBadRequest, err.Error())
		return
	}

	tx := infra.DB.Begin()
	if tx.Error != nil {
		response.Fail(c, http.StatusInternalServerError, "Database transaction start failed")
		return
	}

	article := model.Article{
		Title:   req.Title,
		Content: req.Content,
		Url:     req.Url,
	}

	if err := tx.Create(&article).Error; err != nil {
		tx.Rollback()
		util.LogError("Failed to write to MySQL: %v", err)
		response.Fail(c, http.StatusInternalServerError, "Database error")
		return
	}

	doc := &types.Document{
		Id:      strconv.FormatUint(article.ID, 10),
		IntId:   article.ID,
		Content: article.Content,
		Title:   article.Title,
		Url:     article.Url,
	}
	docBytes, err := proto.Marshal(doc)
	if err != nil {
		tx.Rollback()
		response.Fail(c, http.StatusInternalServerError, err.Error())
		return
	}

	outboxMsg := model.Outbox{
		Topic:   config.Conf.Kafka.Topic,
		Payload: docBytes,
		Status:  0,
	}
	if err := tx.Create(&outboxMsg).Error; err != nil {
		tx.Rollback()
		util.LogError("Failed to write Outbox: %v", err)
		response.Fail(c, http.StatusInternalServerError, "Outbox error")
		return
	}

	if err := tx.Commit().Error; err != nil {
		util.LogError("Failed to commit transaction: %v", err)
		response.Fail(c, http.StatusInternalServerError, "Transaction commit failed")
		return
	}

	response.OK(c, gin.H{"id": article.ID})
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func searchWebSocketHandler(c *gin.Context) {
	if !IsReady() {
		c.Status(http.StatusServiceUnavailable)
		return
	}
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		return
	}
	wc := NewWsConn(conn)
	DefaultWSHub().Add(wc)
	defer DefaultWSHub().Remove(wc)
	defer conn.Close()

	for {
		messageType, p, err := conn.ReadMessage()
		if err != nil {
			break
		}

		query := strings.ToLower(strings.TrimSpace(string(p)))
		if query == "" {
			continue
		}

		cacheKey := "search:" + query
		var cachedRes index_service.SearchResponse
		hit, err := infra.GetSearchCache(context.Background(), cacheKey, &cachedRes)
		if err != nil {
			searchCacheErrorTotal.Inc()
			util.LogError("GetSearchCache error: %v", err)
		}

		if hit {
			searchCacheHitTotal.Inc()
			jsonBytes, _ := json.Marshal(&cachedRes)
			wc.writeMu.Lock()
			err := wc.Conn.WriteMessage(messageType, jsonBytes)
			wc.writeMu.Unlock()
			if err != nil {
				break
			}
			continue
		}
		searchCacheMissTotal.Inc()

		rpcReq := &index_service.SearchRequest{
			Query: &types.TermQuery{
				Should: []*types.TermQuery{
					{Keyword: &types.Keyword{Field: "title", Word: query}},
					{Keyword: &types.Keyword{Field: "content", Word: query}},
				},
			},
		}

		res, err := rpc.Client.Search(context.Background(), rpcReq)
		if err != nil {
			wc.writeMu.Lock()
			_ = wc.Conn.WriteMessage(messageType, []byte("Error: "+err.Error()))
			wc.writeMu.Unlock()
			continue
		}

		jsonBytes, _ := json.Marshal(res)
		wc.writeMu.Lock()
		err = wc.Conn.WriteMessage(messageType, jsonBytes)
		wc.writeMu.Unlock()
		if err != nil {
			break
		}

		go func() {
			if err := infra.SetSearchCache(context.Background(), cacheKey, res, time.Minute); err != nil {
				util.LogError("SetSearchCache error: %v", err)
			}
		}()
	}
}

func searchHTTPHandler(c *gin.Context) {
	query := c.Query("q")
	if query == "" {
		c.Status(http.StatusBadRequest)
		return
	}
	query = strings.ToLower(strings.TrimSpace(query))

	cacheKey := "search:" + query
	var cachedRes index_service.SearchResponse
	hit, err := infra.GetSearchCache(c, cacheKey, &cachedRes)
	if err != nil {
		searchCacheErrorTotal.Inc()
		util.LogError("GetSearchCache error: %v", err)
	}

	if hit {
		searchCacheHitTotal.Inc()
		c.JSON(http.StatusOK, &cachedRes)
		return
	}
	searchCacheMissTotal.Inc()

	rpcReq := &index_service.SearchRequest{
		Query: &types.TermQuery{
			Should: []*types.TermQuery{
				{Keyword: &types.Keyword{Field: "title", Word: query}},
				{Keyword: &types.Keyword{Field: "content", Word: query}},
			},
		},
	}

	res, err := rpc.Client.Search(c, rpcReq)
	if err != nil {
		c.String(http.StatusInternalServerError, "Error: "+err.Error())
		return
	}

	go func() {
		if err := infra.SetSearchCache(context.Background(), cacheKey, res, time.Minute); err != nil {
			util.LogError("SetSearchCache error: %v", err)
		}
	}()

	c.JSON(http.StatusOK, res)
}
