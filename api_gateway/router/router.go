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
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	jsoniter "github.com/json-iterator/go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/protobuf/proto"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

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
)

func init() {
	// 防止重复注册 Panic
	// 在实际项目中，Metrics 注册通常放在独立的 metrics 包 init 中
	// 这里简单处理，先不重复注册，或者假设只引用一次
	prometheus.Register(requestCounter)
	prometheus.Register(requestDuration)
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
func NewRouter() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()

	r.Use(PrometheusMiddleware())
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	r.GET("/article/:id", getArticleDetail)
	r.POST("/add_doc", addDocHandler)
	r.GET("/search", searchWebSocketHandler)
	r.GET("/search_http", searchHTTPHandler)

	return r
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
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		return
	}
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
			util.LogError("GetSearchCache error: %v", err)
		}

		if hit {
			jsonBytes, _ := json.Marshal(&cachedRes)
			if err := conn.WriteMessage(messageType, jsonBytes); err != nil {
				break
			}
			continue
		}

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
			conn.WriteMessage(messageType, []byte("Error: "+err.Error()))
			continue
		}

		jsonBytes, _ := json.Marshal(res)
		if err := conn.WriteMessage(messageType, jsonBytes); err != nil {
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
		util.LogError("GetSearchCache error: %v", err)
	}

	if hit {
		c.JSON(http.StatusOK, &cachedRes)
		return
	}

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
