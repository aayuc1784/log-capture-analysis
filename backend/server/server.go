package main

import (
    "fmt"
	"sync"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

const (
    logBufferSize=5000
    maxConcurrentLogs=20
)

var topics = []string {"auth", "server", "services", "database", "payment", "email"}

const kafkaBrokerHost = "localhost:9094"

type Log struct{
    Level string  `json:"level"`
    Message string `json:"message"`
    ResourceID string `json:"resourceId"`
    Timestamp time.Time `json:"timestamp"`
    TraceID string `json:"traceId"`
    SpanID string `json:"spanId"`
    Commit string `json:"commit"`
    Metadata map[string] interface{} `json:"metadata"`
}

type IngestionContext struct {
    esClient *elasticsearch.Client
    indexName string
    logChannel chan Log
    workerWaitGroup sync.WaitGroup
}

func setUpRoutes(ingestionContext *IngestionContext) *gin.Engine{
    router := gin.Default()

    config := cors.DefaultConfig()
    config.AllowOrigins = [] string{"*"}
    config.AllowMethods = [] string {"GET", "POST", "PATCH", "DELETE"}
    router.Use(cors.New(config))

    router.POST("/", func (c *gin.Context) { AddLog(c, ingestionContext)})

    router.GET("/logs-count", func (c *gin.Context) { CountLogs(c, ingestionContext)})

    router.POST("/search-logs")

    return router
}

func main(){

    elasticConfig := elasticsearch.Config{
        Addresses: []string {"http://localhost:9200"},
        Username: "elastic",
        Password: "elastic",
    }

    esClient, err := elasticsearch.NewClient(elasticConfig)

    if err != nil {
        fmt.Println("Error connecting in Elastic Search", err)
        return
    }

    ingestionContext := &IngestionContext{
        esClient: esClient,
        indexName: "log-ingestor",
        logChannel: make(chan Log, logBufferSize),
    }

    for i:=1; i <= maxConcurrentLogs; i++ {
        ingestionContext.workerWaitGroup.Add(1)
        go saveLogWorker(ingestionContext, i)
    }

    go KafkaConsumer(ingestionContext, topics)

    router := setUpRoutes(ingestionContext)

    fmt.Println("Starting server on port: 3000....")

    if err := router.Run(":3000"); err != nil {
        fmt.Println("Error staring server: ", err)
    }

    close(ingestionContext.logChannel)
    ingestionContext.workerWaitGroup.Wait()
}