package main

import (
    "context"
    "fmt"
    "strings"
    "encoding/json"
    "net/http"
    "github.com/elastic/go-elasticsearch/esapi"
    "github.com/gin-gonic/gin"
)

const batchSize = 10

func AddLog(c* gin.Context, ingestionContext * IngestionContext) {
    var log Log

    if err := c.BindJSON(&log); err != nil {
        c.JSON(http.StatusBadRequest, gin.H {"error" : "Bad Request"})
    return
    }

    ingestionContext.logChannel <- log

    response := gin.H {
        "status" : "Success",
    }
    c.JSON(http.StatusAccepted, response)
}

func saveLogWorker(ingestionContext *IngestionContext, workerId int) {
    defer ingestionContext.workerWaitGroup.Done()

    var logsInsert [] Log

    for log := range ingestionContext.logChannel {
        logsInsert = append(logsInsert, log)

        if len(logsInsert) == batchSize || len(ingestionContext.logChannel) == 0 {
            fmt.Printf("Worker %d processing logs: %v\n\n", workerId, logsInsert)

            var bulkRequest strings.Builder
            for _,log := range logsInsert {
                indexMetadata := fmt.Sprintf(`{"index": {"_index": "%s"}}`, ingestionContext.indexName)
                logJSON, err := json.Marshal(log)

                if err != nil {
                	fmt.Println("Error encoding log to JSON:", err)
                	continue
                }

                bulkRequest.WriteString(indexMetadata)
                bulkRequest.WriteString("\n")
                bulkRequest.Write(logJSON)
                bulkRequest.WriteString("\n")
            }

            req := esapi.BulkRequest{
                Body: strings.NewReader(bulkRequest.String()),
                Refresh : "true",
            }

            res, err := req.Do(context.Background(), ingestionContext.esClient)
            if err != nil {
                fmt.Println("Error indexing logs into Elasticsearch:", err)
                logsInsert = nil
                continue
            }
            defer res.Body.Close()

            if res.IsError() {
                fmt.Printf("Failed to index logs into Elasticsearch. Response: %s\n", res.String())
            	logsInsert = nil
            	continue
        	}
        logsInsert = nil
        }
    }
}