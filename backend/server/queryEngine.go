package main

import(
    "context"
    "fmt"
    "net/http"
    "encoding/json"
    "github.com/elastic/go-elasticsearch/esapi"
    "github.com/gin-gonic/gin"
)

func CountLogs(c* gin.Context, ingestionContext *IngestionContext) {
    countReq := esapi.CountRequest{
        Index: []string {ingestionContext.indexName},
    }
    res, err := countReq.Do(context.Background(), ingestionContext.esClient)

    if err != nil {
        fmt.Println("Error querying logs count from Elasticsearch:", err)
        c.JSON(http.StatusInternalServerError, gin.H{"error": "Internal Server Error"})
        return
    }

    defer res.Body.Close()

    if res.IsError() {
        fmt.Printf("Failed to get logs count from Elasticsearch. Response: %s\n", res.String())
        c.JSON(http.StatusInternalServerError, gin.H{"error": "Internal Server Error"})
        return
    }

    var countResponse map[string] interface {}
    if err := json.NewDecoder(res.Body).Decode(&countResponse); err != nil {
        fmt.Println("Error decoding Elasticsearch count response:", err)
        c.JSON(http.StatusInternalServerError, gin.H{"error": "Internal Server Error"})
        return
    }

    count, ok := countResponse["count"].(float64)

    if !ok {
    	fmt.Println("Error extracting count from Elasticsearch response")
    	c.JSON(http.StatusInternalServerError, gin.H{"error": "Internal Server Error"})
    	return
    }

    c.JSON(http.StatusOK, gin.H{"count": int(count)})
}