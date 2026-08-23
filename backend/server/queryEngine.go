package main

import(
    "context"
    "fmt"
    "net/http"
    "encoding/json"
    "strings"
    "time"
    "github.com/elastic/go-elasticsearch/esapi"
    "github.com/gin-gonic/gin"
)

type SearchParameters struct {
    Text        string              `json:"text"`
    RegexText   string              `json:"regexText"`
    Filters     [] FilterCondition  `json:"filters"`
    TimeRange   TimeRange          `json:"timeRange"`
}

type FilterCondition struct {
    ColumnName   string    `json:"columnName"`
    FilterValues []string  `json:"filterValues"`
}

type TimeRange struct {
    StartTime  time.Time        `json:startTime`
    EndTime    time.Time        `json:endTime`
}

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

func SearchLogs(c *gin.Context, ingestionContext *IngestionContext) {

    var searchParams SearchParameters

    if err := c.BindJSON(&searchParams); err != nil {
        c.JSON(http.StatusBadRequest, gin.H{"error": "Bad Request"})
        return
    }

    var query string

    if searchParams.Text != "" {
        query = fmt.Sprintf("`%s`", searchParams.Text)
    }

    if searchParams.RegexText != "" {
        if query != "" {
            query += " AND "
        }
        query += fmt.Sprintf("`/.*%s.*/`", searchParams.RegexText)
    }

    for _, filters := range searchParams.Filters {
        if filters.ColumnName != "" && len(filters.FilterValues) > 0 {
            var columnQuery string
            for _,filterValue := range filters.FilterValues {
                if columnQuery == "" {
                    columnQuery = fmt.Sprintf("(%s: '%s'", filters.ColumnName, filterValue)
                } else {
                    columnQuery = fmt.Sprintf("%s OR %s: '%s'", columnQuery, filters.ColumnName, filterValue)
                }
            }
            columnQuery = fmt.Sprintf("%s)", columnQuery)
            if query == "" {
                query = columnQuery
            } else {
                query = fmt.Sprintf("%s AND (%s)", query, columnQuery)
            }
        }
    }

    if !searchParams.TimeRange.StartTime.IsZero() || !searchParams.TimeRange.EndTime.IsZero() {
    		var timeQuery string

    		if !searchParams.TimeRange.StartTime.IsZero() && !searchParams.TimeRange.EndTime.IsZero() {
    			timeQuery = fmt.Sprintf("timestamp:[%s TO %s]", searchParams.TimeRange.StartTime.Format(time.RFC3339), searchParams.TimeRange.EndTime.Format(time.RFC3339))
    		} else if !searchParams.TimeRange.StartTime.IsZero() {
    			timeQuery = fmt.Sprintf("timestamp:[%s TO *]", searchParams.TimeRange.StartTime.Format(time.RFC3339))
    		} else {
    			timeQuery = fmt.Sprintf("timestamp:[* TO %s]", searchParams.TimeRange.EndTime.Format(time.RFC3339))
    		}

    		if query == "" {
    			query = timeQuery
    		} else {
    			query = fmt.Sprintf("%s AND %s", query, timeQuery)
    		}
    	}

    if query == "" {
        query = "*:*"
    }

    fmt.Printf("Querying via SQL Syntax : \n Query : %s \n", query)

    searchReq := esapi.SearchRequest {
        Index: []string {ingestionContext.indexName},
        Body: strings.NewReader(fmt.Sprintf(`{"query": {"query_string": {"query" : "%s"}}, "size":5000, "from":0}`, query)),
    }

    res, err := searchReq.Do(context.Background(), ingestionContext.esClient)

    if err != nil {
        fmt.Println("Error querying logs from Elasticsearch", err)
        c.JSON(http.StatusInternalServerError, gin.H{"error" : "Internal Server Error"})
        return
    }
    defer res.Body.Close()

    if res.IsError()  {
        fmt.Printf("Failed to get logs from Elasticsearch. Response: %s\n", res.String())
        c.JSON(http.StatusInternalServerError, gin.H{"error": "Internal Server Error"})
        return
    }

    var searchRes map[string] interface {}
    if err := json.NewDecoder(res.Body).Decode(&searchRes); err != nil {
        fmt.Println("Error decoding Elasticsearch search response:", err)
        c.JSON(http.StatusInternalServerError, gin.H{"error": "Internal Server Error"})
        return
    }

    c.JSON(http.StatusOK, searchRes)
}