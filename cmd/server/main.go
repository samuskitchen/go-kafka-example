package main

import (
    "context"
    "encoding/json"
    "fmt"
    "net/http"
    "os"
    "strings"

    "github.com/gin-gonic/gin"
    _ "github.com/joho/godotenv/autoload"

    "github.com/samuskitchen/go-kafka-example/pkg"
    "github.com/samuskitchen/go-kafka-example/pkg/kafka"
)

type Request struct {
    Username string `json:"username"`
    Message  string `json:"message"`
}

var (
    brokers = os.Getenv("KAFKA_BROKERS")
    topic   = os.Getenv("KAFKA_TOPIC")
)

func main() {

    publisher := kafka.NewPublisher(strings.Split(brokers, ","), topic)

    r := gin.Default()
    r.POST("/join", joinHandler(publisher))
    r.POST("/publish", publishHandler(publisher))
    r.POST("/leave", leaveHandler(publisher))

    _ = r.Run()
}

func joinHandler(publisher pkg.Publisher) func(*gin.Context) {
    return func(c *gin.Context) {
        var req Request
        err := json.NewDecoder(c.Request.Body).Decode(&req)
        if err != nil {
            c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": err})
        }

        message := pkg.NewSystemMessage(fmt.Sprintf("%s has joined the room!", req.Username))

        if err := publisher.Publish(context.Background(), message); err != nil {
            c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": err})
        }

        c.JSON(http.StatusAccepted, gin.H{"message": "message published"})
    }
}

func publishHandler(publisher pkg.Publisher) func(*gin.Context) {
    return func(c *gin.Context) {
        var req Request
        err := json.NewDecoder(c.Request.Body).Decode(&req)
        if err != nil {
            c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": err})
        }

        message := pkg.NewMessage(req.Username, req.Message)

        if err := publisher.Publish(context.Background(), message); err != nil {
            c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": err})
        }

        c.JSON(http.StatusAccepted, gin.H{"message": "message published"})
    }
}

func leaveHandler(publisher pkg.Publisher) func(*gin.Context) {
    return func(c *gin.Context) {
        var req Request
        err := json.NewDecoder(c.Request.Body).Decode(&req)
        if err != nil {
            c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": err})
        }

        message := pkg.LeaveMessage(req.Username, req.Message)

        if err := publisher.Publish(context.Background(), message); err != nil {
            c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": err})
        }

        c.JSON(http.StatusAccepted, gin.H{"message": "message published"})
    }
}