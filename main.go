package main

import (
    "github.com/gin-gonic/gin"
    "fmt"
    "github.com/jakecoffman/cron"
    "log"
)

func main() {
    r := gin.Default()
    r.GET("/ping", func(c *gin.Context) {
        c.JSON(200, gin.H{
            "message": "pong",
        })
    })
    fmt.Println("gin run")

    i :=0
    c:=cron.New()
    spec := "*/3 * * * *"
    c.AddFunc(spec, func() {
        i++
        log.Println("running ", i)
    }, "one")
    c.Start()
    r.Run()

}
