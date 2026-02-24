package main

import (
	"flag"
	"fmt"
	"log"
	"net/url"
	"time"

	"github.com/gorilla/websocket"
)

func main() {
	keyword := flag.String("k", "Kafka", "Keyword to search")
	flag.Parse()

	u := url.URL{Scheme: "ws", Host: "localhost:8080", Path: "/search"}
	log.Printf("Connecting to %s", u.String())

	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatal("dial:", err)
	}
	defer c.Close()

	// 1. 发送查询请求
	log.Printf("Searching for: %s", *keyword)
	err = c.WriteMessage(websocket.TextMessage, []byte(*keyword))
	if err != nil {
		log.Fatal("write:", err)
	}

	// 2. 读取响应
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, message, err := c.ReadMessage()
		if err != nil {
			log.Println("read error:", err)
			return
		}
		fmt.Printf("\n[RESULT] >>> %s\n\n", message)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		log.Println("Timeout waiting for response")
	}
}
