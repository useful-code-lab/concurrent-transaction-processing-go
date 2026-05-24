package main

import (
	"log"
	"net/url"
	"os"
	"os/signal"
	"time"

	"github.com/gorilla/websocket"
)

type Resource struct {
	ID      string `json:"id"`
	Data    string `json:"data"`
	Version int    `json:"version"`
}

type Message struct {
	Action   string   `json:"action"`
	Resource Resource `json:"resource"`
	Error    string   `json:"error,omitempty"`
}

func main() {
	u := url.URL{Scheme: "ws", Host: "localhost:8080", Path: "/ws"}
	log.Printf("Подключение к серверу %s...", u.String())

	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatalf("Ошибка подключения: %v", err)
	}
	defer c.Close()

	// Локальная копия ресурса в памяти клиента
	var localRes Resource

	// Канал для отслеживания ошибок и ретраев
	retryChan := make(chan bool, 1)

	// Читающий поток: постоянно слушает сервер
	go func() {
		for {
			var msg Message
			err := c.ReadJSON(&msg)
			if err != nil {
				log.Println("Ошибка чтения от сервера:", err)
				return
			}

			switch msg.Action {
			case "read", "broadcast":
				localRes = msg.Resource
				log.Printf("[СИНХРОНИЗАЦИЯ] Получена актуальная версия: %d, Данные: %s\n", localRes.Version, localRes.Data)
			case "error":
				if msg.Error == "conflict" {
					log.Printf("[КОНФЛИКТ] Наша версия устарела! Актуальная версия на сервере: %d. Требуется Retry!\n", msg.Resource.Version)
					localRes = msg.Resource // Обновляем локальную версию до актуальной
					retryChan <- true       // Даем сигнал основному потоку сделать попытку еще раз
				}
			}
		}
	}()

	// Первым делом запрашиваем состояние ресурса
	c.WriteJSON(Message{Action: "read"})
	time.Sleep(500 * time.Millisecond) // Даем время на синхронизацию

	// Симулируем попытку конкурентного обновления с алгоритмом Retry
	go func() {
		newData := "Изменение от Умного Клиента"
		backoff := 500 * time.Millisecond // Стартовая задержка повтора

		log.Printf("[АПДЕЙТ] Пытаемся отправить данные с нашей версией: %d\n", localRes.Version)
		c.WriteJSON(Message{
			Action:   "update",
			Resource: Resource{ID: localRes.ID, Data: newData, Version: localRes.Version},
		})

		// Ожидаем: если прилетит конфликт, сработает этот цикл
		for range retryChan {
			log.Printf("[RETRY] Запуск повторной попытки через %v...\n", backoff)
			time.Sleep(backoff)
			
			// Экспоненциальное увеличение задержки (Exponential Backoff)
			backoff *= 2 

			log.Printf("[RETRY] Отправка данных заново с обновленной версией: %d\n", localRes.Version)
			c.WriteJSON(Message{
				Action:   "update",
				Resource: Resource{ID: localRes.ID, Data: newData, Version: localRes.Version},
			})
		}
	}()

	// Держим клиент запущенным
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	<-interrupt
}
