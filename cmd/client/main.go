package main

import (
	"log"
	"os"
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
	serverURL := os.Getenv("SERVER_URL")
	if serverURL == "" {
		serverURL = "ws://localhost:8080/ws"
	}

	// ДИНАМИКА: Каждый клиент может работать со своим уникальным ресурсом
	targetID := os.Getenv("TARGET_RESOURCE_ID")
	if targetID == "" {
		targetID = "1" // Дефолт
	}

	// Формируем динамический URL с параметром ?id=...
	finalURL := serverURL + "?id=" + targetID
	log.Printf("Подключение к серверу платформы: %s...", finalURL)

	c, _, err := websocket.DefaultDialer.Dial(finalURL, nil)
	if err != nil {
		log.Fatalf("Ошибка подключения клиента: %v", err)
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

	// Симулируем постоянную работу пользователя внутри Kubernetes
	go func() {
		newData := "Изменение от Умного Клиента"

		for {
			backoff := 500 * time.Millisecond // Сброс задержки для новой сессии

			log.Printf("[АПДЕЙТ] Отправляем данные. Наша версия: %d\n", localRes.Version)
			c.WriteJSON(Message{
				Action:   "update",
				Resource: Resource{ID: localRes.ID, Data: newData, Version: localRes.Version},
			})

			// Ожидаем сигналов из канала ретраев
			// Если сервер ответит "conflict", сработает этот внутренний цикл
			for {
				select {
				case <-retryChan:
					log.Printf("[RETRY] Запуск повторной попытки через %v...\n", backoff)
					time.Sleep(backoff)
					backoff *= 2 // Экспоненциальный рост паузы

					log.Printf("[RETRY] Отправка заново. Версия: %d\n", localRes.Version)
					c.WriteJSON(Message{
						Action:   "update",
						Resource: Resource{ID: localRes.ID, Data: newData, Version: localRes.Version},
					})
				case <-time.After(2 * time.Second):
					// Если в течение 2 секунд конфликтов не прилетало,
					// значит наше обновление зафиксировано успешно!
					goto NextIteration
				}
			}

		NextIteration:
			log.Println("[УСПЕХ] Данные сохранены. Отдыхаем 5 секунд перед следующим изменением...")
			time.Sleep(5 * time.Second)
		}
	}()

	// ИСПРАВЛЕНО: Вместо ожидания Ctrl+C используем вечный пустой канал,
	// чтобы под в Kubernetes работал бесконечно
	select {}
}
