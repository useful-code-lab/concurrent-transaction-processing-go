package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	amqp "github.com/rabbitmq/amqp091-go"
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

var (
	upgrader  = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	clients   = make(map[*websocket.Conn]bool)
	clientsMu sync.Mutex

	rabbitChan *amqp.Channel
	queueName  = "resource_updates_queue"
)

func main() {
	rabbitURL := os.Getenv("RABBITMQ_URL")
	if rabbitURL == "" {
		rabbitURL = "amqp://guest:guest@localhost:5672/"
	}

	// Подключение к RabbitMQ с Retry логикой
	var conn *amqp.Connection
	var err error
	for i := 1; i <= 10; i++ {
		log.Printf("Попытка подключения к RabbitMQ %d/10...\n", i)
		conn, err = amqp.Dial(rabbitURL)
		if err == nil {
			log.Println("Успешно подключено к RabbitMQ!")
			break
		}
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		log.Fatalf("Ошибка подключения к RabbitMQ: %v", err)
	}
	defer conn.Close()

	rabbitChan, err = conn.Channel()
	if err != nil {
		log.Fatalf("Не удалось открыть канал в RabbitMQ: %v", err)
	}
	defer rabbitChan.Close()

	// Объявляем DURABLE (устойчивую) очередь (Она не пропадет при падении брокера)
	_, err = rabbitChan.QueueDeclare(
		queueName,
		true,  // durable: сохранять на диск!
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		log.Fatalf("Не удалось объявить очередь: %v", err)
	}

	// Внутри функции main() после объявления первой очереди добавьте:
	_, err = rabbitChan.QueueDeclare(
		"resource_events_queue",
		true, false, false, false, nil,
	)
	if err != nil {
		log.Fatalf("Не удалось объявить event-очередь: %v", err)
	}

	// Запускаем асинхронный поток прослушивания ответов от воркера
	go listenWorkerEvents()

	http.HandleFunc("/ws", handleWebConnections)
	log.Println("WS-API Gateway запущен на :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func handleWebConnections(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer ws.Close()

	clientsMu.Lock()
	clients[ws] = true
	clientsMu.Unlock()

	for {
		var msg Message
		if err := ws.ReadJSON(&msg); err != nil {
			clientsMu.Lock()
			delete(clients, ws)
			clientsMu.Unlock()
			break
		}

		if msg.Action == "update" {
			// АСИНХРОННОСТЬ: Вместо обработки на месте, пакуем в JSON и шлем в RabbitMQ
			body, _ := json.Marshal(msg.Resource)

			err = rabbitChan.PublishWithContext(
				context.Background(),
				"",        // exchange
				queueName, // routing key (имя очереди)
				false,     // mandatory
				false,     // immediate
				amqp.Publishing{
					DeliveryMode: amqp.Persistent, // ГАРАНТИЯ: Сообщение пишется на диск!
					ContentType:  "application/json",
					Body:         body,
				},
			)
			if err != nil {
				log.Printf("Ошибка отправки в RabbitMQ: %v", err)
			} else {
				log.Printf("Задача на обновление (Версия %d) принята и отправлена в очередь\n", msg.Resource.Version)
			}
		}
	}
}

func listenWorkerEvents() {
	msgs, err := rabbitChan.Consume("resource_events_queue", "", true, false, false, false, nil)
	if err != nil {
		log.Printf("Ошибка подписки на события воркера: %v", err)
		return
	}

	for d := range msgs {
		var res Resource
		json.Unmarshal(d.Body, &res)

		// Рассылаем абсолютно всем подключенным клиентам новую версию ресурса
		clientsMu.Lock()
		for client := range clients {
			err := client.WriteJSON(Message{Action: "broadcast", Resource: res})
			if err != nil {
				client.Close()
				delete(clients, client)
			}
		}
		clientsMu.Unlock()
		log.Printf("[BROADCAST] Всем клиентам отправлена новая версия из БД: %d\n", res.Version)
	}
}
