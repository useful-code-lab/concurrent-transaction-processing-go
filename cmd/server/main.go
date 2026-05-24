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
	"github.com/jackc/pgx/v5/pgxpool"
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
	dbPool     *pgxpool.Pool // Добавляем пул СУБД для сервера
)

func main() {
	ctx := context.Background()
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

	_, err = rabbitChan.QueueDeclare(
		"resource_events_queue",
		true, false, false, false, nil,
	)
	if err != nil {
		log.Fatalf("Не удалось объявить event-очередь: %v", err)
	}

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:password@postgres-service:5432/concur_db?sslmode=disable"
	}

	for i := 1; i <= 10; i++ {
		log.Printf("Сервер: Попытка подключения к PostgreSQL %d/10...\n", i)
		dbPool, err = pgxpool.New(ctx, dbURL)
		if err == nil {
			if err = dbPool.Ping(ctx); err == nil {
				log.Println("Сервер успешно подключился к PostgreSQL!")
				break
			}
		}
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		log.Fatalf("Сервер не смог подключиться к БД: %v", err)
	}
	defer dbPool.Close()

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

	// ДИНАМИКА: Получаем ID ресурса из URL запроса (например, /ws?id=45)
	resourceID := r.URL.Query().Get("id")
	if resourceID == "" {
		resourceID = "1" // Значение по умолчанию для совместимости
	}

	clientsMu.Lock()
	clients[ws] = true
	clientsMu.Unlock()

	log.Printf("Пользователь подключился к ресурсу ID: %s. Отправляем состояние...\n", resourceID)

	// ИСПРАВЛЕНО: Подставляем resourceID вместо жесткой единицы
	var startRes Resource
	err = dbPool.QueryRow(r.Context(), "SELECT id, data, version FROM resources WHERE id=$1", resourceID).
		Scan(&startRes.ID, &startRes.Data, &startRes.Version)
	if err != nil {
		// Если ресурса еще нет в БД, инициализируем его дефолтными значениями для клиента
		startRes = Resource{ID: resourceID, Data: "Новый ресурс", Version: 1}
	}
	ws.WriteJSON(Message{Action: "read", Resource: startRes})

	for {
		var msg Message
		if err := ws.ReadJSON(&msg); err != nil {
			clientsMu.Lock()
			delete(clients, ws)
			clientsMu.Unlock()
			break
		}

		// Переопределяем ID из соображений безопасности (клиент работает только со своим ID)
		msg.Resource.ID = resourceID

		if msg.Action == "read" {
			var currentRes Resource
			err = dbPool.QueryRow(r.Context(), "SELECT id, data, version FROM resources WHERE id=$1", resourceID).
				Scan(&currentRes.ID, &currentRes.Data, &currentRes.Version)
			if err == nil {
				ws.WriteJSON(Message{Action: "read", Resource: currentRes})
			}
			continue
		}

		if msg.Action == "update" {
			body, _ := json.Marshal(msg.Resource)
			err = rabbitChan.PublishWithContext(
				context.Background(),
				"",        
				queueName, 
				false,     
				false,     
				amqp.Publishing{
					DeliveryMode: amqp.Persistent, 
					ContentType:  "application/json",
					Body:         body,
				},
			)
			if err != nil {
				log.Printf("Ошибка отправки в RabbitMQ: %v", err)
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
