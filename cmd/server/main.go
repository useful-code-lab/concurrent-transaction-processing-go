package main

import (
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

// Resource описывает документ с версией
type Resource struct {
	ID      string `json:"id"`
	Data    string `json:"data"`
	Version int    `json:"version"`
}

// Сообщение, которым обмениваются клиент и сервер
type Message struct {
	Action   string   `json:"action"`   // "read", "update", "broadcast"
	Resource Resource `json:"resource"` // Данные ресурса
	Error    string   `json:"error,omitempty"`
}

var (
	// Защита ресурса и списка клиентов
	mu       sync.Mutex
	resource = Resource{ID: "1", Data: "Стартовое состояние платформы", Version: 1}

	// Список всех активных WebSocket-клиентов
	clients  = make(map[*websocket.Conn]bool)
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true }, // Разрешаем любые подключения
	}
)

func main() {
	http.HandleFunc("/ws", handleWebConnections)
	fmt.Println("WS-Сервер платформы concur-platform запущен на :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func handleWebConnections(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Ошибка апгрейда соединения: %v", err)
		return
	}
	defer ws.Close()

	mu.Lock()
	clients[ws] = true
	mu.Unlock()

	log.Println("Новый пользователь подключился к платформе")

	// Бесконечный цикл чтения сообщений от данного клиента
	for {
		var msg Message
		err := ws.ReadJSON(&msg)
		if err != nil {
			mu.Lock()
			delete(clients, ws)
			mu.Unlock()
			log.Println("Пользователь отключился")
			break
		}

		handleAction(ws, msg)
	}
}

func handleAction(ws *websocket.Conn, msg Message) {
	mu.Lock()
	defer mu.Unlock()

	switch msg.Action {
	case "read":
		// Клиент просто хочет получить текущее состояние
		ws.WriteJSON(Message{Action: "read", Resource: resource})

	case "update":
		// Клиент пытается обновить ресурс (Оптимистическая блокировка)
		if msg.Resource.Version != resource.Version {
			// Конфликт версий! Отправляем ошибку конкретно этому клиенту
			ws.WriteJSON(Message{
				Action:   "error",
				Resource: resource, // Возвращаем актуальную версию, чтобы клиент знал её
				Error:    "conflict",
			})
			log.Printf("Конфликт: Клиент прислал версию %d, а в системе уже %d\n", msg.Resource.Version, resource.Version)
			return
		}

		// Успех: Обновляем данные и инкрементируем версию
		resource.Data = msg.Resource.Data
		resource.Version++
		log.Printf("Ресурс успешно обновлен. Новая версия: %d\n", resource.Version)

		// Мгновенно уведомляем В С Е Х подключенных пользователей о новых данных!
		broadcast(Message{Action: "broadcast", Resource: resource})
	}
}

// Рассылка сообщения абсолютно всем клиентам
func broadcast(msg Message) {
	for client := range clients {
		err := client.WriteJSON(msg)
		if err != nil {
			client.Close()
			delete(clients, client)
		}
	}
}
