package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Resource struct {
	ID      string `json:"id"`
	Data    string `json:"data"`
	Version int    `json:"version"`
}

func main() {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:password@postgres-service:5432/concur_db?sslmode=disable"
	}
	rabbitURL := os.Getenv("RABBITMQ_URL")
	if rabbitURL == "" {
		rabbitURL = "amqp://guest:guest@rabbitmq-service:5672/"
	}

	ctx := context.Background()

	// 1. Подключение к PostgreSQL с логикой Retry
	var dbPool *pgxpool.Pool
	var err error
	for i := 1; i <= 10; i++ {
		log.Printf("Воркер: Попытка подключения к PostgreSQL %d/10...\n", i)
		dbPool, err = pgxpool.New(ctx, dbURL)
		if err == nil {
			if err = dbPool.Ping(ctx); err == nil {
				log.Println("Воркер успешно подключился к PostgreSQL!")
				break
			}
		}
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		log.Fatalf("Воркер не смог подключиться к БД: %v", err)
	}
	defer dbPool.Close()

	// 2. Подключение к RabbitMQ с логикой Retry
	var conn *amqp.Connection
	for i := 1; i <= 10; i++ {
		log.Printf("Воркер: Попытка подключения к RabbitMQ %d/10...\n", i)
		conn, err = amqp.Dial(rabbitURL)
		if err == nil {
			log.Println("Воркер успешно подключился к RabbitMQ!")
			break
		}
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		log.Fatalf("Воркер не смог подключиться к RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Ошибка канала: %v", err)
	}
	defer ch.Close()

	// Prefetch = 1 гарантирует строгую последовательность обработки в один поток!
	err = ch.Qos(1, 0, false)
	if err != nil {
		log.Fatalf("Ошибка настройки Qos: %v", err)
	}

	msgs, err := ch.Consume(
		"resource_updates_queue",
		"",
		false, // auto-ack (ОТКЛЮЧАЕМ: подтверждаем вручную только после записи в БД)
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Ошибка регистрации Consumer: %v", err)
	}

	log.Println("Фоновый воркер успешно запущен и слушает очередь задач...")

	for d := range msgs {
		var req Resource
		if err := json.Unmarshal(d.Body, &req); err != nil {
			log.Printf("Ошибка декодирования: %v", err)
			d.Ack(false)
			continue
		}

				log.Printf("[ОЧЕРЕДЬ] Обработка ресурса ID %s. Пытаемся записать версию %d...\n", req.ID, req.Version)

		// ДИНАМИКА: Используем ON CONFLICT для автоматического создания новых ID,
		// и фильтр по переданному req.Version для оптимистической блокировки существующих.
		query := `
			INSERT INTO resources (id, data, version) 
			VALUES ($1, $2, 1)
			ON CONFLICT (id) 
			DO UPDATE SET data = EXCLUDED.data, version = resources.version + 1
			WHERE resources.version = $3
			RETURNING version;
		`
		var newVersion int
		err := dbPool.QueryRow(ctx, query, req.ID, req.Data, req.Version).Scan(&newVersion)

		if err != nil {
			if errors.Is(err, sql.ErrNoRows) || err.Error() == "no rows in result set" {
				log.Printf("[ОТКЛОНЕНО] Конфликт версий в БД для ID %s (версия %d). Пропускаем.\n", req.ID, req.Version)
			} else {
				log.Printf("[ОШИБКА СУБД] Сбой: %v. Возвращаем в очередь!\n", err)
				d.Nack(false, true) 
				continue
			}
		} else {
			log.Printf("[УСПЕХ] Ресурс ID %s успешно обновлен до версии %d\n", req.ID, newVersion)

			// Отправляем событие обратно для броадкаста
			req.Version = newVersion
			eventBody, _ := json.Marshal(req)
			ch.PublishWithContext(ctx, "", "resource_events_queue", false, false,
				amqp.Publishing{
					DeliveryMode: amqp.Persistent,
					ContentType:  "application/json",
					Body:         eventBody,
				},
			)
		}
		d.Ack(false)
	}
}
