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

		query := `UPDATE resources SET data = $1, version = version + 1 WHERE id = '1' AND version = $2 RETURNING version;`
		var newVersion int
		err := dbPool.QueryRow(ctx, query, req.Data, req.Version).Scan(&newVersion)

		if err != nil {
			if errors.Is(err, sql.ErrNoRows) || err.Error() == "no rows in result set" {
				log.Printf("[ОТКЛОНЕНО] Конфликт версий в БД для версии %d. Пропускаем.\n", req.Version)
			} else {
				log.Printf("[ОШИБКА СУБД] Сбой: %v. Возвращаем в очередь!\n", err)
				d.Nack(false, true)
				continue
			}
		} else {
			log.Printf("[УСПЕХ] База данных обновлена до версии %d\n", newVersion)
		}

		d.Ack(false) // Стираем задачу из RabbitMQ после обработки
	}
}
