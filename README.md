# Orders (Kafka → Postgres → HTTP)

Небольшой учебный сервис для приёма заказов из Kafka, валидации, сохранения в Postgres и выдачи по HTTP.
В комплекте — продюсер для отправки JSON (из файлов или сгенерированных), простой кэш в памяти и минимальный веб-интерфейс для просмотра заказа.

## Быстрый старт

1) Требования
	•	Go 1.22+ (подойдёт и 1.21+, но проект ориентирован на актуальные версии)
	•	Docker + Docker Compose
	•	Make (опционально, но удобно)

2) Поднять инфраструктуру (Postgres, Kafka/ZooKeeper и т.п.)
make up
make topic-create           # создаст топик orders (или свой, см. переменные)
3) Запустить сервис
make run                    # слушает :8082 по умолчанию
## Быстрые алиасы:
` make run8081 | make run8082 | make run8083 `

## Проверка работоспособности:

` make health `
 
## Открыть встроенный UI (macOS):
` make ui `
## Отправить данные
### Вариант А: отправить все JSON из data/*.json через продюсер на Go:
` make produce `
### Вариант Б: отправить один файл напрямую консольным продюсером:
` make produce-file FILE=data/order1.json `
### Вариант В (fallback продюсера): сгенерировать N случайных заказов, если файлов нет:
` GEN_COUNT=10 GEN_INTERVAL_MS=200 make produce `
### Получить заказ по order_uid
### пример из задания:
` make get ID=b563feb7b2b84b6test `

### или через curl:
` curl -i http://localhost:8082/order/b563feb7b2b84b6test `

 В ответе заголовок X-Cache: HIT|MISS покажет, попали ли в кэш.

## Архитектура
	•	Kafka consumer (cmd/internal/main.go):
	•	Читает сообщения из топика.
	•	Парсит JSON → model.Order.
	•	Валидирует (internal/validate).
	•	Пишет/апсертит в Postgres (internal/store.Repo.UpsertOrder).
	•	Кладёт в кэш (in-memory) для быстрых GET.
	•	HTTP API:
	•	GET /order/{order_uid} — получить заказ (сначала из кэша, затем из БД; заголовок X-Cache).
	•	GET /healthz — health-check.
	•	In-memory кэш:
	•	Параметры: TTL, ограничение по кол-ву ключей, периодический janitor.
	•	Прогрев кэша при старте (можно выключить).
	•	Миграции:
	•	Управляются из кода (golang-migrate + embed). Режим задаётся DB_MIGRATE=up|down|force.
	•	Продюсер:
	•	Отправляет JSON-объекты заказов в Kafka.
	•	Может читать один файл (object) или массив объектов.
	•	Если файлов не найдено — генерит N заказов через gofakeit.

## HTTP эндпоинты
```
GET /order/{order_uid}
```
200 — JSON заказа
404 — не найдено
Заголовок: X-Cache: HIT|MISS
```
GET /healthz
```
### Пример:
```
curl -s http://localhost:8082/order/b563feb7b2b84b6test \
  -H 'Accept: application/json' -i
```


### Валидация (internal/validate):
	•	order_uid — 6..64 символов [A-Za-z0-9._-]
	•	track_number — 6..32 A-Z0-9
	•	customer_id — 1..64 [A-Za-z0-9._-]
	•	date_created — не в будущем
	•	payment.currency — ISO-3 (A-Z)
	•	суммы/стоимости — >= 0
	•	items — минимум 1, с обязательными полями и здравыми диапазонами

## Работа с БД
Подключение: DB_DSN (см. Makefile для локального порта 5433). 
Схема создаётся миграциями автоматически при старте сервиса (DB_MIGRATE=up).
Удобные цели:
```
make db-list 
make db-shell 
```
Тесты и моки
	•	Юнит-тесты на валидацию и слой сервиса (internal/validate, internal/service).
	•	Моки репозитория через gomock.

Установка mockgen и генерация моков:
```
go install github.com/golang/mock/mockgen@latest
go generate ./internal/store
```

Генерация управляется директивой в internal/store/repo.go:
```
//go:generate mockgen -destination=./storemock/repo_mock.go -package=storemock demo/orders/internal/store Repository
```

Запуск тестов: go test ./...
Makefile: полезные цели
### Инфраструктура
```
make up            # поднять docker-compose
make down          # остановить
make clean         # остановить + удалить тома (очистит БД)
make nuke          # полная зачистка docker system prune -f
make logs          # логи всего
make logs-kafka
make logs-postgres
```
### Kafka
```
make topic-create
make topic-list
make consume-one   # прочитать одно сообщение с начала
make produce       # отправить все JSON из DATA_GLOB через продюсер
make produce-file FILE=data/order.json
```
### Сервис
```
make run           # запустить сервис (HTTP_ADDR, DB_DSN, KAFKA_BROКERS берутся из переменных)
make run8081|2|3
```
### HTTP helpers
```
make health
make ui
make get ID=<order_uid>
```

### Postgres
```
make db-shell
make db-list
```
### Мини-smoke
```
make smoke
```
## Автор

Order Viewer by Koltyshev Egor
