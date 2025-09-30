# ====== Config ======
COMPOSE          ?= docker compose
HTTP_PORT        ?= 8082
DB_PORT          ?= 5433
KAFKA_PORT       ?= 9094

TOPIC            ?= orders
DATA_GLOB        ?= data/*.json

DB_DSN           ?= postgres://app:app@localhost:$(DB_PORT)/orders_db?sslmode=disable
KAFKA_BROKERS    ?= localhost:$(KAFKA_PORT)

SERVICE_CMD      ?= go run ./cmd/service
PRODUCER_CMD     ?= go run ./cmd/producer

CURL             ?= curl -sS
PSQL             ?= psql

CACHE_WARM       ?= 1  # 1 = прогревать кэш при старте сервиса, 0 = не прогревать

# ====== Tests & Mocks ======
TEST_PKGS      ?= ./...
GOTESTFLAGS    ?= -count=1

# ====== Phony ======
.PHONY: up down restart ps logs logs-kafka logs-postgres clean nuke \
        topic-create topic-list consume-one produce produce-file \
        run run8081 run8082 run8083 \
        health ui get \
        db-shell db-list \
        smoke \
        test test-race cover cover-html tidy deps mock generate mockclean

# ====== Docker Compose ======
up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

restart: down up

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f

logs-kafka:
	$(COMPOSE) logs -f kafka

logs-postgres:
	$(COMPOSE) logs -f postgres

# Удалить контейнеры + тома (удалит данные БД)
clean:
	$(COMPOSE) down -v

# Полная очистка Docker артефактов (осторожно!)
nuke: clean
	docker system prune -f

# ====== Kafka ======
# Создать топик (внутри контейнера Kafka)
topic-create:
	$(COMPOSE) exec kafka \
	  kafka-topics.sh --bootstrap-server localhost:9092 \
	  --create --if-not-exists --topic $(TOPIC) --partitions 1 --replication-factor 1

# Список топиков (внутри контейнера Kafka)
topic-list:
	$(COMPOSE) exec kafka \
	  kafka-topics.sh --bootstrap-server localhost:9092 --list

# Прочитать одно сообщение с начала (удобно для проверки)
consume-one:
	$(COMPOSE) exec kafka \
	  kafka-console-consumer.sh --bootstrap-server localhost:9092 \
	  --topic $(TOPIC) --from-beginning --max-messages 1

# Отправить все JSON из DATA_GLOB через Go-продюсер (читает и одиночные объекты, и массивы)
produce:
	KAFKA_BROKERS='$(KAFKA_BROKERS)' \
	KAFKA_TOPIC='$(TOPIC)' \
	DATA_GLOB='$(DATA_GLOB)' \
	$(PRODUCER_CMD)

# Отправить один JSON-файл напрямую через консольный продюсер внутри контейнера
# Использование: make produce-file FILE=data/order1.json
produce-file:
	@test -n "$(FILE)" || (echo "Usage: make produce-file FILE=data/order.json" && exit 1)
	cat "$(FILE)" | $(COMPOSE) exec -T kafka \
	  kafka-console-producer.sh --bootstrap-server localhost:9092 --topic $(TOPIC)

# ====== Service (Go) ======
# Запуск сервиса с текущими портами/переменными
run:
	HTTP_ADDR=':$(HTTP_PORT)' \
	DB_DSN='$(DB_DSN)' \
	KAFKA_BROKERS='$(KAFKA_BROKERS)' \
	CACHE_WARM='$(CACHE_WARM)' \
	$(SERVICE_CMD)

# Быстрые алиасы на частые порты
run8081:
	$(MAKE) run HTTP_PORT=8081
run8082:
	$(MAKE) run HTTP_PORT=8082
run8083:
	$(MAKE) run HTTP_PORT=8083

# ====== HTTP helpers ======
health:
	$(CURL) -i http://localhost:$(HTTP_PORT)/healthz

# Открыть веб-страницу (macOS). Для Linux можно заменить на xdg-open.
ui:
	open "http://localhost:$(HTTP_PORT)/" || true
	@echo "UI: http://localhost:$(HTTP_PORT)/  (или /web/index.html если не настроен fs.Sub)"

# GET заказа по ID: make get ID=b563feb7b2b84b6test
get:
	@test -n "$(ID)" || (echo "Usage: make get ID=<order_uid>" && exit 1)
	$(CURL) -i http://localhost:$(HTTP_PORT)/order/$(ID)

# ====== Postgres ======
# Подключиться к БД через psql
db-shell:
	$(PSQL) "$(DB_DSN)"

# Показать таблицы
db-list:
	$(PSQL) "$(DB_DSN)" -c '\dt'

# ====== Developer comfort ======
# Мини-smoke: поднять infra, создать топик, запустить сервис (в отдельном терминале),
# затем закинуть тестовые JSON и сходить за одним из заказов
smoke: up topic-create
	@echo ">>> Запусти в ДРУГОМ терминале: make run8082 (или run8081)"
	@echo ">>> Затем в ЭТОМ терминале:"
	@echo "    make produce"
	@echo "    make get ID=b563feb7b2b84b6test"

# ====== Tests & Mocks ======
# Генерация моков для слоя хранилища
# Предполагается, что интерфейс Repository объявлен в internal/store (package store)
# и мок-реализация будет сгенерирована в internal/store/storemock (package storemock).

mockclean:
	@rm -f internal/store/storemock/repo_mock.go \
	      internal/store/storemock/repository_mock.go

mock: mockclean
	@echo ">> installing mockgen"
	go install github.com/golang/mock/mockgen@latest
	@echo ">> generating mocks for store.Repository"
	@mkdir -p internal/store/storemock
	mockgen -package storemock \
		-destination internal/store/storemock/repository_mock.go \
		demo/orders/internal/store Repository

generate:
	go generate $(TEST_PKGS)

deps:
	go mod download

tidy:
	go mod tidy

test: mock
	go test $(GOTESTFLAGS) $(TEST_PKGS)

test-race: mock
	go test -race $(GOTESTFLAGS) $(TEST_PKGS)

cover: mock
	go test -coverprofile=coverage.out $(GOTESTFLAGS) $(TEST_PKGS)
	@go tool cover -func=coverage.out | tail -n 1

cover-html: cover
	go tool cover -html=coverage.out -o coverage.html
	@echo "coverage: coverage.html"