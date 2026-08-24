SEED_SOURCE ?= /Users/ivy/Documents/1kaggle_competition/archive

.PHONY: build up down restart logs seed-data status clean

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

restart:
	docker compose restart

logs:
	docker compose logs -f

status:
	docker compose ps

seed-data:
	cp $(SEED_SOURCE)/*.csv data/seed/

init: build seed-data up
	@echo "Waiting for services to start..."
	@sleep 10
	@echo ""
	@echo "Services:"
	@echo "  Airflow UI:      http://localhost:8082  (admin / admin)"
	@echo "  Spark Master UI: http://localhost:8083"
	@echo "  MinIO Console:   http://localhost:9003  (minio_access_key / minio_secret_key)"
	@echo "  ClickHouse HTTP: http://localhost:8124"

clean:
	docker compose down -v
	rm -f data/seed/*.csv
