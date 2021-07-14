test: up-dbs test-unit test-chart

up-dbs:
	docker-compose -f docker-compose.db.yml up -d

test-unit:
	cd src && go test ./... -v --tags=unit

test-integration:
	cd src && go test ./... -v --tags=integration

up:
	docker-compose -f docker-compose.db.yml -f docker-compose.yml up -d
