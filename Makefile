test: up-dbs test-unit test-integration

up-dbs:  ## Bring up the DBs
	docker-compose -f docker-compose.db.yml up -d

down-dbs:  ## Take down the DBs
	docker-compose -f docker-compose.db.yml down

test-unit:  ## Run unit tests - Need DB compose up
	cd src && go test ./... -v --tags=unit
	#ginkgo -r -tags unit --randomizeAllSpecs --randomizeSuites --failOnPending --cover --trace --race --progress -v

test-integration:  ## Run integration tests - Need DB compose up
	cd src && go test ./... -v --tags=integration
	#ginkgo -r -tags integration --randomizeAllSpecs --randomizeSuites --failOnPending --cover --trace --race --progress -v

up:  ## Bring everything up as containers
	docker-compose -f docker-compose.db.yml -f docker-compose.yml up -d

down:  ## Take down all the containers
	docker-compose -f docker-compose.db.yml -f docker-compose.yml down

build-swagger:  ## Build the swagger docs
	go get github.com/swaggo/swag/cmd/swag; \
    go get github.com/alecthomas/template; \
    go get github.com/riferrei/srclient@v0.3.0; \
    cd src/api && swag init -g routes/api.go

build:  ## Build everything
	docker-compose build

build-api:  ## Build the api
	docker-compose build blocks-api

build-worker:  ## Build the worker
	docker-compose build blocks-worker

ps:  ## List all containers and running status
	docker-compose -f docker-compose.db.yml -f docker-compose.yml ps

postgres-console:  ## Start postgres terminal
	docker-compose -f docker-compose.db.yml -f docker-compose.yml exec postgres psql -U postgres

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-16s\033[0m %s\n", $$1, $$2}'
