test: up-dbs test-unit test-chart

up-dbs:  ## Bring up the DBs
	docker-compose -f docker-compose.db.yml up -d

test-unit:  ## Run unit tests - Need DB compose up
	cd src && go test ./... -v --tags=unit
	#ginkgo -r -tags unit --randomizeAllSpecs --randomizeSuites --failOnPending --cover --trace --race --progress -v

test-integration:  ## Run integration tests - Need DB compose up
	cd src && go test ./... -v --tags=integration
	#ginkgo -r -tags integration --randomizeAllSpecs --randomizeSuites --failOnPending --cover --trace --race --progress -v

up:  ## Bring everything up as containers
	docker-compose -f docker-compose.db.yml -f docker-compose.yml up -d

build-swagger:  ## Build the swagger docs
	go get github.com/swaggo/swag/cmd/swag; \
    go get github.com/alecthomas/template; \
    go get github.com/riferrei/srclient@v0.3.0; \
    cd src && swag init -g api/routes/api.go

build-api:  ## Build the api
	cd src && go build -o main ./api

build-worker:  ## Build the worker
	cd src && go build -o main ./worker

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-16s\033[0m %s\n", $$1, $$2}'
