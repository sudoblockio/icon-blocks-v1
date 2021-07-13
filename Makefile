test: up-dbs test-unit test-chart

up-dbs:
	docker-compose -f docker-compose.dbs.yml up -d

test-unit:
	ginkgo -r -tags unit --randomizeAllSpecs --randomizeSuites --failOnPending --cover --trace --race --progress -v src/crud

test-integration:
	ginkgo -r -tags integration --randomizeAllSpecs --randomizeSuites --failOnPending --cover --trace --race --progress -v

up:
	docker-compose -f docker-compose.dbs.yml -f docker-compose.yml up -d
