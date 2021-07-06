build:
	docker-compose build

up:
	docker-compose up -d

ps:
	docker-compose ps

test: test-unit test-chart

test-unit:
	@docker-compose up -d; \
 	docker-compose run api go test .

test-chart:
	# TODO
	docker-compose ps
