all: test

test:
	docker-compose up -d
	ETCD_ENDPOINTS="127.0.0.1:2379" REDIS_ADDR="127.0.0.1:6379" ZOOKEEPER_ENDPOINTS="127.0.0.1" CONSUL_ADDR="127.0.0.1:8500" AWS_ADDR="127.0.0.1:8000" MEMCACHED_ADDR="127.0.0.1:11211" POSTGRES_URL="postgres://postgres@localhost:5432/?sslmode=disable" go test -race -v -failfast -bench=.
