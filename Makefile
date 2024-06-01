all: test benchmark

docker-compose-up:
	docker-compose up -d

test: docker-compose-up
	ETCD_ENDPOINTS="127.0.0.1:2379" REDIS_ADDR="127.0.0.1:6379" REDIS_NODES="127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003,127.0.0.1:7004,127.0.0.1:7005" ZOOKEEPER_ENDPOINTS="127.0.0.1" CONSUL_ADDR="127.0.0.1:8500" AWS_ADDR="127.0.0.1:8000" MEMCACHED_ADDR="127.0.0.1:11211" POSTGRES_URL="postgres://postgres@localhost:5432/?sslmode=disable" go test -race -v -failfast

benchmark: docker-compose-up
	ETCD_ENDPOINTS="127.0.0.1:2379" REDIS_ADDR="127.0.0.1:6379" REDIS_NODES="127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003,127.0.0.1:7004,127.0.0.1:7005" ZOOKEEPER_ENDPOINTS="127.0.0.1" CONSUL_ADDR="127.0.0.1:8500" AWS_ADDR="127.0.0.1:8000" MEMCACHED_ADDR="127.0.0.1:11211" POSTGRES_URL="postgres://postgres@localhost:5432/?sslmode=disable" go test -race -run=nonexistent -bench=.
