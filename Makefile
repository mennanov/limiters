all: test

test:
	docker-compose up -d
	ETCD_ENDPOINTS="localhost:2379" REDIS_ADDR="localhost:6379" ZOOKEEPER_ENDPOINTS="localhost" CONSUL_ADDR="localhost:8500" AWS_ADDR="localhost:8000" MEMCACHED_ADDR="localhost:11211" POSTGRES_URL="postgres://postgres@localhost:5432/?sslmode=disable" MYSQL_URL="root@tcp(localhost:3306)/" go test -race -v -failfast -bench=.
