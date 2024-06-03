package redis_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func getClient() *redis.Client {
	c := redis.NewClient(&redis.Options{
		Addr: redisHost,
	})
	return c
}

func newContext() (context.Context, func()) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}

func assertPing(t *testing.T, conn net.Conn) {
	r := require.New(t)
	_, err := conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	r.NoError(err)

	var b = make([]byte, 7)
	n, err := conn.Read(b)
	r.NoError(err)

	r.Equal([]byte("+PONG\r\n"), b[:n])
}
