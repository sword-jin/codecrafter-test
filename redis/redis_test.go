package redis_test

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/sword-jin/codecrafter-test/redis/internal"
)

func TestBindAPort(t *testing.T) {
	close := startRedisServer(t, 6379)
	defer close()

	conn, err := net.Dial("tcp", defaultRedisHost)
	require.NoError(t, err)
	defer conn.Close()
}

func TestRespondToPING(t *testing.T) {
	close := startRedisServer(t, 6379)
	defer close()

	r := require.New(t)
	conn, err := net.Dial("tcp", defaultRedisHost)
	r.NoError(err)
	defer conn.Close()

	assertPing(t, conn)
}

func TestRespondToMultiplePINGs(t *testing.T) {
	close := startRedisServer(t, 6379)
	defer close()

	t.Parallel()
	r := require.New(t)
	conn, err := net.Dial("tcp", defaultRedisHost)
	r.NoError(err)

	defer conn.Close()

	for i := 0; i < 10; i++ {
		assertPing(t, conn)
	}
}

func TestHandleConcurrentClients(t *testing.T) {
	close := startRedisServer(t, 6379)
	defer close()

	t.Parallel()
	r := require.New(t)

	wg := &sync.WaitGroup{}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			time.Sleep(10 * time.Microsecond)
			defer wg.Done()
			conn, err := net.Dial("tcp", defaultRedisHost)
			r.NoError(err)
			defer conn.Close()

			for i := 0; i < 10; i++ {
				assertPing(t, conn)
			}
		}()
	}
	wg.Wait()
}

func TestImplementTheECHOCommand(t *testing.T) {
	close := startRedisServer(t, 6379)
	defer close()

	r := require.New(t)
	conn, err := net.Dial("tcp", defaultRedisHost)
	r.NoError(err)
	defer conn.Close()
	reader := internal.NewReader(conn)

	for i := 0; i < 10; i++ {
		hey := fmt.Sprintf("hey%d", i)
		_, err := conn.Write([]byte(fmt.Sprintf("*2\r\n$4\r\nECHO\r\n$%d\r\n%s\r\n", len("hey")+1, hey)))
		r.NoError(err)

		a1, err := reader.ReadLine()
		r.NoError(err)
		r.Equal([]byte(`$4`), a1)

		a2, err := reader.ReadLine()
		r.NoError(err)
		r.Equal([]byte(hey), a2)
	}
}

func TestImplementTheSETGETcommands(t *testing.T) {
	close := startRedisServer(t, 6379)
	defer close()

	r := require.New(t)
	conn, err := net.Dial("tcp", defaultRedisHost)
	r.NoError(err)
	defer conn.Close()
	reader := internal.NewReader(conn)

	for i := 0; i < 10; i++ {
		// GET not found
		key := fmt.Sprintf("%s%d", genKey(), i)
		{
			_, err := conn.Write([]byte(fmt.Sprintf("*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", len(key), key)))
			r.NoError(err)
			_, err = reader.ReadLine()
			r.ErrorIs(err, internal.Nil)
		}

		// SET
		value := fmt.Sprintf("value%d", i)
		{
			_, err := conn.Write([]byte(fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(value), value)))
			r.NoError(err)
			a1, err := reader.ReadLine()
			r.NoError(err)
			r.Equal([]byte("+OK"), a1)
		}

		// GET
		{
			_, err := conn.Write([]byte(fmt.Sprintf("*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", len(key), key)))
			r.NoError(err)
			a1, err := reader.ReadLine()
			r.NoError(err)
			r.Equal([]byte(fmt.Sprintf("$%d", len(value))), a1)
			a2, err := reader.ReadLine()
			r.NoError(err)
			r.Equal([]byte(value), a2)
		}
	}
}

func TestExpiry(t *testing.T) {
	close := startRedisServer(t, 6379)
	defer close()

	r := require.New(t)
	conn, err := net.Dial("tcp", defaultRedisHost)
	r.NoError(err)
	defer conn.Close()
	reader := internal.NewReader(conn)

	key := genKey()

	{
		sendRedisCommand(t, conn, "GET", key)
		_, err = reader.ReadLine()
		r.ErrorIs(err, internal.Nil)
	}

	value := "value"
	{
		sendRedisCommand(t, conn, "SET", key, value, "PX", 100)
		ok, err := reader.ReadLine()
		r.NoError(err)
		r.Equal("+OK", string(ok))
	}

	{
		sendRedisCommand(t, conn, "GET", key)
		a1, err := reader.ReadLine()
		r.NoError(err)
		r.Equal(fmt.Sprintf("$%d", len(value)), string(a1))
		a2, err := reader.ReadLine()
		r.NoError(err)
		r.Equal(value, string(a2))
	}

	time.Sleep(1 * time.Second)
	{
		sendRedisCommand(t, conn, "GET", key)
		_, err = reader.ReadLine()
		r.ErrorIs(err, internal.Nil)
	}
}

func TestConfigureListeningPort(t *testing.T) {
	close := startRedisServer(t, 6382)
	defer close()

	conn, err := net.Dial("tcp", "localhost:6382")
	require.NoError(t, err)
	defer conn.Close()
}
