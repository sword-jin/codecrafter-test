package redis_test

import (
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/sword-jin/codecrafter-test/redis/internal"
)

func TestMain(m *testing.M) {
	close, err := startRedisServer(3*time.Second, 6379)
	if err != nil {
		println(err.Error())
		os.Exit(1)
	}

	code := m.Run()
	close()
	os.Exit(code)
}

func TestBindAPort(t *testing.T) {
	// nothing
}

func TestRespondToPING(t *testing.T) {
	r := require.New(t)
	conn, err := net.Dial("tcp", defaultRedisHost)
	r.NoError(err)
	defer conn.Close()

	assertPing(t, conn)
}

func TestRespondToMultiplePINGs(t *testing.T) {
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
	r := require.New(t)

	close, err := startRedisServer(3*time.Second, 6382)
	defer close()
	r.NoError(err)

	conn, err := net.Dial("tcp", "localhost:6382")
	require.NoError(t, err)
	defer conn.Close()
}

func TestTheInfoCommand(t *testing.T) {
	t.Log(`I assume you reply:
# Replication
role:master`)

	r := require.New(t)
	conn, err := net.Dial("tcp", "localhost:6379")
	r.NoError(err)
	defer conn.Close()

	sendRedisCommand(t, conn, "INFO")
	assertReadNContains(t, conn, 26+4, "role:master")
}

func TestInfoCommandOnAReplica(t *testing.T) {
	close, err := startRedisServer(3*time.Second, 6382, func() []string {
		return []string{"--replicaof", `localhost 6379`}
	})
	defer close()

	r := require.New(t)
	r.NoError(err)
	conn, err := net.Dial("tcp", "localhost:6382")
	r.NoError(err)
	defer conn.Close()

	sendRedisCommand(t, conn, "INFO")
	assertReadNContains(t, conn, 26+4, "role:slave")
}
