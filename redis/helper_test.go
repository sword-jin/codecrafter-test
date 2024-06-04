package redis_test

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/sword-jin/codecrafter-test/redis/internal"
)

var (
	yourProgramPath string
)

const (
	defaultRedisHost = "localhost:6379"
	EnvRedisHost     = "TEST_REDIS_HOST"
	EnvProgram       = "YOUR_REDIS_PROGRAM_PATH"
)

func init() {
	if os.Getenv(EnvProgram) == "" {
		panic("YOUR_REDIS_PROGRAM_PATH is not set")
	}

	yourProgramPath = os.Getenv(EnvProgram)
	if _, err := os.Stat(yourProgramPath); os.IsNotExist(err) {
		log.Fatalf("YOUR_REDIS_PROGRAM_PATH is not a valid path: %s", yourProgramPath)
	}
}

func startRedisServer(t *testing.T, port int) (close func()) {
	cmd := exec.Command(
		yourProgramPath,
		"--port",
		strconv.Itoa(port),
	)
	output := bytes.NewBuffer(nil)
	cmd.Stdout = output
	cmd.Stderr = output

	defer func() {
		if t.Failed() {
			t.Log("Redis server output:")
			println(output.String())
		}
	}()

	err := cmd.Start()
	require.NoError(t, err)

	for {
		conn, err := net.Dial("tcp", "localhost:"+strconv.Itoa(port))
		if err == nil {
			defer conn.Close()
			break
		}
		t.Log("Waiting for redis server to start...")
		time.Sleep(100 * time.Millisecond)
	}

	return func() {
		require.NoError(t, cmd.Process.Kill())
	}
}

// genKey generates a unique key based on current time for testing
func genKey() string {
	return fmt.Sprintf("key-%d", time.Now().UnixNano())
}

func getClient() *redis.Client {
	c := redis.NewClient(&redis.Options{
		Addr: defaultRedisHost,
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

func sendRedisCommand(t *testing.T, conn net.Conn, args ...any) {
	buf := bytes.NewBuffer(nil)
	writer := internal.NewWriter(buf)

	err := writer.WriteArgs(args)
	require.NoError(t, err)
	_, err = conn.Write(buf.Bytes())
	require.NoError(t, err)
}
