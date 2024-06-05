package redis_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/sword-jin/codecrafter-test/redis/internal"
)

const (
	ping = "*1\r\n$4\r\nPING\r\n"
	pong = "+PONG\r\n"
	ok   = "+OK\r\n"
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

func newStartRedisServerCmd(port int, appendArgs ...func() []string) *exec.Cmd {
	args := []string{
		"--port", strconv.Itoa(port),
	}
	for _, f := range appendArgs {
		args = append(args, f()...)
	}
	return exec.Command(yourProgramPath, args...)
}

func startRedisServer(timeout time.Duration, port int, appendArgs ...func() []string) (close func() error, err error) {
	cmd := newStartRedisServerCmd(port, appendArgs...)
	output := bytes.NewBuffer(nil)
	cmd.Stdout = output
	cmd.Stderr = output

	defer func() {
		if err != nil {
			println("redis-server output:")
			fmt.Println(output.String())
		}
	}()

	err = cmd.Start()
	if err != nil {
		return
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
Loop:
	for {
		select {
		case <-timer.C:
			err = fmt.Errorf("timeout starting redis-server")
			return
		default:
			conn, err := net.Dial("tcp", "localhost:"+strconv.Itoa(port))
			if err == nil {
				defer conn.Close()
				break Loop
			}
			time.Sleep(1 * time.Second)
		}
	}

	return func() error {
		err := cmd.Process.Kill()
		// println("master redis-server output:")
		// fmt.Println(output.String())
		if err != nil {
			println("redis-server output:")
			fmt.Println(output.String())
			return err
		}
		return nil
	}, nil
}

// we assume all the operations are finished in 1 second
func dialConn(t *testing.T, port int) net.Conn {
	r := require.New(t)
	conn, err := net.Dial("tcp", "localhost:"+strconv.Itoa(port))
	r.NoError(err)
	err = conn.SetDeadline(time.Now().Add(1 * time.Second))
	r.NoError(err)
	return conn
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
	_, err := conn.Write([]byte(ping))
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

func readBulkString(t *testing.T, reader *internal.Reader) string {
	r := require.New(t)
	firstLine, err := reader.ReadLine()
	r.NoError(err)
	length, err := strconv.Atoi(string(firstLine[1:]))
	r.NoError(err)

	buf := make([]byte, length+2) // add the \r\n
	readN, err := reader.Read(buf)
	r.NoError(err)
	r.Equal(readN, length+2)
	return string(buf)
}

func assertReadLines(t *testing.T, reader *internal.Reader, expected ...string) {
	r := require.New(t)
	for _, e := range expected {
		a, err := reader.ReadLine()
		r.NoError(err)
		r.Equal(e, string(a))
	}
}

func assertReadNContains(t *testing.T, ro io.Reader, n int, expected string) {
	r := require.New(t)
	a := assert.New(t)
	actual := make([]byte, n)
	readN, err := ro.Read(actual)
	r.NoError(err)
	a.Equal(n, readN, "actual: %s", string(actual))
	a.Contains(string(actual), expected)
}

func assertErrorIsNilOrMatch(t *testing.T, err error, match func(error) bool) {
	if err == nil {
		return
	}
	require.True(t, match(err))
}
