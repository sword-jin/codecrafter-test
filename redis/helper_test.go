package redis_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

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
	EnvProgram = "YOUR_REDIS_PROGRAM_PATH"
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

type nodeInfo struct {
	t        *testing.T
	isMaster bool
	port     int
	close    func() error
	output   *bytes.Buffer
}

func (n nodeInfo) startSlave() (net.Conn, nodeInfo) {
	return startNode(n.t, freePort(n.t), "--replicaof", fmt.Sprintf("localhost %d", n.port))
}

func (n nodeInfo) dial() net.Conn {
	conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", n.port))
	require.NoError(n.t, err)
	return conn
}

func startNode(t *testing.T, port int, args ...string) (net.Conn, nodeInfo) {
	timeout := 10 * time.Second // may need compile your program
checkPort:
	for {
		select {
		case <-time.After(timeout):
			t.Fatalf("port %d is not free", port)
		default:
			conn, err := net.Dial("tcp", "localhost:"+strconv.Itoa(port))
			if err != nil {
				break checkPort
			}
			conn.Close()
			time.Sleep(200 * time.Millisecond)
		}
	}

	cmd := newStartRedisServerCmd(port, args...)
	output := bytes.NewBuffer(nil)
	cmd.Stdout = output
	cmd.Stderr = output

	var err error
	defer func() {
		if t.Failed() {
			fmt.Printf("redis-server on %d error: %v", port, err)
			fmt.Println(output.String())
		}
	}()

	err = cmd.Start()
	require.NoError(t, err)

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	var conn net.Conn
Loop:
	for {
		select {
		case <-timer.C:
			t.Fatalf("timeout starting redis-server")
		default:
			conn, err = net.Dial("tcp", "localhost:"+strconv.Itoa(port))
			if err == nil {
				break Loop
			}
			time.Sleep(1 * time.Second)
		}
	}

	conn.(*net.TCPConn).SetReadDeadline(time.Now().Add(60 * time.Second)) // keep the root connection alive longer

	close := func() error {
		defer func() {
			if t.Failed() {
				println("redis-server output:")
				fmt.Println(output.String())
			}
		}()

		cmd.Process.Kill()
		return nil
	}

	return conn, nodeInfo{t: t, port: port, close: close, output: output}
}

func startMasterOn6379(t *testing.T) (net.Conn, nodeInfo) {
	return startNode(t, 6379)
}

func startMaster(t *testing.T) (net.Conn, nodeInfo) {
	conn, node := startNode(t, freePort(t))
	node.isMaster = true
	return conn, node
}

func startMasterAndSlave(t *testing.T) (nodeInfo, nodeInfo, func() error) {
	masterConn, master := startMaster(t)
	masterConn.Close()
	slaveConn, slaveInfo := master.startSlave()
	slaveConn.Close()
	return master, slaveInfo, func() error {
		return errors.Join(master.close(), slaveInfo.close())
	}
}

func freePort(t *testing.T) int {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	require.NoError(t, err)

	l, err := net.ListenTCP("tcp", addr)
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

func newStartRedisServerCmd(port int, extraArgs ...string) *exec.Cmd {
	args := []string{
		"--port", strconv.Itoa(port),
	}
	args = append(args, extraArgs...)
	return exec.Command(yourProgramPath, args...)
}

// we assume all the operations are finished in 1 second
func dialConn(t *testing.T, port int) net.Conn {
	r := require.New(t)
	conn, err := net.Dial("tcp", "localhost:"+strconv.Itoa(port))
	r.NoError(err)
	err = conn.SetDeadline(time.Now().Add(3 * time.Second))
	r.NoError(err)
	return conn
}

// genKey generates a unique key based on current time for testing
func genKey() string {
	return fmt.Sprintf("key-%d", time.Now().UnixNano())
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

func readN(t *testing.T, reader *internal.Reader, n int) []byte {
	r := require.New(t)
	actual := make([]byte, n)
	readN, err := reader.Read(actual)
	r.NoError(err)
	r.Equal(n, readN, "actual: %s", string(actual))
	return actual
}

func assertReadNContains(t *testing.T, reader *internal.Reader, n int, expected string) {
	r := require.New(t)
	a := assert.New(t)
	actual := readN(t, reader, n)
	a.Contains(string(actual), expected)
	// after read, we should read all the remaining bytes
	var err error
	for {
		b := make([]byte, 1)
		_, err = reader.Read(b)
		if err != nil {
			break
		}
	}
	if isTimeout(err) {
		return
	}
	r.NoError(err)
}

func isTimeout(err error) bool {
	if err == nil {
		return false
	}
	netErr, ok := err.(net.Error)
	if !ok {
		return false
	}
	return netErr.Timeout()
}

func assertErrorIsNilOrMatch(t *testing.T, err error, match func(error) bool) {
	if err == nil {
		return
	}
	require.True(t, match(err))
}

type fakeRedisServer struct {
	t  *testing.T
	r  *require.Assertions
	ln net.Listener
}

func newFakeRedisServer(t *testing.T, port int) *fakeRedisServer {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	r := require.New(t)
	r.NoError(err)

	return &fakeRedisServer{ln: ln, t: t, r: r}
}

func (s *fakeRedisServer) accept(timeout time.Duration) net.Conn {
	conn, err := s.ln.Accept()
	s.r.NoError(err)
	return conn
}

func (s *fakeRedisServer) assertReceiveAndReply(reader *internal.Reader, conn net.Conn, expected string, reply []byte, skipNBytes ...int) {
	expectedRead := len(expected)
	for _, n := range skipNBytes {
		expectedRead += n
	}
	assertReadNContains(s.t, reader, expectedRead, string(expected))
	_, err := conn.Write(reply)
	s.r.NoError(err)
}

func assertGetValue(t *testing.T, conn net.Conn, reader *internal.Reader, key, value string) {
	r := require.New(t)

	_, err := conn.Write([]byte(fmt.Sprintf("*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", len(key), key)))
	r.NoError(err)
	a1, err := reader.ReadLine()
	r.NoError(err)
	r.Equal([]byte(fmt.Sprintf("$%d", len(value))), a1)
	a2, err := reader.ReadLine()
	r.NoError(err)
	r.Equal([]byte(value), a2)
}

func assertReceiveOk(t *testing.T, reader *internal.Reader) {
	r := require.New(t)
	actual := readN(t, reader, 5)
	r.Equal([]byte(ok), actual)
}

func assertReceiveInteger(t *testing.T, conn net.Conn, expected int) {
	r := require.New(t)
	reader := internal.NewReader(conn)
	actual, err := reader.ReadInt()
	r.NoError(err)
	r.Equal(int64(expected), actual)
}

func assertGetArray(t *testing.T, reader *internal.Reader, strict bool, expected ...string) {
	r := require.New(t)
	length, err := reader.ReadArrayLen()
	r.NoError(err)
	r.Len(expected, length)

	if strict {
		for i, e := range expected {
			actual, err := reader.ReadString()
			r.NoError(err)
			r.Equal(e, actual, "index: %d", i)
		}
	} else {
		for i := 0; i < length; i++ {
			actual, err := reader.ReadString()
			r.NoError(err)
			r.Contains(expected, actual)
		}
	}
}

func assertReceiveErr(t *testing.T, reader *internal.Reader) {
	r := require.New(t)
	line, err := reader.ReadLine()
	r.NoError(err)
	r.True(strings.HasPrefix(string(line), "-ERR"), "actual: %s", string(line))
}

func assertReceiveSimpleString(t *testing.T, reader *internal.Reader, expected string) {
	r := require.New(t)
	actual, err := reader.ReadString()
	r.NoError(err)
	r.Equal(expected, string(actual))
}

func assertXRangeValue(t *testing.T, reader *internal.Reader, expected string) {
	b := make([]byte, 1024)
	n, err := reader.Read(b)
	require.NoError(t, err)
	actual := ""
	for _, s := range bytes.Split(b[:n], []byte("\r\n")) {
		actual += string(bytes.TrimRight(s, "\r\n"))
	}
	require.Equal(t, expected, actual)
}
