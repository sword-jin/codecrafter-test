package redis_test

import (
	"fmt"
	"net"
	"os"
	"strings"
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

func TestInitialReplicationIDAndOffset(t *testing.T) {
	r := require.New(t)
	conn := dialConn(t, 6379)
	defer conn.Close()

	sendRedisCommand(t, conn, "INFO")
	s := readBulkString(t, internal.NewReader(conn))
	lines := strings.Split(s, "\n")
	r.Equal("# Replication", lines[0])
	r.Equal("role:master", lines[1])
	r.True(strings.HasPrefix(lines[2], "master_replid:"))
	r.Equal(len("master_replid:")+40, len(lines[2]))
	r.Equal("master_repl_offset:0", strings.TrimSpace(lines[3]))
}

// assume 36392 is not used
func startRedisServerWithAFakeMaster(t *testing.T) (*fakeRedisServer, net.Conn, func() error) {
	r := require.New(t)
	fakeRedis := newFakeRedisServer(t, 36382)
	connCh := make(chan net.Conn, 1)
	go func() {
		connCh <- fakeRedis.accept(3 * time.Second)
	}()

	close, err := startRedisServer(3*time.Second, 36393, func() []string {
		return []string{"--replicaof", `localhost 36382`}
	})
	r.NoError(err)

	conn := <-connCh
	return fakeRedis, conn, close
}

func TestSendHandshake1(t *testing.T) {
	fakeRedis, conn, close := startRedisServerWithAFakeMaster(t)
	defer close()
	defer conn.Close()

	fakeRedis.assertReceiveAndReply(conn, ping, []byte(pong))
}

func TestSendHandshake2(t *testing.T) {
	fakeRedis, conn, close := startRedisServerWithAFakeMaster(t)
	defer close()
	defer conn.Close()

	fakeRedis.assertReceiveAndReply(conn, ping, []byte(pong))
	fakeRedis.assertReceiveAndReply(
		conn,
		fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$5\r\n%d\r\n", 36393),
		[]byte(ok))
}

func TestSendHandshake3(t *testing.T) {
	fakeRedis, conn, close := startRedisServerWithAFakeMaster(t)
	defer close()
	defer conn.Close()

	fakeRedis.assertReceiveAndReply(conn, ping, []byte(pong))
	fakeRedis.assertReceiveAndReply(
		conn,
		fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$5\r\n%d\r\n", 36393),
		[]byte(ok))
	fakeRedis.assertReceiveAndReply(
		conn,
		"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n",
		[]byte(ok))
}

func TestReceiveHandshake1(t *testing.T) {
	r := require.New(t)
	close, err := startRedisServer(3*time.Second, 6382, func() []string {
		return []string{"--replicaof", `localhost 6379`}
	})
	r.NoError(err)
	close()

	cmd := newStartRedisServerCmd(6382, func() []string {
		return []string{"--replicaof", `localhost 26666`} // use a not existing port
	})
	r.NoError(cmd.Start())
	r.Error(cmd.Wait())
	r.Equal("exit status 1", cmd.ProcessState.String())
}

func TestReceiveHandshake2(t *testing.T) {
	TestReceiveHandshake1(t)
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
	s.ln.(*net.TCPListener).SetDeadline(time.Now().Add(timeout))
	conn, err := s.ln.Accept()
	s.r.NoError(err)
	return conn
}

func (s *fakeRedisServer) assertReceiveAndReply(conn net.Conn, expected string, reply []byte) {
	conn.(*net.TCPConn).SetDeadline(time.Now().Add(3 * time.Second))
	assertReadNContains(s.t, conn, len(expected), string(expected))
	_, err := conn.Write(reply)
	s.r.NoError(err)
}
