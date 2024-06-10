package redis_test

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/model"
	"github.com/stretchr/testify/require"
	"github.com/sword-jin/codecrafter-test/redis/internal"
)

func TestBindAPort(t *testing.T) {
	// nothing
	conn, node := startMasterOn6379(t)
	conn.Close()
	node.close()
}

func TestRespondToPING(t *testing.T) {
	conn, node := startMasterOn6379(t)
	defer conn.Close()
	defer node.close()

	assertPing(t, conn)
}

func TestRespondToMultiplePINGs(t *testing.T) {
	conn, node := startMasterOn6379(t)
	defer conn.Close()
	defer node.close()

	for i := 0; i < 10; i++ {
		assertPing(t, conn)
	}
}

func TestHandleConcurrentClients(t *testing.T) {
	conn, node := startMasterOn6379(t)
	conn.Close()
	defer node.close()

	wg := &sync.WaitGroup{}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			time.Sleep(10 * time.Microsecond)
			defer wg.Done()
			conn := node.dial()
			defer conn.Close()

			for i := 0; i < 10; i++ {
				assertPing(t, conn)
			}
		}()
	}
	wg.Wait()
}

func TestImplementTheECHOCommand(t *testing.T) {
	conn, node := startMasterOn6379(t)
	defer conn.Close()
	defer node.close()

	r := require.New(t)
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
	conn, node := startMasterOn6379(t)
	defer conn.Close()
	defer node.close()

	r := require.New(t)
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
	conn, node := startMasterOn6379(t)
	defer conn.Close()
	defer node.close()

	r := require.New(t)
	reader := internal.NewReader(conn)

	key := genKey()

	{
		sendRedisCommand(t, conn, "GET", key)
		_, err := reader.ReadLine()
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
		_, err := reader.ReadLine()
		r.ErrorIs(err, internal.Nil)
	}
}

func TestConfigureListeningPort(t *testing.T) {
	conn, node := startMaster(t) // on a random port
	conn.Close()
	node.close()
}

func TestTheInfoCommand(t *testing.T) {
	conn, node := startMaster(t)
	defer conn.Close()
	defer node.close()

	t.Log(`I assume you reply:
# Replication
role:master`)

	r := require.New(t)

	sendRedisCommand(t, conn, "INFO")
	info := readBulkString(t, internal.NewReader(conn))
	r.Contains(info, "role:master")
}

func TestInfoCommandOnAReplica(t *testing.T) {
	_, slave, close := startMasterAndSlave(t)
	defer close()
	conn := slave.dial()
	defer conn.Close()
	sendRedisCommand(t, conn, "INFO")
	assertReadNContains(t, conn, 26+4, "role:slave")
}

func TestInitialReplicationIDAndOffset(t *testing.T) {
	conn, node := startMaster(t)
	defer conn.Close()
	defer node.close()

	r := require.New(t)

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
	port := freePort(t)
	fakeRedis := newFakeRedisServer(t, port)
	connCh := make(chan net.Conn, 1)
	go func() {
		connCh <- fakeRedis.accept(3 * time.Second)
	}()

	_, close, err := startRedisServer(3*time.Second, 36393, "--replicaof", fmt.Sprintf(`localhost %d`, port))
	r.NoError(err)

	conn := <-connCh
	return fakeRedis, conn, func() error {
		return errors.Join(fakeRedis.ln.Close(), close())
	}
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
		"$8\r\nREPLCONF\r\n$4\r\ncapa\r\n",
		[]byte(ok),
		4, // we read 4 bytes more, because the $length is uncertain
	)
	fakeRedis.assertReceiveAndReply(
		conn,
		"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n",
		[]byte(fmt.Sprintf("+FULLRESYNC %s 0\r\n", strings.Repeat("abcd", 10))))
}

func TestReceiveHandshake1(t *testing.T) {
	_, _, close := startMasterAndSlave(t)
	defer close()
}

func TestReceiveHandshake2(t *testing.T) {
	// same as TestReceiveHandshake1
	TestReceiveHandshake1(t)
}

func TestEmptyRDBTransfer(t *testing.T) {
	r := require.New(t)
	defer func() {
		if t.Failed() {
			t.Log("be careful, debug your server")
		}
	}()

	conn, master := startMaster(t)
	defer conn.Close()
	defer master.close()

	reader := internal.NewReader(conn)

	assertPing(t, conn)
	{
		_, err := conn.Write([]byte(fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n%d\r\n", 6382)))
		r.NoError(err)
		a1, err := reader.ReadLine()
		r.NoError(err)
		r.Equal([]byte("+OK"), a1)
	}
	{
		_, err := conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$9\r\neof capa2\r\n"))
		r.NoError(err)
		a1, err := reader.ReadLine()
		r.NoError(err)
		r.Equal([]byte("+OK"), a1)
	}

	{
		_, err := conn.Write([]byte("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"))
		r.NoError(err)
		line, err := reader.ReadLine()
		r.NoError(err)
		tmp := strings.Split(string(line), " ")
		r.Equal("+FULLRESYNC", tmp[0])
		r.Equal(40, len(tmp[1])) // replid
		r.Equal("0", tmp[2])     // offset always 0
	}

	// RDB transfer
	conn.(*net.TCPConn).SetReadDeadline(time.Now().Add(1 * time.Second))
	var line = make([]byte, 5)
	_, err := conn.Read(line)
	r.NoError(err)

	rdbContentLen, err := strconv.Atoi(strings.TrimSpace(string(line[1:])))
	r.NoError(err, "expecting a number after $")
	rdbContent := make([]byte, rdbContentLen)
	_, err = conn.(*net.TCPConn).Read(rdbContent)
	r.NoError(err)

	decoder := core.NewDecoder(bytes.NewReader(rdbContent))
	err = decoder.Parse(func(object model.RedisObject) bool {
		r.False(true, "should be a empty rdb file")
		return true
	})
	r.NoError(err)
}

func TestSingleReplicaPropagation(t *testing.T) {
	testMultiReplicaPropagation(t, 1, false)
}

func TestMultiReplicaPropagation(t *testing.T) {
	slaveCount := 5
	testMultiReplicaPropagation(t, slaveCount, false)
}

func TestCommandProcessing(t *testing.T) {
	testMultiReplicaPropagation(t, 1, true)
}

func testMultiReplicaPropagation(t *testing.T, count int, verifyReplica bool) {
	t.Logf("I will start %d slaves and a master, and set some values on the master", count)
	if verifyReplica {
		t.Log("then verify the values on the slaves.")
	}

	r := require.New(t)
	masterConn, master := startMaster(t)
	defer masterConn.Close()
	defer master.close()

	conns := make([]net.Conn, count)
	for i := 0; i < count; i++ {
		var slave nodeInfo
		conns[i], slave = master.startSlave()
		defer slave.close()
		defer conns[i].Close()
	}

	values := map[string]int{
		"foo": 1,
		"bar": 2,
		"baz": 3,
	}
	reader := internal.NewReader(masterConn)
	for key, value := range values {
		_, err := masterConn.Write([]byte(fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%d\r\n", len(key), key, len(strconv.Itoa(value)), value)))
		r.NoError(err)
		a1, err := reader.ReadLine()
		r.NoError(err)
		r.Equal([]byte("+OK"), a1)
	}

	// verify
	if !verifyReplica {
		return
	}

	for _, conn := range conns {
		defer conn.Close()
		for key, value := range values {
			assertGetValue(t, conn, key, strconv.Itoa(value))
		}
	}
}

var fullResyncRegex = regexp.MustCompile(`\+FULLRESYNC (?P<replid>\w{40}) (?P<offset>\d+)\r\n`)

func TestACKsWithNoCommands(t *testing.T) {
	t.Logf(`I need to start a master and a slave by your program, I have nothing to assert in this test.
	Because from now on, nothing exposes to me in the ACKs feature,
	Check the next test`)
	conn, master := startMaster(t)
	defer conn.Close()
	defer master.close()

	sendRedisCommand(t, conn, "PSYNC", "?", "-1")
	actual := readN(t, conn, 56)
	require.True(t, fullResyncRegex.Match(actual))
}

func TestACKsWithCommands(t *testing.T) {
	t.Logf(`I need to start a master and a slave by your program, I have nothing to assert in this test.
	Because from now on, nothing exposes to me in the ACKs feature,
	check the next test`)
}

func TestWithNoReplicas(t *testing.T) {
	t.Logf("Same reason as the last two tests, I only send a WAIT command to the master")
	conn, master := startMaster(t)
	defer conn.Close()
	defer master.close()
	sendRedisCommand(t, conn, "WAIT", "0", "60000")
	actual := readN(t, conn, 1)
	require.Equal(t, []byte(`0`), actual)
}
