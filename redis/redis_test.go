package redis_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
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
	port := freePort(t)
	fakeRedis := newFakeRedisServer(t, port)
	connCh := make(chan net.Conn, 1)
	go func() {
		connCh <- fakeRedis.accept(3 * time.Second)
	}()

	conn, node := startNode(t, 36393, "--replicaof", fmt.Sprintf(`localhost %d`, port))
	defer conn.Close()

	return fakeRedis, <-connCh, func() error {
		return errors.Join(fakeRedis.ln.Close(), node.close())
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
	testMultiReplicaPropagation(t, 1, 20, false)
}

func TestMultiReplicaPropagation(t *testing.T) {
	slaveCount := 5
	testMultiReplicaPropagation(t, slaveCount, 3, false)
}

func TestCommandProcessing(t *testing.T) {
	testMultiReplicaPropagation(t, 1, 2, true)
	testMultiReplicaPropagation(t, 5, 10, true)
	testMultiReplicaPropagation(t, 10, 2, true)
}

func testMultiReplicaPropagation(t *testing.T, replicasN int, commandN int, verifyReplica bool) {
	t.Logf("I will start %d slaves and a master, and set some values on the master", replicasN)
	if verifyReplica {
		t.Log("then verify the values on the slaves.")
	}

	r := require.New(t)
	masterConn, master := startMasterOn6379(t)

	conns := make([]net.Conn, replicasN)
	slaves := make([]nodeInfo, replicasN)
	wg := &sync.WaitGroup{}
	wg.Add(replicasN)
	for i := 0; i < replicasN; i++ {
		go func(i int) {
			defer wg.Done()
			conns[i], slaves[i] = master.startSlave()
		}(i)
	}
	wg.Wait()

	for i := 0; i < replicasN; i++ {
		if conns[i] == nil {
			t.Skipf("slave %d is not started, please run again", i)
		}
	}

	var failedSlaveIndex int
	defer func() {
		time.Sleep(100 * time.Millisecond)
		for _, slave := range slaves {
			slave.close()
		}
		if t.Failed() {
			fmt.Printf("slave %d output:\n", slaves[failedSlaveIndex].port)
			println(slaves[failedSlaveIndex].output.String())
		}
	}()

	// close master first
	defer masterConn.Close()
	defer master.close()
	defer func() {
		if t.Failed() {
			t.Log("master redis output:")
			println(master.output.String())
		}
	}()

	time.Sleep(1 * time.Second)

	items := make([][2]string, 0, commandN)
	for i := 0; i < commandN; i++ {
		key := fmt.Sprintf("key%d", i)
		items = append(items, [2]string{key, fmt.Sprintf("%d", i)})
	}
	time.Sleep(1 * time.Second)

	reader := internal.NewReader(masterConn)
	for _, item := range items {
		key, value := item[0], item[1]
		sendRedisCommand(t, masterConn, "SET", key, value)
		a1, err := reader.ReadLine()
		r.NoError(err)
		r.Equal([]byte("+OK"), a1)
	}

	// verify
	if !verifyReplica {
		return
	}

	defer func() {
		for _, conn := range conns {
			conn.Close()
		}
	}()

	var successed = map[int]bool{}
	require.Eventually(t, func() bool {
		for i, conn := range conns {
			if successed[i] {
				continue
			}
			for _, item := range items {
				key, value := item[0], item[1]
				reader := internal.NewReader(conn)
				sendRedisCommand(t, conn, "GET", key)
				a1, err := reader.ReadString()
				if err != nil {
					failedSlaveIndex = i
					t.Logf("server: %s, get %s: %v", conn.RemoteAddr().String(), key, err)
					return false
				}
				if a1 != value {
					t.Logf("get key %s, expect %s, got %s", key, value, a1)
					return false
				}
			}
			successed[i] = true
		}
		return true
	}, 1*time.Minute, 1*time.Second)
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
	conn, master := startMaster(t)
	defer conn.Close()
	defer master.close()
	sendRedisCommand(t, conn, "WAIT", "0", "60000")
	reader := internal.NewReader(conn)
	actual, err := reader.ReadInt()
	require.NoError(t, err)
	require.Equal(t, int64(0), actual)
}

func TestWaitWithNoCommands(t *testing.T) {
	masterConn, master := startMaster(t)
	defer masterConn.Close()
	defer master.close()

	var replicas_count int
	for {
		replicas_count = rand.IntN(10)
		if replicas_count > 0 {
			break
		}
	}
	t.Logf("I will start %d slaves and a master, and send WAIT commands to the master", replicas_count)
	for i := 0; i < replicas_count; i++ {
		conn, slave := master.startSlave()
		defer slave.close()
		defer conn.Close()
	}

	sendRedisCommand(t, masterConn, "WAIT", "1", "500")
	assertReceiveInteger(t, masterConn, replicas_count)
	sendRedisCommand(t, masterConn, "WAIT", "3", "500")
	assertReceiveInteger(t, masterConn, replicas_count)
	sendRedisCommand(t, masterConn, "WAIT", "5", "500")
	assertReceiveInteger(t, masterConn, replicas_count)
	sendRedisCommand(t, masterConn, "WAIT", "7", "500")
	assertReceiveInteger(t, masterConn, replicas_count)
	sendRedisCommand(t, masterConn, "WAIT", "9", "500")
	assertReceiveInteger(t, masterConn, replicas_count)
}

func TestWaitWithMultipleCommands(t *testing.T) {
	r := require.New(t)

	masterConn, master := startMaster(t)
	masterReader := internal.NewReader(masterConn)
	defer masterConn.Close()
	defer master.close()

	var replicas_count int
	for {
		replicas_count = rand.IntN(10)
		if replicas_count > 4 {
			break
		}
	}
	brokenSlaveCount := replicas_count / 2
	t.Logf("I will start %d mock slaves(%d of them are broken) and a master, and send WAIT commands to the master", replicas_count, brokenSlaveCount)

	mockSlaves := make([]net.Conn, replicas_count)
	for i := 0; i < replicas_count; i++ {
		conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", master.port))
		r.NoError(err)
		mockSlaves[i] = conn
		sendRedisCommand(t, conn, "PSYNC", "?", "-1")
		actual := readN(t, conn, 56)
		r.True(fullResyncRegex.Match(actual))
	}

	sendRedisCommand(t, masterConn, "SET", "foo", "123")
	ok, err := masterReader.ReadLine()
	r.NoError(err)
	r.Equal("+OK", string(ok))

	for i, conn := range mockSlaves {
		if i >= brokenSlaveCount {
			// normal slave
			sendRedisCommand(t, conn, "REPLCONF", "ACK", "31") // "set foo 123" is 31 bytes
		}
	}

	r.Eventually(func() bool {
		sendRedisCommand(t, masterConn, "WAIT", fmt.Sprintf("%d", replicas_count), "500")
		synced, err := masterReader.ReadInt()
		r.NoError(err)
		t.Logf("checking, synced %d", synced)
		return synced == int64(replicas_count-brokenSlaveCount)
	}, 10*time.Second, 100*time.Millisecond)
}

/**********************************************
 *
 *             RDB PERSISTENCE
 *
 **********************************************/
func TestRDBFileConfig(t *testing.T) {
	dir := "/tmp/redis-files"
	filename := "dump.rdb"
	conn, master := startNode(t, freePort(t), "--dir", dir, "--dbfilename", filename)
	defer conn.Close()
	defer master.close()
	sendRedisCommand(t, conn, "CONFIG", "GET", "dir")
	assertGetArray(t, conn, true, "dir", dir)
	sendRedisCommand(t, conn, "CONFIG", "GET", "dbfilename")
	assertGetArray(t, conn, true, "dbfilename", filename)
	sendRedisCommand(t, conn, "CONFIG", "GET", "other")
	assertGetArray(t, conn, true)
}

// We don't test with any other types, only string and the value is also string
// because the int value is also has complicated encoding
func TestReadAKey(t *testing.T) {
	t.Log("in this test, we only has one key <string, string>")
	r := require.New(t)
	b, err := os.ReadFile("testdata/a_string.rdb")
	r.NoError(err)
	var dir = t.TempDir()
	r.NoError(os.WriteFile(dir+"/dump.rdb", b, 0644))

	conn, node := startNode(t, freePort(t), "--dir", dir, "--dbfilename", "dump.rdb")
	defer conn.Close()
	defer node.close()

	sendRedisCommand(t, conn, "KEYS", "*")
	assertGetArray(t, conn, true, "foo")
}

func TestReadAStringValue(t *testing.T) {
	t.Log("in this test, we only has one key <string, string>, and test the value")

	r := require.New(t)
	b, err := os.ReadFile("testdata/a_string.rdb")
	r.NoError(err)
	var dir = t.TempDir()
	r.NoError(os.WriteFile(dir+"/dump.rdb", b, 0644))

	conn, node := startNode(t, freePort(t), "--dir", dir, "--dbfilename", "dump.rdb")
	defer conn.Close()
	defer node.close()

	sendRedisCommand(t, conn, "KEYS", "*")
	assertGetArray(t, conn, false, "foo")
	assertGetValue(t, conn, "foo", "bar")
}

func TestMultipleKeys(t *testing.T) {
	t.Log("in this test, we only has multiple keys <string, string>")

	r := require.New(t)
	b, err := os.ReadFile("testdata/multiple_string.rdb")
	r.NoError(err)
	var dir = t.TempDir()
	r.NoError(os.WriteFile(dir+"/dump.rdb", b, 0644))

	conn, node := startNode(t, freePort(t), "--dir", dir, "--dbfilename", "dump.rdb")
	defer conn.Close()
	defer node.close()

	sendRedisCommand(t, conn, "KEYS", "*")
	assertGetArray(t, conn, false, "foo", "bar")
}

func TestMultipleStringValues(t *testing.T) {
	t.Log("in this test, we only has multiple keys <string, string>, and test the value")
	r := require.New(t)
	b, err := os.ReadFile("testdata/multiple_string.rdb")
	r.NoError(err)
	var dir = t.TempDir()
	r.NoError(os.WriteFile(dir+"/dump.rdb", b, 0644))

	conn, node := startNode(t, freePort(t), "--dir", dir, "--dbfilename", "dump.rdb")
	defer conn.Close()
	defer node.close()

	assertGetValue(t, conn, "foo", "bar")
	assertGetValue(t, conn, "bar", "baz")
}

// This is the commands to generate the rdb file
// 127.0.0.1:16379> set bar baz PX 10240
// OK
// 127.0.0.1:16379> set loz sha
// OK
// 127.0.0.1:16379> set foo bar EX 3
// OK
// 127.0.0.1:16379> save
// OK
func TestReadValueWithExpiry(t *testing.T) {
	t.Log("in this test, we only has two key <string, string>, one with expiry, one not")

	r := require.New(t)
	b, err := os.ReadFile("testdata/expires_string.rdb")
	r.NoError(err)

	after3Seconds := time.Now().Add(3 * time.Second).UnixMilli()
	barTimestampBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(barTimestampBytes, uint64(after3Seconds))

	after10Seconds := time.Now().Add(10 * time.Second).UnixMilli()
	fooTimestampBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(fooTimestampBytes, uint64(after10Seconds))

	b = bytes.Replace(b,
		[]byte{0xFC, 0x3d, 0xb4, 0x2c, 0x10, 0x90, 0x01, 0x00, 0x00}, // the fixed expiry time for 'bar'
		append([]byte{0xFC}, barTimestampBytes...),
		1)
	b = bytes.Replace(b,
		[]byte{0xFC, 0x68, 0xa8, 0x2c, 0x10, 0x90, 0x01, 0x00, 0x00}, // the fixed expiry time for 'foo'
		append([]byte{0xFC}, fooTimestampBytes...),
		1)

	var dir = t.TempDir()
	r.NoError(os.WriteFile(dir+"/dump.rdb", b, 0644))

	conn, node := startNode(t, freePort(t), "--dir", dir, "--dbfilename", "dump.rdb")
	defer conn.Close()
	defer node.close()

	defer func() {
		if t.Failed() {
			println(node.output.String())
		}
	}()

	assertGetValue(t, conn, "foo", "bar")
	assertGetValue(t, conn, "bar", "baz")

	reader := internal.NewReader(conn)
	r.Eventually(func() bool {
		// bar should be expired
		sendRedisCommand(t, conn, "GET", "bar")
		_, err := reader.ReadLine()
		return err == nil
	}, 5*time.Second, 500*time.Millisecond)

	sendRedisCommand(t, conn, "GET", "foo")
	_, err = reader.ReadLine()
	r.NoError(err)
}

/**********************************************
 *
 *             STREAMS
 *
 **********************************************/
func TestTypeCommand(t *testing.T) {
	conn, node := startMaster(t)
	defer conn.Close()
	defer node.close()

	sendRedisCommand(t, conn, "SET", "some_key", "foo")
	assertReceiveOk(t, conn)
	sendRedisCommand(t, conn, "TYPE", "some_key")
	actual := readN(t, conn, 9)
	require.Equal(t, []byte("+string\r\n"), actual)
}
