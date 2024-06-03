package redis_test

import (
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var redisHost string

func init() {
	redisHost = os.Getenv("TEST_REDIS_HOST")
	if redisHost == "" {
		redisHost = "localhost:6379"
	}
}

func TestBindAPort(t *testing.T) {
	t.Parallel()
	conn, err := net.Dial("tcp", redisHost)
	require.NoError(t, err)
	defer conn.Close()

	getClient()
}

func TestRespondToPING(t *testing.T) {
	t.Parallel()
	r := require.New(t)
	conn, err := net.Dial("tcp", redisHost)
	r.NoError(err)
	defer conn.Close()

	assertPing(t, conn)
}

func TestRespondToMultiplePINGs(t *testing.T) {
	t.Parallel()
	r := require.New(t)
	conn, err := net.Dial("tcp", redisHost)
	r.NoError(err)

	defer conn.Close()

	for i := 0; i < 10; i++ {
		assertPing(t, conn)
	}
}

// type reader struct {
// 	rd *bufio.Reader
// }

// func newReader(rd io.Reader) *reader {
// 	return &reader{rd: bufio.NewReader(rd)}
// }

// func (r *reader) readLine() ([]byte, error) {
// 	b, err := r.rd.ReadSlice('\n')
// 	if err != nil {
// 		if err != bufio.ErrBufferFull {
// 			return nil, err
// 		}

// 		full := make([]byte, len(b))
// 		copy(full, b)

// 		b, err = r.rd.ReadBytes('\n')
// 		if err != nil {
// 			return nil, err
// 		}

// 		full = append(full, b...) //nolint:makezero
// 		b = full
// 	}
// 	if len(b) <= 2 || b[len(b)-1] != '\n' || b[len(b)-2] != '\r' {
// 		return nil, fmt.Errorf("redis: invalid reply: %q", b)
// 	}
// 	return b[:len(b)-2], nil
// }
