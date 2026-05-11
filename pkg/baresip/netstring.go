package baresip

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// Netstring frames per http://cr.yp.to/proto/netstrings.txt: <len>:<data>,

const maxNetstringLen = 1 << 20

func writeNetstring(w io.Writer, data []byte) error {
	_, err := fmt.Fprintf(w, "%d:", len(data))
	if err != nil {
		return err
	}
	if _, err := w.Write(data); err != nil {
		return err
	}
	_, err = w.Write([]byte{','})
	return err
}

func readNetstring(r *bufio.Reader) ([]byte, error) {
	header, err := r.ReadString(':')
	if err != nil {
		return nil, err
	}
	lenStr := header[:len(header)-1]
	n, err := strconv.Atoi(lenStr)
	if err != nil {
		return nil, fmt.Errorf("invalid netstring length %q: %w", lenStr, err)
	}
	if n < 0 || n > maxNetstringLen {
		return nil, fmt.Errorf("netstring length out of range: %d", n)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	term, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	if term != ',' {
		return nil, errors.New("netstring: missing trailing comma")
	}
	return buf, nil
}
