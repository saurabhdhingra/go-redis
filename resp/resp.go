package resp

import (
	"bufio"
	"fmt"
	"io"
	"strings"
	"strconv"
)

type Resp struct {
	reader *bufio.Reader
}

func NewResp(rd io.Reader) *Resp {
	return &Resp{reader: bufio.NewReader(rd)}
}

func (r *Resp) readLine() (string, error) {
	line, err := r.reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(line, "\r\n"), nil
}

func (r *Resp) Read() (Value, error) {
	_type, err := r.reader.ReadByte()
	if err != nil {
		return Value{}, err
	}

	switch _type {
	case '+': // Simple Strings
		s, err := r.readLine()
		return Value{Type: "string", Str: s}, err
	case '-': // Errors
		s, err := r.readLine()
		return Value{Type: "error", Str: s}, err
	case ':': // Integers
		s, err := r.readLine()
		i, err := strconv.Atoi(s)
		return Value{Type: "integer", Num: i}, err
	case '$': // Bulk Strings
		sLen, err := r.readLine()
		if err != nil {
			return Value{}, err
		}
		length, err := strconv.Atoi(sLen)
		if err != nil {
			return Value{}, err
		}

		bulk := make([]byte, length)
		_, err = io.ReadFull(r.reader, bulk)
		if err != nil {
			return Value{}, err
		}
		_, err = r.readLine() // Read the trailing CRLF
		if err != nil {
			return Value{}, err
		}
		return Value{Type: "bulk", Bulk: string(bulk)}, nil
	case '*': // Arrays
		sLen, err := r.readLine()
		if err != nil {
			return Value{}, err
		}
		length, err := strconv.Atoi(sLen)
		if err != nil {
			return Value{}, err
		}

		array := make([]Value, length)
		for i := 0; i < length; i++ {
			val, err := r.Read()
			if err != nil {
				return Value{}, err
			}
			array[i] = val
		}
		return Value{Type: "array", Array: array}, nil
	default:
		return Value{}, fmt.Errorf("unknown RESP type: %v", string(_type))
	}
}

func Respond(writer io.Writer, val Value) error{
	switch val.Type {
	case "string": 
		_, err := fmt.Fprintf(writer, "+%s\r\n", val.Str)
		return err
	case "error":
		_, err := fmt.Fprintf(writer, "-%s\r\n", val.Str)
		return err
	case "integer":
		_, err := fmt.Fprintf(writer, ":%d\r\n", val.Num)
		return err
	case "bulk":
		_, err := fmt.Fprintf(writer, "$%d\r\n%s\r\n", len(val.Bulk), val.Bulk)
		return err
	case "array":
		_, err := fmt.Fprintf(writer, "*%d\r\n", len(val.Array))
		if err != nil {
			return err
		}
		for _, v := range val.Array {
			err := Respond(writer, v)
		if err != nil {
			return err
		}
		}
		return nil
	case "nil":
		_, err := fmt.Fprintf(writer, "$-1\r\n")
		return err
	default: 
		return fmt.Errorf("unknown RESP type to respond: %s", val.Type)
	}
}