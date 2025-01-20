package resp

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

const (
	STRING  = '+'
	ERROR   = '-'
	INTEGER = ':'
	BULK    = '$'
	ARRAY   = '*'
)

// This struct will support in serialization and deserialization process for
// RESP (Redis Serialization Protocol)
type Value struct {
	Typ   string
	Str   string
	Num   int
	Bulk  string
	Array []Value
}

func (v Value) String() any {
	panic("unimplemented")
}

func (v Value) Marshal() []byte {

	// For writing data back, we need to Marshal the data into RESP. We are doing
	// this based on the type and calling specific methods for each

	switch v.Typ {
	case "array":
		return v.marshalArray()
	case "bulk":
		return v.marshalBulk()
	case "string":
		return v.marshalString()
	case "null":
		return v.marshalNull()
	case "error":
		return v.marshalError()
	case "integer":
		return v.marshalInteger()
	default:
		return []byte{}
	}
}

func (v Value) marshalArray() []byte {
	var bytes []byte
	length := len(v.Array)
	bytes = append(bytes, ARRAY)
	bytes = append(bytes, strconv.Itoa(length)...)
	bytes = append(bytes, '\r', '\n')

	for i := 0; i < length; i++ {
		bytes = append(bytes, v.Array[i].Marshal()...)
	}

	return bytes
}

func (v Value) marshalBulk() []byte {
	var bytes []byte
	bytes = append(bytes, BULK)
	bytes = append(bytes, strconv.Itoa(len(v.Bulk))...)
	bytes = append(bytes, '\r', '\n') // CRLF for RESP
	bytes = append(bytes, v.Bulk...)
	bytes = append(bytes, '\r', '\n') // CRLF for RESP

	return bytes
}

func (v Value) marshalString() []byte {
	var bytes []byte
	bytes = append(bytes, STRING)     // Adding the string type
	bytes = append(bytes, v.Str...)   // Adding the string bytes
	bytes = append(bytes, '\r', '\n') // Adding the CRLF for redis-cli to understand

	return bytes
}

func (v Value) marshalNull() []byte {
	return []byte("$-1\r\n") // This is the null representation according to RESP
}

func (v Value) marshalError() []byte {
	var bytes []byte
	bytes = append(bytes, ERROR)
	bytes = append(bytes, v.Str...)
	bytes = append(bytes, '\r', '\n') // CRLF for RESP

	return bytes
}

// marshalInteger converts an integer value to a RESP-formatted integer reply.
func (v Value) marshalInteger() []byte {
	return []byte(fmt.Sprintf(":%d\r\n", v.Num)) // The format is ":<integer>\r\n" as specified by the Redis Serialization Protocol.
}

type Resp struct {
	reader *bufio.Reader
}

func NewResp(rd io.Reader) *Resp {
	// The buffer created during the connection to PORT 6379 would be passed to
	// this function for generating responses
	return &Resp{
		reader: bufio.NewReader(rd),
	}
}

func (r *Resp) readLine() (line []byte, n int, err error) {
	// Read line from buffer. We read one byte at a time until we reach '\r',
	// which indicates the end of the line. Then we return the line without the
	// last two bytes, which are '\r\n' and number of bytes in the line.
	for {
		b, err := r.reader.ReadByte()
		if err != nil {
			return nil, 0, err
		}
		n += 1

		line = append(line, b)

		// At any point if the second last character of the line buffer is carriage
		// return then we can break out of the for loop and return the line by
		// trimming out the last two characters.
		if len(line) >= 2 && line[len(line)-2] == '\r' {
			break
		}
	}

	return line[:len(line)-2], n, nil
}

func (r *Resp) readInteger() (x int, n int, err error) {
	line, n, err := r.readLine()
	if err != nil {
		return 0, 0, err
	}

	// After reading the line successfully, we try to convert the required string
	// bits into an integer with base 10 and size 64. Otherwise, we then further
	// typecase the 64 bit integer to int-type which is system default and return.

	i64, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return 0, n, err
	}
	return int(i64), n, nil
}

func (r *Resp) Read() (Value, error) {
	// Method to read from the buffer recursively. This is needed to read the
	// Value again and again for each step of the input we received so that we
	// can parse it according to the character at the beginning of the line.

	_type, err := r.reader.ReadByte()
	if err != nil {
		return Value{}, err
	}

	// Read the first byte to determine the RESP type which is to be parsed as per
	// the switch statement.
	switch _type {
	case ARRAY:
		return r.readArray()
	case BULK:
		return r.readBulk()
	case STRING:
		return r.readString()
	case INTEGER:
		return r.readIntegerVal()
	case ERROR:
		return r.readError()
	default:
		fmt.Printf("Unknown type: %v", string(_type))
		return Value{}, nil
	}
}

func (r *Resp) readArray() (v Value, err error) {

	// Steps for reading the Array:
	// 1. Skip the first byte because we have already read it in the Read method
	// 2. Read the integer that represents the number of elements in the array
	// 3. Iterate over the array and for each line, call the Read method to parse
	// the type according too the character at the beginning of the line
	// 4. With each iteration, append the parsed value to the array in the Value
	// object and return it

	v.Typ = "array"

	length, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	// for each line, parse and read the Value
	v.Array = make([]Value, length)
	for i := 0; i < length; i++ {
		val, err := r.Read()
		if err != nil {
			return v, err
		}

		// add parsed value to array
		v.Array[i] = val
	}

	return v, nil
}

func (r *Resp) readBulk() (v Value, err error) {
	// Steps for reading Bulk data:
	// 1. Skip the first byte because we have already read it in the Read method
	// 2. Read the integer that represents the number of bytes in the bulk string
	// 3. Read the bulk string, followed by the '\r\n' that indicates the end of
	// the bulk string
	// 4. Return the Value object

	v.Typ = "bulk"
	length, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	if length == -1 {
		v.Typ = "null"
		return v, nil
	}

	bulk := make([]byte, length)
	_, err = io.ReadFull(r.reader, bulk)
	if err != nil {
		return v, err
	}
	v.Bulk = string(bulk)

	// Read the trailing CRLF so that the pointer is effectively moved to the
	// next bulk string correctly. Otherwise, the pointer would be stuck at '\r'
	// and Read method would not work properly
	r.readLine()

	return v, nil
}

func (r *Resp) readString() (v Value, err error) {
	v.Typ = "string"
	line, _, err := r.readLine()
	if err != nil {
		return v, err
	}
	v.Str = string(line)
	return v, nil
}

func (r *Resp) readIntegerVal() (v Value, err error) {
	v.Typ = "integer"
	num, _, err := r.readInteger()
	if err != nil {
		return v, err
	}
	v.Num = num
	return v, nil
}

func (r *Resp) readError() (v Value, err error) {
	v.Typ = "error"
	line, _, err := r.readLine()
	if err != nil {
		return v, err
	}
	v.Str = string(line)
	return v, nil
}

type Writer struct {
	writer io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{
		writer: w,
	}
}

func (w *Writer) Write(value Value) error {
	// Get all the required bytes after marshalling and write everything to the
	// io.Writer provided in the function. Could be a file or a stdout
	respData := value.Marshal()
	_, err := w.writer.Write(respData)
	return err
}
