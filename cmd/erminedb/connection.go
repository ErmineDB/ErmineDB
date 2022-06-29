package erminedb

import (
	"bufio"
	"errors"
	"net"
	"strconv"
	"strings"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"

	"github.com/ErmineDB/ErmineDB/internal/formatter"
	"github.com/ErmineDB/ErmineDB/internal/helpers"
	"github.com/gobwas/glob"
)

type argObj struct {
	command string
	args    []string
}

type ProtoHandler struct {
	S *Server
}

func NewProtoHandler(s *Server) *ProtoHandler {
	return &ProtoHandler{S: s}
}

func procArgs(data []string, minArgs int) (*argObj, error) {
	obj := argObj{command: data[1]}

	i := 0
	for _, v := range data {
		if i > 1 {
			if i%2 == 0 {

			} else {
				obj.args = append(obj.args, v)
			}
		}
		i += 1
	}

	if len(obj.args) < minArgs {
		message := errors.New("-ERR wrong number of arguments for '" + obj.command + "' command")
		return &obj, message
	}
	return &obj, nil
}

func HandleConnection(conn net.Conn, db *ErmineDB) {
	// log.Info().Msg("New connection")
	defer conn.Close()

	for {
		reader := bufio.NewReaderSize(conn, 4096)

		var data []byte
		var err error

		_, err = reader.Peek(6) // *123\r\n
		if err != nil {
			break
		}

		var byteSize = reader.Buffered()
		data = make([]byte, byteSize)
		reader.Read(data)

		dataString := string(data[:])
		if len(dataString) > 0 {
			splitData := helpers.Parsedata(string(data[:]))
			if len(splitData) > 1 {
				// command := strings.ToLower(splitData[1])
				command := cases.Lower(language.Und).String(splitData[1])
				// command = strings.Title(command)
				command = cases.Title(language.Und).String(command)
				// log.Info().Msgf("Command: %s", command)
				switch command {
				case "Del":
					returnMsg := keyDelete(db, splitData)
					conn.Write([]byte(returnMsg))
				case "Get":
					returnMsg := keyGet(db, splitData)
					conn.Write([]byte(returnMsg))
				case "Keys":
					returnMsg := keyKey(db, splitData)
					conn.Write([]byte(returnMsg))
				case "Ping":
					if len(splitData) == 2 {
						conn.Write([]byte("+PONG\r\n"))
					} else {
						returnMsg := formatter.SimpleString(splitData[3])
						conn.Write([]byte(returnMsg))
					}
				case "Set":
					returnMsg := stringSet(db, splitData)
					conn.Write([]byte(returnMsg))
				default:
					conn.Write([]byte("$-1\r\n"))
				}
			}
		}
	}
}

func keyDelete(db *ErmineDB, data []string) string {
	_, err := procArgs(data, 1)
	if err != nil {
		return formatter.BulkString(err.Error())
	}

	deleted := 0

	for i := 3; i < len(data); i += 2 {
		err := db.Update(func(tx *Tx) error {
			err := tx.Delete(data[i])
			return err
		})
		if err == nil {
			deleted += 1
		}
	}
	return formatter.Integer(int64(deleted))
}

func keyGet(db *ErmineDB, data []string) string {
	argobj, err := procArgs(data, 1)
	if err != nil {
		return formatter.BulkString(err.Error())
	}

	var val string
	err = db.View(func(tx *Tx) error {
		val, err = tx.Get(argobj.args[0])
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return "$-1\r\n"
	}

	return formatter.BulkString(val)
}

func keyKey(db *ErmineDB, data []string) string {
	argobj, err := procArgs(data, 1)
	if err != nil {
		return formatter.BulkString(err.Error())
	}

	var keys []string
	err = db.View(func(tx *Tx) error {
		keys = tx.Keys()
		return nil
	})
	if err != nil {
		return "$-1\r\n"
	}

	var matched []string
	var g glob.Glob = glob.MustCompile(argobj.args[0])
	for _, v := range keys {
		if g.Match(v) {
			matched = append(matched, v)
		}
	}
	return formatter.List(matched)
	// var n uint16 = 65535
	// keys, _ := h.S.Store.KeysOf([]byte(""), []byte("0"), n)

	// var matched []string
	// var g glob.Glob

	// g = glob.MustCompile(bucketName + argobj.args[0])

	// for _, k := range keys.Keys {
	// 	if g.Match(k) {
	// 		key := strings.TrimPrefix(k, bucketName)
	// 		matched = append(matched, key)
	// 	}
	// }
	// return formatter.List(matched)
}

func stringSet(db *ErmineDB, data []string) string {
	argobj, err := procArgs(data, 2)
	if err != nil {
		return formatter.BulkString(err.Error())
	}

	if len(data) > 6 {
		var exp int64
		var upperData []string
		for _, v := range data {
			upperData = append(upperData, strings.ToUpper(v))
		}

		if helpers.Contains(data, "EX", true) {
			if helpers.Contains(data, "PX", false) || helpers.Contains(data, "EXAT", false) || helpers.Contains(data, "PXAT", false) || helpers.Contains(data, "KEEPTTL", false) {
				return "-ERR syntax error\r\n"
			}
			idx := helpers.IndexOf(upperData, "EX")
			exp, _ = strconv.ParseInt(data[idx+2], 10, 64)
			err := db.Update(func(tx *Tx) error {
				err := tx.SetEx(argobj.args[0], data[5], exp)
				return err
			})
			if err != nil {
				return formatter.BulkString(err.Error())
			}
			return formatter.SimpleString("OK")

		}
	} else if len(data) == 6 {
		err := db.Update(func(tx *Tx) error {
			err := tx.Set(argobj.args[0], data[5])
			return err
		})
		if err != nil {
			return formatter.BulkString(err.Error())
		}
		return formatter.SimpleString("OK")
	} else {
		return "-ERR syntax error\r\n"
	}
	return "-ERR syntax error\r\n"
}
