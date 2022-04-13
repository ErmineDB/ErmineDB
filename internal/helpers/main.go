package helpers

import (
    "log"
    "net"
    "sync"
    "strings"
    "github.com/google/uuid"
    "github.com/dgraph-io/badger/v3"
)

type Client struct {
    Db      string
    Socket  net.Conn
    Subs    []string
    Uuid    uuid.UUID
}

// var Mutex = &sync.Mutex{}
// var Manager = make(map[uuid.UUID]Client)
var Manager = sync.Map{}
var (
    ClientManager = make(map[uuid.UUID]Client)
)

type Channel struct {
    Name string
    Clients []Client
}
// var Channels []*Channel

func Parsedata(data string) []string {
    dataTypes := []string{"+", "-", ":", "$", "*"}
    // For Simple Strings the first byte of the reply is "+"
    // For Errors the first byte of the reply is "-"
    // For Integers the first byte of the reply is ":"
    // For Bulk Strings the first byte of the reply is "$"
    // For Arrays the first byte of the reply is "*"

    var splitData []string

    if len(data) == 0 {
        return splitData
    }

    dataC1 := string([]rune(data)[0])
    if Contains(dataTypes, dataC1) {
        // This must be a single command
        splitData = strings.Split(data[4:], "\r\n")
        if splitData[len(splitData) - 1] == "" {
            splitData = RemoveIndex(splitData, len(splitData) - 1)
        }
    }
    //else {
        // This must be a pipeline or bad data
    //}
//     log.Printf("splitData in helpers %v", splitData)
//     log.Printf("%v", len(splitData))
    return splitData
}

// returns true or false if str is in array s
func Contains(s []string, str string) bool {
    for _, v := range s {
        if v == str {
            return true
        }
    }
    return false
}

// RemoveIndex removes the item located at index in s
// returns the updated array
func RemoveIndex(s []string, index int) []string {
	return append(s[:index], s[index+1:]...)
}

// IndexOf returns the first index of str in s
// or -1 if not found
func IndexOf(s []string, str string) int {
    for i, v := range s {
        if v == str {
            return i
        }
    }
    return -1
}


// Find key in interface (recursively) and return value as interface
func FindInterface(obj interface{}, key string) (interface{}, bool) {

	//if the argument is not a map, ignore it
    mobj, ok := obj.(map[string]interface{})
    if !ok {
        return nil, false
    }

    for k, v := range mobj {
        // key match, return value
        if k == key {
            return v, true
        }

        // if the value is a map, search recursively
        if m, ok := v.(map[string]interface{}); ok {
            if res, ok := FindInterface(m, key); ok {
                return res, true
            }
        }
        // if the value is an array, search recursively
        // from each element
        if va, ok := v.([]interface{}); ok {
            for _, a := range va {
                if res, ok := FindInterface(a, key); ok {
                    return res,true
                }
            }
        }
    }

    // element not found
    return nil,false
}


func QueryDB(bucketName, key []byte, db *badger.DB) (val []byte, length int) {

	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
        if err != nil {
            return nil
        }
        val, _ = item.ValueCopy(nil)
		return nil
	})
	if err != nil {
		log.Printf("There was an error: ", "error", err)
	}
	log.Printf("Found something %v", val)
	return val, len(string(val))
}
