package helpers

import (
    "net"
    "sync"
    "github.com/google/uuid"
)

type Client struct {
    Socket net.Conn
    Uuid uuid.UUID
    Subs []string
}

// var Mutex = &sync.Mutex{}
// var Manager = make(map[uuid.UUID]Client)
var Manager = sync.Map{}

type Channel struct {
    Name string
    Clients []Client
}
// var Channels []*Channel

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
