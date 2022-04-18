package protocol

import (
    "errors"
    "strings"
//     "bytes"
    "fmt"
    "time"
    "strconv"
//     "sync"
    "log"
    "math/rand"
//     "reflect"
//     "encoding/gob"
//     "encoding/json"
//     "github.com/google/uuid"
    "github.com/shamaton/msgpack/v2"
    "github.com/gobwas/glob"
    "github.com/ErmineDB/ErmineDB/cmd/ermined/eRaft"
//     "github.com/ErmineDB/ErmineDB/cmd/ermined/database"
    "github.com/ErmineDB/ErmineDB/internal/formatter"
    "github.com/ErmineDB/ErmineDB/internal/helpers"

//     "github.com/dgraph-io/badger/v3"
    "github.com/hashicorp/raft"
//     "github.com/rs/zerolog"
//     "github.com/rs/zerolog/log"
)

type ProtoHandler struct {
    s *eRaft.Server
}

func NewProtoHandler(s *eRaft.Server) *ProtoHandler {
    return &ProtoHandler{s: s}
}

type jsonObj struct {
    created int64   `json:"created"`
    EXP int64       `json:"EXP"`
    Type string     `json:"type"`
    Data string     `json:"data"`
}

type dataStore struct {
    created int64
    EXP int64
    Type string
    Data string
}

type dataStoreJson struct {
    created int64
    EXP int64
    Type string
    Data map[string]string
}

type argObj struct {
    command string
    args []string
}

func procArgs(data []string, minArgs int) (*argObj, error) {
    obj := argObj{command: data[1]}

    i := 0
    for _,v := range data {
        if i > 1 {
            if (i%2==0) {

            }else {
                obj.args = append(obj.args, v)
            }
        }

        i += 1
    }

    if len(obj.args) < minArgs {
        message := errors.New("-ERR wrong number of arguments for '" + obj.command  + "' command")
        return &obj, message
    }

    return &obj, nil
}

func Commands() []string {
    commands := []string{"Append", "Command", "Config", "Copy", "Dbsize", "Del", "Dump", "Exists", "Get", "Hexists", "Hdel", "Hget", "Hgetall", "Hincrby", "Hincrbyfloat", "Hkeys", "Hlen", "Hmget", "Hmset", "Hrandfield", "Hset", "Hsetnx", "Keys", "Ping", "Restore", "Select", "Set", "Time"}
    return commands
}

// func Call(funcName string, params ...interface{}) string {
func Call(funcName string, data []string, hdl ProtoHandler, client helpers.Client) string {
    var results string
//     log.Printf("Made it to Call")
    switch funcName {
        case "Append":
            results = hdl.Append(data, client.Db)
        case "Command":
            results = hdl.Command(data)
        case "Config":
            results = hdl.Config(data)
        case "Copy":
            results = hdl.Copy(data, client.Db)
        case "Dbsize":
            results = hdl.Dbsize(data, client.Db)
        case "Del":
            results = hdl.Del(data, client.Db)
        case "Dump":
            results = hdl.Dump(data, client.Db)
        case "Exists":
            results = hdl.Exists(data, client.Db)
        case "Get":
            results = hdl.Get(data, client.Db)
        case "Hexists":
            results = hdl.Hexists(data, client.Db)
        case "Hdel":
            results = hdl.Hdel(data, client.Db)
        case "Hget":
            results = hdl.Hget(data, client.Db)
        case "Hgetall":
            results = hdl.Hgetall(data, client.Db)
        case "Hincrby":
            results = hdl.Hincrby(data, client.Db)
        case "Hincrbyfloat":
            results = hdl.Hincrbyfloat(data, client.Db)
        case "Hkeys":
            results = hdl.Hkeys(data, client.Db)
        case "Hlen":
            results = hdl.Hlen(data, client.Db)
        case "Hmget":
            results = hdl.Hmget(data, client.Db)
        case "Hrandfield":
            results = hdl.Hrandfield(data, client.Db)
        case "Hmset":
            results = hdl.Hset(data, client.Db)
        case "Hset":
            results = hdl.Hset(data, client.Db)
        case "Hsetnx":
            results = hdl.Hsetnx(data, client.Db)
        case "Keys":
            results = hdl.Keys(data, client.Db)
        case "Ping":
            results = Ping(data)
        case "Restore":
            results = hdl.Restore(data, client.Db)
        case "Select":
            results = Select(data, client)
        case "Set":
            results = hdl.Set(data, client.Db)
        case "Time":
            results = Time(data)
    }

    return results
}


/* =========================
      Utility Functions
========================= */

func (h *ProtoHandler) forwardToLeader(data []string) string {
    configFuture := h.s.Raft.GetConfiguration()
    log.Printf("Config: %v", configFuture)
    log.Printf("Config: %v", h.s.Raft.Leader())
    log.Printf("boot peers %v", h.s.BootPeers)

    for key := range h.s.BootPeers {
//         log.Printf("key: %v", key)
        sKey := strings.Split(key, ":")
        tIP := sKey[0] + ":" + sKey[1]
        leaderIP := fmt.Sprintf("%v", h.s.Raft.Leader())
        if tIP == leaderIP {
            leaderRedis := sKey[0] + ":" + sKey[2]
            x := fmt.Sprintf("-MOVED 0 %v \r\n", leaderRedis)
            return x
        }
    }

    return "$-1\r\n"
}



/* =========================
      Protocol Functions
========================= */

func (h *ProtoHandler) Append(data []string, bucketName string) string {
    if h.s.Raft.State() != raft.Leader {
        return h.forwardToLeader(data)
    }

    argobj, err := procArgs(data, 2)
    if err != nil {
        return formatter.BulkString(err.Error())
    }

    timeStamp := time.Now().UnixNano() / int64(time.Millisecond)
    var exp int64
    exp = 0
    dataObj := &dataStore{
        created: timeStamp,
        EXP: exp,
        Type: "$",
        Data: ""}


    resultObj, _ := h.s.Store.GetData([]byte(bucketName + argobj.args[0]))
    resLen := len(resultObj)

    if resLen != 0 {
        var m dataStore
        err := msgpack.Unmarshal(resultObj, &m)
        if err != nil {
            panic(err)
        }

        oldData := m.Data
        if m.Type != "$" {
            message := "-ERR WRONGTYPE Operation against a key holding the wrong kind of value"
            return formatter.BulkString(message)
        }

        dataObj.Data = oldData + data[5]
    } else {

        dataObj.Data = data[5]
    }

    do, err := msgpack.Marshal(&dataObj)
    if err != nil {
        panic(err)
    }
    h.s.RaftSet(bucketName + argobj.args[0], do)
    return formatter.Integer(int64(len(dataObj.Data)))

}

func Auth(data []string, bucketName string) string {
    return ""
}

func (h *ProtoHandler) Command(data []string) string{
//     log.Printf("Command data %v", data)
//     return "*1\r\n*6\r\nget\r\n:2\r\n*1\r\n$8\r\nreadonly\r\n1\r\n1\r\n1\r\n"
    return "+OK\r\n"
}

func (h *ProtoHandler) Config(data []string) string{
    return "+OK\r\n"
}

func (h *ProtoHandler) Copy(data []string, bucketName string) string {
    if h.s.Raft.State() != raft.Leader {
        return h.forwardToLeader(data)
    }

    argobj, err := procArgs(data, 2)
    if err != nil {
        return formatter.BulkString(err.Error())
    }

    useNewDb := helpers.Contains(argobj.args, "db", true)
    var destinationDb string
    if useNewDb {
        destinationDb = "bucket" + argobj.args[3]
    } else {
        destinationDb = bucketName
    }

    replace := helpers.Contains(argobj.args, "replace", true)
    if replace {
        h.s.RaftDelete(destinationDb + argobj.args[1])
    }

    replaceObj, _ := h.s.Store.GetData([]byte(destinationDb + argobj.args[1]))
    replaceLen := len(replaceObj)

    resultObj, _ := h.s.Store.GetData([]byte(bucketName + argobj.args[0]))
    resLen := len(resultObj)

    if replaceLen > 0 || resLen == 0 {
        return formatter.Integer(int64(0))
    } else {
        h.s.RaftSet(destinationDb + argobj.args[1], resultObj)
        return formatter.Integer(int64(1))
    }

}

func (h *ProtoHandler) Dbsize(data []string, bucketName string) string {
//     count := 0
    var n uint16 = 65535
    keys, _ := h.s.Store.KeysOf([]byte(""), []byte("0"), n)
    count := len(keys.Keys)

    return formatter.Integer(int64(count))
}

func (h *ProtoHandler) Del(data []string, bucketName string) string {
    if h.s.Raft.State() != raft.Leader {
        return h.forwardToLeader(data)
    }

    argobj, err := procArgs(data, 1)
    if err != nil {
        return formatter.BulkString(err.Error())
    }

    deleted := 0

    for i := 3; i < len(data); i += 2 {
        if err := h.s.RaftDelete(bucketName + argobj.args[0]); err != nil {
            deleted = deleted
        } else {
            deleted += 1
        }
    }

    return formatter.Integer(int64(deleted))
}

func (h *ProtoHandler) Dump(data []string, bucketName string) string {
    argobj, err := procArgs(data, 1)
    if err != nil {
        return formatter.BulkString(err.Error())
    }

    resultObj, _ := h.s.Store.GetData([]byte(bucketName + argobj.args[0]))
    resLen := len(resultObj)

    if resLen == 0 {
        return "$-1\r\n"
    } else {
//         return formatter.BulkString(fmt.Sprintf("%v", resultObj))
        return formatter.BulkString(string(resultObj))
    }
}

func (h *ProtoHandler) Exists(data []string, bucketName string) string {
//     timeStamp := time.Now().UnixNano() / int64(time.Millisecond)
    argobj, err := procArgs(data, 1)
    if err != nil {
        return formatter.BulkString(err.Error())
    }

    found := 0

    for _,v := range argobj.args {
        resultObj, _ := h.s.Store.GetData([]byte(bucketName + v))
        resLen := len(resultObj)
        if resLen > 0 {
            found += 1
        }
    }

    return formatter.Integer(int64(found))
}

func (h *ProtoHandler) Get(data []string, bucketName string) string {
//     Get the value of a key.  If the key does not exist return nil.
//     If the value stored at key is not a string, an error is returned.

    argobj, err := procArgs(data, 1)
    if err != nil {
        return formatter.BulkString(err.Error())
    }

    if len(argobj.args) != 1 {
        return formatter.BulkString("-ERR wrong number of argument for 'GET' command")
    }

    resultObj, _ := h.s.Store.GetData([]byte(bucketName + argobj.args[0]))
    resLen := len(resultObj)
//     log.Printf("resultObj: %v", string(resultObj))

    if resLen == 0 {
//         log.Printf("returning because reLen == 0")
        return "$-1\r\n"
    } else {

        var m dataStore
        err := msgpack.Unmarshal(resultObj, &m)
        if err != nil {
            panic(err)
        }


        exp := m.EXP
        if exp != 0 {
            timeStamp := time.Now().UnixNano() / int64(time.Millisecond)
            if exp <= timeStamp {
                h.s.RaftDelete(bucketName + argobj.args[0])
                return "$-1\r\n"
            }
        }

        result := m.Data
        if m.Type != "$" {
            message := "-ERR WRONGTYPE Operation against a key holding the wrong kind of value"
            return formatter.BulkString(message)
        }

        return formatter.BulkString(fmt.Sprintf("%v", result))
    }
}

func getHashObjOrNew(h *ProtoHandler, data []string, bucketName string) (dataStoreJson, error) {
    argobj, _ := procArgs(data, 0)
    resultObj, _ := h.s.Store.GetData([]byte(bucketName + argobj.args[0]))
    resLen := len(resultObj)

    var dataObj dataStoreJson

    if resLen == 0 {
        timeStamp := time.Now().UnixNano() / int64(time.Millisecond)

        //     setup a data container
        var exp int64
        exp = 0

        dataObj.created = timeStamp
        dataObj.EXP = exp
        dataObj.Type = "hash"
        dataObj.Data = map[string]string{}
    } else {
        err := msgpack.Unmarshal(resultObj, &dataObj)
        if err != nil {
            return dataObj, err
        }

        if dataObj.Type != "hash" {
            message := errors.New("-ERR WRONGTYPE Operation against a key holding the wrong kind of value")
            return dataObj, message
        }

        return dataObj, nil
    }

    return dataObj, nil
}

func (h *ProtoHandler) Hexists(data []string, bucketName string) string {
    argobj, err := procArgs(data, 2)
    if err != nil {
        return formatter.BulkString(err.Error())
    }

    resultObj, _ := h.s.Store.GetData([]byte(bucketName + argobj.args[0]))
    resLen := len(resultObj)

    if resLen == 0 {
//         log.Printf("returning because reLen == 0")
        return formatter.Integer(int64(0))
    } else {
        var m dataStoreJson
        err := msgpack.Unmarshal(resultObj, &m)
        if err != nil {
            panic(err)
        }

        if m.Type != "hash" {
            message := "-ERR WRONGTYPE Operation against a key holding the wrong kind of value"
            return formatter.BulkString(message)
        }

        mobj := m.Data
        _, found := mobj[argobj.args[1]]

        if found {
            return formatter.Integer(int64(1))
        }
        return formatter.Integer(int64(0))
    }
}

func (h *ProtoHandler) Hdel(data []string, bucketName string) string {
    if h.s.Raft.State() != raft.Leader {
        return h.forwardToLeader(data)
    }

    argobj, err := procArgs(data, 2)
    if err != nil {
        return formatter.BulkString(err.Error())
    }

    resultObj, _ := h.s.Store.GetData([]byte(bucketName + argobj.args[0]))
    resLen := len(resultObj)

    if resLen == 0 {
        log.Printf("returning because reLen == 0")
        return "$-1\r\n"
    } else {
        var m dataStoreJson
        err := msgpack.Unmarshal(resultObj, &m)
        if err != nil {
            panic(err)
        }

        if m.Type != "hash" {
            message := "-ERR WRONGTYPE Operation against a key holding the wrong kind of value"
            return formatter.BulkString(message)
        }

        mobj := m.Data

        c := 0
        for i := 0; i < len(mobj); i++ {
            for j := 1; j < len(argobj.args); j++ {
                _, found := mobj[argobj.args[j]]
                if found {
                    log.Printf("found")
                    delete(mobj, argobj.args[j])
                    c++
                    break
                }
            }
        }

        m.Data = mobj

        do, err := msgpack.Marshal(&m)
        if err != nil {
            panic(err)
        }
        h.s.RaftSet(bucketName + argobj.args[0], do)

        return formatter.Integer(int64(c))
    }
}

func (h *ProtoHandler) Hget(data []string, bucketName string) string {
    argobj, err := procArgs(data, 2)
    if err != nil {
        return formatter.BulkString(err.Error())
    }

    resultObj, _ := h.s.Store.GetData([]byte(bucketName + argobj.args[0]))
    resLen := len(resultObj)

    if resLen == 0 {
//         log.Printf("returning because reLen == 0")
        return "$-1\r\n"
    } else {
        var m dataStoreJson
        err := msgpack.Unmarshal(resultObj, &m)
        if err != nil {
            panic(err)
        }

        if m.Type != "hash" {
            message := "-ERR WRONGTYPE Operation against a key holding the wrong kind of value"
            return formatter.BulkString(message)
        }

        mobj := m.Data
        val, found := mobj[argobj.args[1]]

        if found {
            return formatter.BulkString(fmt.Sprintf("%v", val))
        }
        return "$-1\r\n"
    }
}

func (h *ProtoHandler) Hgetall(data []string, bucketName string) string {
    argobj, err := procArgs(data, 1)
    if err != nil {
        return formatter.BulkString(err.Error())
    }

    resultObj, _ := h.s.Store.GetData([]byte(bucketName + argobj.args[0]))
    resLen := len(resultObj)

    var returnString []string

    if resLen == 0 {

        return formatter.List(returnString)
    } else {
        var m dataStoreJson
        err := msgpack.Unmarshal(resultObj, &m)
        if err != nil {
            panic(err)
        }

        if m.Type != "hash" {
            message := "-ERR WRONGTYPE Operation against a key holding the wrong kind of value"
            return formatter.BulkString(message)
        }

        mobj := m.Data

        for key, element := range mobj {
            returnString = append(returnString, key)
            returnString = append(returnString, element)
        }
        return formatter.List(returnString)
    }
}

func (h *ProtoHandler) Hincrby(data []string, bucketName string) string {
    if h.s.Raft.State() != raft.Leader {
        return h.forwardToLeader(data)
    }

    argobj, err := procArgs(data, 3)
    if err != nil {
        return formatter.BulkString(err.Error())
    }

    if len(argobj.args) != 3 {
        return formatter.BulkString("-ERR wrong number of arguments for 'HINCRBY' command")
    }

    incrBy, err := strconv.ParseInt(argobj.args[2], 10, 64)
    if err != nil {
        return formatter.BulkString("-ERR value is not an integer or out of range")
    }

    dataObj, err := getHashObjOrNew(h, data, bucketName)
    if err != nil {
        return formatter.BulkString(err.Error())
    }

    var v int64
    val, found := dataObj.Data[argobj.args[1]]
    if found {
        v, err = strconv.ParseInt(val, 10, 64)
        if err != nil {
            return formatter.BulkString("-ERR hash value is not an integer")
        }
    } else {
        v = 0
    }

    v = v + incrBy
    dataObj.Data[argobj.args[1]] = strconv.FormatInt(int64(v), 10)

    do, err := msgpack.Marshal(&dataObj)
    if err != nil {
        panic(err)
    }
    h.s.RaftSet(bucketName + argobj.args[0], do)

    return formatter.Integer(v)
}

func (h *ProtoHandler) Hincrbyfloat(data []string, bucketName string) string {
    if h.s.Raft.State() != raft.Leader {
        return h.forwardToLeader(data)
    }

    argobj, err := procArgs(data, 3)
    if err != nil {
        return formatter.BulkString(err.Error())
    }

    if len(argobj.args) != 3 {
        return formatter.BulkString("-ERR wrong number of arguments for 'HINCRBYFLOAT' command")
    }

    incrBy, err := strconv.ParseFloat(argobj.args[2], 64)
    if err != nil {
        return formatter.BulkString("-ERR value is not a valid float")
    }

    dataObj, err := getHashObjOrNew(h, data, bucketName)
    if err != nil {
        return formatter.BulkString(err.Error())
    }

    var v float64
    val, found := dataObj.Data[argobj.args[1]]
    if found {
        v, err = strconv.ParseFloat(val, 64)
        if err != nil {
            return formatter.BulkString("-ERR hash value is not a float")
        }
    } else {
        v = 0
    }

    v = v + incrBy
    dataObj.Data[argobj.args[1]] = strconv.FormatFloat(v, 'f', 17, 64)

    do, err := msgpack.Marshal(&dataObj)
    if err != nil {
        panic(err)
    }
    h.s.RaftSet(bucketName + argobj.args[0], do)

    return formatter.BulkString(dataObj.Data[argobj.args[1]])
}

func (h *ProtoHandler) Hkeys(data []string, bucketName string) string {
    argobj, err := procArgs(data, 1)
    if err != nil {
        return formatter.BulkString(err.Error())
    }

    resultObj, _ := h.s.Store.GetData([]byte(bucketName + argobj.args[0]))
    resLen := len(resultObj)

    var returnString []string

    if resLen == 0 {

        return formatter.List(returnString)
    } else {
        var m dataStoreJson
        err := msgpack.Unmarshal(resultObj, &m)
        if err != nil {
            panic(err)
        }

        if m.Type != "hash" {
            message := "-ERR WRONGTYPE Operation against a key holding the wrong kind of value"
            return formatter.BulkString(message)
        }

        mobj := m.Data
        for key, _ := range mobj {
            returnString = append(returnString, key)
        }
        return formatter.List(returnString)
    }
}

func (h *ProtoHandler) Hlen(data []string, bucketName string) string {
    argobj, err := procArgs(data, 1)
    if err != nil {
        return formatter.BulkString(err.Error())
    }

    resultObj, _ := h.s.Store.GetData([]byte(bucketName + argobj.args[0]))
    resLen := len(resultObj)

    var returnString []string

    if resLen == 0 {
        return formatter.List(returnString)
    } else {
        var m dataStoreJson
        err := msgpack.Unmarshal(resultObj, &m)
        if err != nil {
            panic(err)
        }

        if m.Type != "hash" {
            message := "-ERR WRONGTYPE Operation against a key holding the wrong kind of value"
            return formatter.BulkString(message)
        }

        mobj := m.Data
        var c int64
        for range mobj {
            c += 1
        }
        return formatter.Integer(c)
    }
}

func (h *ProtoHandler) Hmget(data []string, bucketName string) string {
    argobj, err := procArgs(data, 2)
    if err != nil {
        return formatter.BulkString(err.Error())
    }

    resultObj, _ := h.s.Store.GetData([]byte(bucketName + argobj.args[0]))
    resLen := len(resultObj)

    var returnString []string

    if resLen == 0 {

        return formatter.List(returnString)
    } else {
        var m dataStoreJson
        err := msgpack.Unmarshal(resultObj, &m)
        if err != nil {
            panic(err)
        }

        if m.Type != "hash" {
            message := "-ERR WRONGTYPE Operation against a key holding the wrong kind of value"
            return formatter.BulkString(message)
        }

        mobj := m.Data
        var found int
//         log.Printf("mobj type %t", mobj)
        for _,v := range argobj.args[1:] {
            found = 0
            for storedKey, storedValue := range mobj {
                if storedKey == v {
                    found = 1
                    returnString = append(returnString, storedValue)
                    break
                }
            }
            if found != 1 {
                returnString = append(returnString, "")
            }
        }

        return formatter.List(returnString)
    }
}

func returnRand(data map[string]string) (string, string) {
    rNum := rand.Intn(len(data))
    c := 0
    for k, v := range data {
        if c == rNum {
            return k, v
        }
        c += 1
    }
    return "", ""
}

func (h *ProtoHandler) Hrandfield(data []string, bucketName string) string {
    argobj, err := procArgs(data, 1)
    if err != nil {
        return formatter.BulkString(err.Error())
    }

    argLen := len(argobj.args)

    resultObj, _ := h.s.Store.GetData([]byte(bucketName + argobj.args[0]))
    resLen := len(resultObj)

    var returnString []string
    if resLen == 0 {
        return formatter.List(returnString)
    } else {
        var m dataStoreJson
        err := msgpack.Unmarshal(resultObj, &m)
        if err != nil {
            panic(err)
        }

        if m.Type != "hash" {
            message := "-ERR WRONGTYPE Operation against a key holding the wrong kind of value"
            return formatter.BulkString(message)
        }

        mobj := m.Data

        if argLen == 1 {
            k, _ := returnRand(mobj)
            return formatter.BulkString(k)
        } else if argLen >= 2 {
            withvalues := false
            count, err := strconv.Atoi(argobj.args[1])
            if err != nil {
                return formatter.BulkString("-ERR value is not an integer or out of range")
            }

            if argLen == 3 {
                if strings.ToLower(argobj.args[2]) != "withvalues" {
                    message := "-ERR syntax error"
                    return formatter.BulkString(message)
                }
                withvalues = true
            } else if argLen > 3 {
                message := "-ERR syntax error"
                return formatter.BulkString(message)
            }

            if count > 0 {
                c := 0

                for k, v := range mobj {
                    if c < len(mobj) && c < count {
                        returnString = append(returnString, k)
                        if withvalues {
                            returnString = append(returnString, v)
                        }
                    }
                    c +=1
                }
            } else {
                c := 0
                count = count * -1
                for i := 0; i < count; i++ {
                    k, v := returnRand(mobj)
                    if c < len(mobj) && c < count {
                        returnString = append(returnString, k)
                        if withvalues {
                            returnString = append(returnString, v)
                        }
                    }
                    c +=1
                }
            }
            return formatter.List(returnString)
        }
        return formatter.List(returnString)
    }
}

func (h *ProtoHandler) Hset(data []string, bucketName string) string {
    if h.s.Raft.State() != raft.Leader {
        return h.forwardToLeader(data)
    }

    argobj, err := procArgs(data, 2)
    if err != nil {
        return formatter.BulkString(err.Error())
    }

    dataObj, err := getHashObjOrNew(h, data, bucketName)
    if err != nil {
        return formatter.BulkString(err.Error())
    }

//     d := dataObj.Data
    c := 0
    for i:=1; i<len(argobj.args); i = i+2 {
        if i+1 < len(argobj.args) {
//             d[argobj.args[i]] = argobj.args[i+1]
            dataObj.Data[argobj.args[i]] = argobj.args[i+1]
            c += 1
        }
    }

//     dataObj.Data = d

    do, err := msgpack.Marshal(&dataObj)
    if err != nil {
        panic(err)
    }
    h.s.RaftSet(bucketName + argobj.args[0], do)

    return formatter.Integer(int64(c))
}

func (h *ProtoHandler) Hsetnx(data []string, bucketName string) string {
    if h.s.Raft.State() != raft.Leader {
        return h.forwardToLeader(data)
    }

    argobj, err := procArgs(data, 3)
    if err != nil {
        return formatter.BulkString(err.Error())
    }

    dataObj, err := getHashObjOrNew(h, data, bucketName)
    if err != nil {
        return formatter.BulkString(err.Error())
    }

    found := dataObj.Data[argobj.args[1]]
    if found != "" {
        return formatter.Integer(int64(0))
    } else {
        dataObj.Data[argobj.args[1]] = argobj.args[2]
    }

    do, err := msgpack.Marshal(&dataObj)
    if err != nil {
        panic(err)
    }
    h.s.RaftSet(bucketName + argobj.args[0], do)

    return formatter.Integer(int64(1))
}

func (h *ProtoHandler) Keys(data []string, bucketName string) string {
    argobj, err := procArgs(data, 1)
    if err != nil {
        return formatter.BulkString(err.Error())
    }

    var n uint16 = 65535
    keys, _ := h.s.Store.KeysOf([]byte(""), []byte("0"), n)

    var matched []string
    var g glob.Glob
//     g = glob.MustCompile(argobj.args[0])
    g = glob.MustCompile(bucketName + argobj.args[0])

    for _, k := range keys.Keys {
        if g.Match(k) {
            key := strings.TrimPrefix(k, bucketName)
            matched = append(matched, key)
        }
    }
    return formatter.List(matched)
}

func Ping(data []string) string {
    if len(data) == 2 {
        return formatter.SimpleString("PONG")
    } else {
        return formatter.SimpleString(data[3])
    }
}

func (h *ProtoHandler) Restore(data []string, bucketName string) string {
    //need to add support for IDLETIME and FREQ
    if h.s.Raft.State() != raft.Leader {
        return h.forwardToLeader(data)
    }

    argobj, err := procArgs(data, 3)
    if err != nil {
        return formatter.BulkString(err.Error())
    }

//     absTtl := helpers.Contains(argobj.args, "absttl", true)

    replace := helpers.Contains(argobj.args, "replace", true)
    if replace {
        h.s.RaftDelete(bucketName + argobj.args[1])
    }

    replaceObj, _ := h.s.Store.GetData([]byte(bucketName + argobj.args[0]))
    replaceLen := len(replaceObj)

    if replaceLen > 0 && replace == false {
        return formatter.BulkString("-BUSYKEY Target key name already exists.")
    } else {
//         var converter = map[rune]rune{}
//         for _, runeValue := range argobj.args[2] {
//             converter[runeValue] = runeValue
//         }

        ttl, err := strconv.Atoi(argobj.args[1])
        if err != nil {
            return formatter.BulkString("-ERR value is not an integer or out of range")
        }
        absttl := helpers.Contains(argobj.args, "absttl", true)
        var exp int64
        if absttl {
            exp = int64(ttl)
        } else {
            if ttl != 0 {
                exp = time.Now().UnixNano() / int64(ttl)
            } else {
                exp = 0
            }
        }

        var m dataStore
        err = msgpack.Unmarshal([]byte(argobj.args[2]), &m)
        if err != nil {
            panic(err)
        }

        m.EXP = exp
        //test to make sure this is good data
        log.Printf("this is m: %v", m)
        do, err := msgpack.Marshal(&m)
        log.Printf("This is do: %v", do)
        if err != nil {
            panic(err)
        }
        h.s.RaftSet(bucketName + argobj.args[0], do)
        return formatter.Integer(int64(1))
    }
}

func (h *ProtoHandler) Set(data []string, bucketName string) string {
    if h.s.Raft.State() != raft.Leader {
        return h.forwardToLeader(data)
    }

    argobj, err := procArgs(data, 2)
    if err != nil {
        return formatter.BulkString(err.Error())
    }

    timeStamp := time.Now().UnixNano() / int64(time.Millisecond)

//     setup a data container
    var exp int64
    exp = 0
    dataObj := &dataStore{
        created: timeStamp,
        EXP: exp,
        Type: "$",
        Data: ""}

//     check if the key exists. save to tmp var
//     oldData, found := db[data[3]]

    if len(data) > 6 {
        var exp int64
        var upperData []string
        for _,v := range data {
            upperData = append(upperData, v)
        }

        if helpers.Contains(upperData, "EX", false) {
            if helpers.Contains(upperData, "PX", false) || helpers.Contains(upperData, "EXAT", false) || helpers.Contains(upperData, "PXAT", false) || helpers.Contains(upperData, "KEEPTTL", false) {
                return "-ERR syntax error\r\n"
            } else {
                idx := helpers.IndexOf(upperData, "EX")
                exp,_ = strconv.ParseInt(data[idx + 2], 10, 64)
                exp = (exp * 1000) + timeStamp
            }
        }
        if helpers.Contains(upperData, "PX", false) {
            if helpers.Contains(upperData, "EX", false) || helpers.Contains(upperData, "EXAT", false) || helpers.Contains(upperData, "PXAT", false) || helpers.Contains(upperData, "KEEPTTL", false) {
                return "-ERR syntax error\r\n"
            } else {
                idx := helpers.IndexOf(upperData, "PX")
                exp,_ = strconv.ParseInt(data[idx + 2], 10, 64)
                // convert exp from seconds to to milliseconds
                exp = exp + timeStamp
            }
        }
        if helpers.Contains(upperData, "EXAT", false) {
            if helpers.Contains(upperData, "EX", false) || helpers.Contains(upperData, "PX", false) || helpers.Contains(upperData, "PXAT", false) || helpers.Contains(upperData, "KEEPTTL", false) {
                return "-ERR syntax error\r\n"
            } else {
                idx := helpers.IndexOf(upperData, "EXAT")
                exp,_ = strconv.ParseInt(data[idx + 2], 10, 64)
                // convert exp from seconds to to milliseconds
                exp = exp * 1000
            }
        }
        if helpers.Contains(upperData, "PXAT", false) {
            if helpers.Contains(upperData, "EX", false) || helpers.Contains(upperData, "PX", false) || helpers.Contains(upperData, "EXAT", false) || helpers.Contains(upperData, "KEEPTTL", false) {
                return "-ERR syntax error\r\n"
            } else {
                idx := helpers.IndexOf(upperData, "PXAT")
                exp,_ = strconv.ParseInt(data[idx + 2], 10, 64)
                // convert exp from seconds to to milliseconds
                exp = exp
            }
        }
//         if helpers.Contains(upperData, "KEEPTTL", false) {
//
//         }

        dataObj.EXP = exp
    } else if len(data) == 6 {
        _ = dataObj
    }else {
        return "-ERR syntax error\r\n"
    }

    dataObj.Data = data[5]

// msgpack
    do, err := msgpack.Marshal(&dataObj)
    log.Printf("This is do: %v", do)
    if err != nil {
        panic(err)
    }
    h.s.RaftSet(bucketName + argobj.args[0], do)


    return "+OK\r\n"
}

func Select(data []string, client helpers.Client) string {
    argobj, err := procArgs(data, 1)
    if err != nil {
        return formatter.BulkString(err.Error())
    }

    dbInt, err := strconv.Atoi(argobj.args[0])
    if err != nil {
        return formatter.BulkString("-ERR value is not an integer or out of range")
    }

    if dbInt > 15 {
        return formatter.BulkString("ERR DB index is out of range")
    }

    client.Db = "bucket" + argobj.args[0]
    helpers.ClientManager[client.SessionId] = client
    return "+OK\r\n"
}

func Time(data []string) string {
    tm := time.Now().UnixMicro()
    tmS := strconv.FormatInt(tm, 10)

    var returnString []string
    returnString = append(returnString, tmS[:10])
    returnString = append(returnString, tmS[10:])

    return formatter.List(returnString)
}
