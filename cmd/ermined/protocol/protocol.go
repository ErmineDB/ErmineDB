package protocol

import (
    "errors"
    "strings"
//     "bytes"
    "fmt"
    "time"
    "strconv"
    "log"
//     "reflect"
//     "encoding/gob"
//     "encoding/json"
    "github.com/shamaton/msgpack/v2"
    "github.com/gobwas/glob"
    "github.com/ErmineDB/ErmineDB/cmd/ermined/eRaft"
//     "github.com/ErmineDB/ErmineDB/cmd/ermined/database"
    "github.com/ErmineDB/ErmineDB/internal/formatter"
    "github.com/ErmineDB/ErmineDB/internal/helpers"

//     "github.com/dgraph-io/badger/v3"
    "github.com/hashicorp/raft"
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

func procArgs(data []string) *argObj {
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
    return &obj
}

func Commands() []string {
    commands := []string{"Append", "Command", "Config", "Dbsize", "Del", "Exists", "Get", "Hexists", "Hdel", "Hget", "Hgetall", "Hincrby", "Hincrbyfloat", "Hkeys", "Hlen", "Hmget", "Hset", "Keys", "Ping", "Select", "Set"}
    return commands
}

func Call(funcName string, params ...interface{}) string {
    var results string
//     log.Printf("Made it to Call")
    switch funcName {
        case "Append":
            hdl := params[1].(ProtoHandler)
            results = hdl.Append(params[0].([]string), []byte("bucket0"))
        case "Command":
            hdl := params[1].(ProtoHandler)
            results = hdl.Command(params[0].([]string))
        case "Config":
            hdl := params[1].(ProtoHandler)
            results = hdl.Config(params[0].([]string))
        case "Dbsize":
            hdl := params[1].(ProtoHandler)
            results = hdl.Dbsize(params[0].([]string), []byte("bucket0"))
        case "Del":
            hdl := params[1].(ProtoHandler)
            results = hdl.Del(params[0].([]string), []byte("bucket0"))
        case "Exists":
            hdl := params[1].(ProtoHandler)
            results = hdl.Exists(params[0].([]string), []byte("bucket0"))
        case "Get":
            hdl := params[1].(ProtoHandler)
            results = hdl.Get(params[0].([]string), []byte("bucket0"))
//             results = Get(params[0].([]string), []byte("bucket0"))
        case "Hexists":
            hdl := params[1].(ProtoHandler)
            results = hdl.Hexists(params[0].([]string), []byte("bucket0"))
        case "Hdel":
            hdl := params[1].(ProtoHandler)
            results = hdl.Hdel(params[0].([]string), []byte("bucket0"))
        case "Hget":
            hdl := params[1].(ProtoHandler)
            results = hdl.Hget(params[0].([]string), []byte("bucket0"))
        case "Hgetall":
            hdl := params[1].(ProtoHandler)
            results = hdl.Hgetall(params[0].([]string), []byte("bucket0"))
        case "Hincrby":
            hdl := params[1].(ProtoHandler)
            results = hdl.Hincrby(params[0].([]string), []byte("bucket0"))
        case "Hincrbyfloat":
            hdl := params[1].(ProtoHandler)
            results = hdl.Hincrbyfloat(params[0].([]string), []byte("bucket0"))
        case "Hkeys":
            hdl := params[1].(ProtoHandler)
            results = hdl.Hkeys(params[0].([]string), []byte("bucket0"))
        case "Hlen":
            hdl := params[1].(ProtoHandler)
            results = hdl.Hlen(params[0].([]string), []byte("bucket0"))
        case "Hmget":
            hdl := params[1].(ProtoHandler)
            results = hdl.Hmget(params[0].([]string), []byte("bucket0"))
        case "Hset":
            hdl := params[1].(ProtoHandler)
            results = hdl.Hset(params[0].([]string), []byte("bucket0"))
        case "Keys":
            hdl := params[1].(ProtoHandler)
            results = hdl.Keys(params[0].([]string), []byte("bucket0"))
        case "Ping":
            results = Ping(params[0].([]string))
        case "Select":
            results = Select(params[0].([]string), []byte("bucket0"))
        case "Set":
            hdl := params[1].(ProtoHandler)
            results = hdl.Set(params[0].([]string), []byte("bucket0"))
//             results = Set(params[0].([]string), []byte("bucket0"))
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

func (h *ProtoHandler) Command(data []string) string{
//     log.Printf("Command data %v", data)
//     return "*1\r\n*6\r\nget\r\n:2\r\n*1\r\n$8\r\nreadonly\r\n1\r\n1\r\n1\r\n"
    return "+OK\r\n"
}

func (h *ProtoHandler) Config(data []string) string{
    return "+OK\r\n"
}

func (h *ProtoHandler) Append(data []string, bucketName []byte) string {
    if h.s.Raft.State() != raft.Leader {
        return h.forwardToLeader(data)
    }

    timeStamp := time.Now().UnixNano() / int64(time.Millisecond)
    var exp int64
    exp = 0
    dataObj := &dataStore{
        created: timeStamp,
        EXP: exp,
        Type: "$",
        Data: ""}

    argobj := procArgs(data)
    resultObj, _ := h.s.Store.GetData([]byte(argobj.args[0]))
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
    h.s.RaftSet(data[3], do)
    return formatter.Integer(int64(len(dataObj.Data)))

}

func Auth(data []string, bucketName []byte) string {
    return ""
}

func (h *ProtoHandler) Dbsize(data []string, bucketName []byte) string {
//     count := 0
    var n uint16 = 65535
    keys, _ := h.s.Store.KeysOf([]byte(""), []byte("0"), n)
    count := len(keys.Keys)

    return formatter.Integer(int64(count))
}


func (h *ProtoHandler) Del(data []string, bucketName []byte) string {
//     log.Printf("data in Del: %v", data)
    //[$3 del $3 bob $3 jim $3 tom]

    if h.s.Raft.State() != raft.Leader {
        return h.forwardToLeader(data)
    }

    deleted := 0

    for i := 3; i < len(data); i += 2 {
//         log.Printf("would have del: %v", data[i])
//         _, resLen := database.QueryDB(bucketName, []byte(data[3]))
//         if resLen > 0 {
//             database.DeleteKey(bucketName, []byte(data[i]))
//             deleted += 1
//         }
        if err := h.s.RaftDelete(data[3]); err != nil {
            deleted = deleted
        } else {
            deleted += 1
        }
    }

    return formatter.Integer(int64(deleted))
}

func (h *ProtoHandler) Exists(data []string, bucketName []byte) string {
//     timeStamp := time.Now().UnixNano() / int64(time.Millisecond)
    argobj := procArgs(data)

    found := 0

    for _,v := range argobj.args {
        resultObj, _ := h.s.Store.GetData([]byte(v))
        resLen := len(resultObj)
        if resLen > 0 {
            found += 1
        }
    }

    return formatter.Integer(int64(found))
}

func (h *ProtoHandler) Get(data []string, bucketName []byte) string {
//     Get the value of a key.  If the key does not exist return nil.
//     If the value stored at key is not a string, an error is returned.

    argobj := procArgs(data)
//     log.Printf("argobj %v", argobj.args)

    if len(argobj.args) != 1 {
        return formatter.BulkString("-ERR wrong number of argument for 'GET' command")
    }

    resultObj, _ := h.s.Store.GetData([]byte(argobj.args[0]))
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


//         exp := m["EXP"].(int64)
//         if exp != 0 {
//             if exp <= timeStamp {
//                 database.DeleteKey(bucketName, []byte(argobj.args[0]))
//                 return "$-1\r\n"
//             }
//         }

        result := m.Data
        if m.Type != "$" {
            message := "-ERR WRONGTYPE Operation against a key holding the wrong kind of value"
            return formatter.BulkString(message)
        }

        return formatter.BulkString(fmt.Sprintf("%v", result))
    }
}


func getHashObjOrNew(h *ProtoHandler, data []string, bucketName []byte) (error, dataStoreJson) {

    argobj := procArgs(data)
    resultObj, _ := h.s.Store.GetData([]byte(argobj.args[0]))
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
            return err, dataObj
        }

        if dataObj.Type != "hash" {
            message := errors.New("-ERR WRONGTYPE Operation against a key holding the wrong kind of value")
            return message, dataObj
        }

        return nil, dataObj
    }

    return nil, dataObj
}

func (h *ProtoHandler) Hexists(data []string, bucketName []byte) string {
    argobj := procArgs(data)

    resultObj, _ := h.s.Store.GetData([]byte(argobj.args[0]))
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

func (h *ProtoHandler) Hdel(data []string, bucketName []byte) string {
    if h.s.Raft.State() != raft.Leader {
        return h.forwardToLeader(data)
    }

    argobj := procArgs(data)
//     gob.Register(map[string]string{})

    resultObj, _ := h.s.Store.GetData([]byte(argobj.args[0]))
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

        c := 0
        for i := 0; i < len(mobj); i++ {
            for j := 1; j < len(argobj.args); j++ {
                _, found := mobj[argobj.args[j]]
                if found {
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
        h.s.RaftSet(data[3], do)

        return formatter.Integer(int64(c))
    }
}

func (h *ProtoHandler) Hget(data []string, bucketName []byte) string {
    argobj := procArgs(data)
//     gob.Register(map[string]string{})

    resultObj, _ := h.s.Store.GetData([]byte(argobj.args[0]))
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

func (h *ProtoHandler) Hgetall(data []string, bucketName []byte) string {
    argobj := procArgs(data)

    resultObj, _ := h.s.Store.GetData([]byte(argobj.args[0]))
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

func (h *ProtoHandler) Hincrby(data []string, bucketName []byte) string {
    if h.s.Raft.State() != raft.Leader {
        return h.forwardToLeader(data)
    }

    argobj := procArgs(data)

    if len(argobj.args) != 3 {
        return formatter.BulkString("-ERR wrong number of arguments for 'HINCRBY' command")
    }

    incrBy, err := strconv.ParseInt(argobj.args[2], 10, 64)
    if err != nil {
        return formatter.BulkString("-ERR value is not an integer or out of range")
    }

    err, dataObj := getHashObjOrNew(h, data, bucketName)
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
    h.s.RaftSet(data[3], do)

    return formatter.Integer(v)
}

func (h *ProtoHandler) Hincrbyfloat(data []string, bucketName []byte) string {
    if h.s.Raft.State() != raft.Leader {
        return h.forwardToLeader(data)
    }

    argobj := procArgs(data)

    if len(argobj.args) != 3 {
        return formatter.BulkString("-ERR wrong number of arguments for 'HINCRBYFLOAT' command")
    }

    incrBy, err := strconv.ParseFloat(argobj.args[2], 64)
    if err != nil {
        return formatter.BulkString("-ERR value is not a valid float")
    }

    err, dataObj := getHashObjOrNew(h, data, bucketName)
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
    h.s.RaftSet(data[3], do)

    return formatter.BulkString(dataObj.Data[argobj.args[1]])
}

func (h *ProtoHandler) Hkeys(data []string, bucketName []byte) string {
    argobj := procArgs(data)

    resultObj, _ := h.s.Store.GetData([]byte(argobj.args[0]))
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

func (h *ProtoHandler) Hlen(data []string, bucketName []byte) string {
    argobj := procArgs(data)

    resultObj, _ := h.s.Store.GetData([]byte(argobj.args[0]))
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


func (h *ProtoHandler) Hmget(data []string, bucketName []byte) string {
    argobj := procArgs(data)

    resultObj, _ := h.s.Store.GetData([]byte(argobj.args[0]))
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
        log.Printf("mobj type %t", mobj)
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

func (h *ProtoHandler) Hset(data []string, bucketName []byte) string {
    if h.s.Raft.State() != raft.Leader {
        return h.forwardToLeader(data)
    }

    argobj := procArgs(data)

    err, dataObj := getHashObjOrNew(h, data, bucketName)
    if err != nil {
        return formatter.BulkString(err.Error())
    }

    d := dataObj.Data
    c := 0
    for i:=1; i<len(argobj.args); i = i+2 {
        if i+1 < len(argobj.args) {
            d[argobj.args[i]] = argobj.args[i+1]
            c += 1
        }
    }

    dataObj.Data = d

    do, err := msgpack.Marshal(&dataObj)
    if err != nil {
        panic(err)
    }
    h.s.RaftSet(data[3], do)

    return formatter.Integer(int64(c))
}

func (h *ProtoHandler) Keys(data []string, bucketName []byte) string {

    var n uint16 = 65535
    keys, _ := h.s.Store.KeysOf([]byte(""), []byte("0"), n)

    var matched []string
    var g glob.Glob
    g = glob.MustCompile(data[3])

    for _, k := range keys.Keys {
        if g.Match(k) {
            matched = append(matched, k)
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

// func Set(data []string, db map[string]interface{}) string{
func (h *ProtoHandler) Set(data []string, bucketName []byte) string {

    if h.s.Raft.State() != raft.Leader {
        return h.forwardToLeader(data)
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

        if helpers.Contains(upperData, "EX") {
            if helpers.Contains(upperData, "PX") || helpers.Contains(upperData, "EXAT") || helpers.Contains(upperData, "PXAT") || helpers.Contains(upperData, "KEEPTTL") {
                return "-ERR syntax error\r\n"
            } else {
                idx := helpers.IndexOf(upperData, "EX")
                exp,_ = strconv.ParseInt(data[idx + 2], 10, 64)
                exp = (exp * 1000) + timeStamp
            }
        }
        if helpers.Contains(upperData, "PX") {
            if helpers.Contains(upperData, "EX") || helpers.Contains(upperData, "EXAT") || helpers.Contains(upperData, "PXAT") || helpers.Contains(upperData, "KEEPTTL") {
                return "-ERR syntax error\r\n"
            } else {
                idx := helpers.IndexOf(upperData, "PX")
                exp,_ = strconv.ParseInt(data[idx + 2], 10, 64)
                // convert exp from seconds to to milliseconds
                exp = exp + timeStamp
            }
        }
        if helpers.Contains(upperData, "EXAT") {
            if helpers.Contains(upperData, "EX") || helpers.Contains(upperData, "PX") || helpers.Contains(upperData, "PXAT") || helpers.Contains(upperData, "KEEPTTL") {
                return "-ERR syntax error\r\n"
            } else {
                idx := helpers.IndexOf(upperData, "EXAT")
                exp,_ = strconv.ParseInt(data[idx + 2], 10, 64)
                // convert exp from seconds to to milliseconds
                exp = exp * 1000
            }
        }
        if helpers.Contains(upperData, "PXAT") {
            if helpers.Contains(upperData, "EX") || helpers.Contains(upperData, "PX") || helpers.Contains(upperData, "EXAT") || helpers.Contains(upperData, "KEEPTTL") {
                return "-ERR syntax error\r\n"
            } else {
                idx := helpers.IndexOf(upperData, "PXAT")
                exp,_ = strconv.ParseInt(data[idx + 2], 10, 64)
                // convert exp from seconds to to milliseconds
                exp = exp
            }
        }
//         if helpers.Contains(upperData, "KEEPTTL") {
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
    if err != nil {
        panic(err)
    }
    h.s.RaftSet(data[3], do)


    return "+OK\r\n"
}

func Select(data []string, bucketName []byte) string {
    return "+OK\r\n"
}
