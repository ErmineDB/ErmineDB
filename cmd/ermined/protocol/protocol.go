package protocol

import (
//     "errorÎs"
    "strings"
    "bytes"
    "fmt"
    "time"
    "strconv"
    "log"
//     "reflect"
    "encoding/gob"
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



// var funcMap = map[string]interface{}{
//     "Append": Append,
//     "Command": Command,
//     "Config": Config,
//     "Dbsize": Dbsize,
//     "Del": Del,
//     "Exists": Exists,
//     "Get": Get,
//     "keys": Keys,
//     "Ping": Ping,
//     "Select": Select,
//     "Set": Set,
// }

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
    commands := []string{"Append", "Command", "Config", "Dbsize", "Del", "Exists", "Get", "Hget", "Hset", "Keys", "Ping", "Select", "Set"}
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
        case "Hget":
            hdl := params[1].(ProtoHandler)
            results = hdl.Hget(params[0].([]string), []byte("bucket0"))
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

//     if err := configFuture.Error(); err != nil {
//         log.Printf("config error")
//         return "$-1\r\n"
//     } else {
//     for _, peer := range configFuture.Configuration().Servers {
//         log.Printf("%v", peer.Address)
//         if peer.Address == h.s.Raft.Leader() {
//     x := fmt.Sprintf("-MOVED 0 %v \r\n", h.s.Raft.Leader())
//     log.Printf("This is x %v", x)
//     return x
//                 peerRaft, httpPort := parsePeer(string(peer.ID))
// 				url = fmt.Sprintf("http://%s:%s", strings.Split(peerRaft, ":")[0], httpPort) // TODO this may need to be customized
//         }
//     }
//         if len(url) == 0 {
// 			onError(w, errors.New("leader not found"), http.StatusBadGateway)
//             return "$-1\r\n"
//         }
//     }
// //     log.Printf("why did I make it here?")
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
    dataObj := map[string]interface{}{
        "created": timeStamp,
        "EXP": exp,
        "type": "+",
        "data": "",
    }

    argobj := procArgs(data)
//     resultObj, resLen := database.QueryDB(bucketName, []byte(data[3]))
    resultObj, _ := h.s.Store.GetData([]byte(argobj.args[0]))
    resLen := len(resultObj)

    if resLen != 0 {
        buf := bytes.NewBuffer(resultObj)
        dec := gob.NewDecoder(buf)
        m := make(map[string]interface{})
        if err := dec.Decode(&m); err != nil {
            log.Fatal(err)
        }

        oldData, _ := helpers.FindInterface(m,"data")
//         log.Printf("oldData in Append: %v", oldData)
        dataObj["data"] = oldData.(string) + data[5]
    } else {

        dataObj["data"] = data[5]
    }

    var buf bytes.Buffer
    enc := gob.NewEncoder(&buf)
    if err := enc.Encode(dataObj); err != nil {
        log.Fatal(err)
    }
//     database.UpdateDB(bucketName, []byte(data[3]), buf.Bytes())
    h.s.RaftSet(data[3], buf.Bytes())
    return formatter.Integer(len(dataObj["data"].(string)))

}

func Auth(data []string, bucketName []byte) string {
    return ""
}

func (h *ProtoHandler) Dbsize(data []string, bucketName []byte) string {
//     count := 0
    var n uint16 = 65535
    keys, _ := h.s.Store.KeysOf([]byte(""), []byte("0"), n)
    count := len(keys.Keys)

    return formatter.Integer(count)
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

    return formatter.Integer(deleted)
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

    return formatter.Integer(found)
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

    if resLen == 0 {
//         log.Printf("returning because reLen == 0")
        return "$-1\r\n"
    } else {
        buf := bytes.NewBuffer(resultObj)
        dec := gob.NewDecoder(buf)
        m := make(map[string]interface{})
        if err := dec.Decode(&m); err != nil {
            log.Fatal(err)
        }

//         exp := m["EXP"].(int64)
//         if exp != 0 {
//             if exp <= timeStamp {
//                 database.DeleteKey(bucketName, []byte(argobj.args[0]))
//                 return "$-1\r\n"
//             }
//         }


        result, _ := helpers.FindInterface(m,"data")
        if _, ok := result.(string); !ok {
            message := "-ERR WRONGTYPE Operation against a key holding the wrong kind of value"
            return formatter.BulkString(message)
        }
        return formatter.BulkString(fmt.Sprintf("%v", result))
    }
}


func (h *ProtoHandler) Hget(data []string, bucketName []byte) string {
    argobj := procArgs(data)
    gob.Register(map[string]string{})

    resultObj, _ := h.s.Store.GetData([]byte(argobj.args[0]))
    resLen := len(resultObj)

    if resLen == 0 {
//         log.Printf("returning because reLen == 0")
        return "$-1\r\n"
    } else {
        buf := bytes.NewBuffer(resultObj)
        dec := gob.NewDecoder(buf)
        m := make(map[string]interface{})
        if err := dec.Decode(&m); err != nil {
            log.Fatal(err)
        }

        if m["type"] != "hash" {
            message := "-ERR WRONGTYPE Operation against a key holding the wrong kind of value"
            return formatter.BulkString(message)
        }
        result, _ := helpers.FindInterface(m,"data")
        mobj, _ := result.(map[string]string)
        val, found := mobj[argobj.args[1]]

        log.Printf("This is result: %v", result)
        if found {
            log.Printf("This is found: %v", val)
            return formatter.BulkString(fmt.Sprintf("%v", val))
        }
        return "$-1\r\n"
    }
}

func (h *ProtoHandler) Hset(data []string, bucketName []byte) string {
    log.Printf("Doing Hset")
    if h.s.Raft.State() != raft.Leader {
        return h.forwardToLeader(data)
    }

    gob.Register(map[string]string{})
    timeStamp := time.Now().UnixNano() / int64(time.Millisecond)

    //     setup a data container
    var exp int64
    exp = 0
    dataObj := map[string]interface{}{
        "created": timeStamp,
        "EXP": exp,
        "type": "hash",
        "data": map[string]string{},
    }

    argobj := procArgs(data)
    log.Printf("argobj: ", argobj)
    d := make(map[string]string)
    c := 0
    for i:=1; i<len(argobj.args); i = i+2 {
        if i+1 < len(argobj.args) {
            d[argobj.args[i]] = argobj.args[i+1]
            c += 1
        }
    }
    dataObj["data"] = d
    log.Printf("datObj: ", dataObj)

    var buf bytes.Buffer
    enc := gob.NewEncoder(&buf)
    if err := enc.Encode(dataObj); err != nil {
        log.Fatal(err)
    }
    h.s.RaftSet(data[3], buf.Bytes())

    return formatter.Integer(c)
}

func (h *ProtoHandler) Keys(data []string, bucketName []byte) string {
//     keys := database.Keys(bucketName)
    var n uint16 = 65535
    keys, _ := h.s.Store.KeysOf([]byte(""), []byte("0"), n)
    log.Printf("Keys %v", keys.Keys)
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
    dataObj := map[string]interface{}{
        "created": timeStamp,
        "EXP": exp,
        "type": "$",
        "data": "",
    }

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

        dataObj["EXP"] = exp
    } else if len(data) == 6 {
//         log.Printf("this is good %v", len(data))
        _ = dataObj
    }else {
        return "-ERR syntax error\r\n"
    }

    dataObj["data"] = data[5]
    var buf bytes.Buffer
    enc := gob.NewEncoder(&buf)
    if err := enc.Encode(dataObj); err != nil {
        log.Fatal(err)
    }
//     database.UpdateDB(bucketName, []byte(data[3]), buf.Bytes())
    h.s.RaftSet(data[3], buf.Bytes())
    return "+OK\r\n"
}

func Select(data []string, bucketName []byte) string {
    return "+OK\r\n"
}
