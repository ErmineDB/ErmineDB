package protocol

import (
//     "errorÎs"
//     "strings"
    "bytes"
    "fmt"
    "time"
    "strconv"
    "log"
//     "reflect"
    "encoding/gob"
    "github.com/gobwas/glob"
    "github.com/ErmineDB/ErmineDB/cmd/ermined/database"
    "github.com/ErmineDB/ErmineDB/internal/formatter"
    "github.com/ErmineDB/ErmineDB/internal/helpers"
)

var funcMap = map[string]interface{}{
    "Append": Append,
    "Command": Command,
    "Config": Config,
    "Dbsize": Dbsize,
    "Del": Del,
    "Exists": Exists,
    "Get": Get,
    "keys": Keys,
    "Ping": Ping,
    "Select": Select,
    "Set": Set,
}

type argObj struct {
    command string
    args []string
}

func procArgs(data []string) *argObj {
//     var obj *ArgObj

//     log.Printf("data in procArgs: %v", data)
//     obj.Command = data[1]
    obj := argObj{command: data[1]}
//     obj.Args = []string

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
    commands := []string{"Append", "Command", "Config", "Dbsize", "Del", "Exists", "Get", "Keys", "Ping", "Select", "Set"}
    return commands
}

func Call(funcName string, params ...interface{}) string {
    var results string
    switch funcName {
        case "Append":
            results = Append(params[0].([]string), []byte("bucket0"))
        case "Command":
            results = Command(params[0].([]string))
        case "Config":
            results = Config(params[0].([]string))
        case "Dbsize":
            results = Dbsize(params[0].([]string), []byte("bucket0"))
        case "Del":
            results = Del(params[0].([]string), []byte("bucket0"))
        case "Exists":
            results = Exists(params[0].([]string), []byte("bucket0"))
        case "Get":
            results = Get(params[0].([]string), []byte("bucket0"))
        case "Keys":
            results = Keys(params[0].([]string), []byte("bucket0"))
        case "Ping":
            results = Ping(params[0].([]string))
        case "Select":
            results = Select(params[0].([]string), []byte("bucket0"))
        case "Set":
            results = Set(params[0].([]string), []byte("bucket0"))
    }

    return results
}

func Command(data []string) string{
//     log.Printf("Command data %v", data)
//     return "*1\r\n*6\r\nget\r\n:2\r\n*1\r\n$8\r\nreadonly\r\n1\r\n1\r\n1\r\n"
    return "+OK\r\n"
}

func Config(data []string) string{
    return "+OK\r\n"
}

func Append(data []string, bucketName []byte) string {
    timeStamp := time.Now().UnixNano() / int64(time.Millisecond)
    var exp int64
    exp = 0
    dataObj := map[string]interface{}{
        "created": timeStamp,
        "EXP": exp,
        "type": "+",
        "data": "",
    }

    resultObj, resLen := database.QueryDB(bucketName, []byte(data[3]))
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
    database.UpdateDB(bucketName, []byte(data[3]), buf.Bytes())
    return formatter.Integer(len(dataObj["data"].(string)))

}

func Auth(data []string, bucketName []byte) string {
    return ""
}

func Dbsize(data []string, bucketName []byte) string {
    count := database.Size(bucketName)
    return formatter.Integer(count)
}


func Del(data []string, bucketName []byte) string {
//     log.Printf("data in Del: %v", data)
    //[$3 del $3 bob $3 jim $3 tom]

    deleted := 0

    for i := 3; i < len(data); i += 2 {
//         log.Printf("would have del: %v", data[i])
        _, resLen := database.QueryDB(bucketName, []byte(data[3]))
        if resLen > 0 {
            database.DeleteKey(bucketName, []byte(data[i]))
            deleted += 1
        }
    }

    return formatter.Integer(deleted)
}

func Exists(data []string, bucketName []byte) string {
//     timeStamp := time.Now().UnixNano() / int64(time.Millisecond)

    argobj := procArgs(data)

    found := 0

    for _,v := range argobj.args {
        _, resLen := database.QueryDB(bucketName, []byte(v))
        if resLen > 0 {
            found += 1
        }
    }

    return formatter.Integer(found)
}

func Get(data []string, bucketName []byte) string {
//     Get the value of a key.  If the key does not exist return nil.
//     If the value stored at key is not a string, an error is returned.
//     now := time.Now()
//     var nano int64
//     nano = now.Nanosecond()
//     // timeStamp in milliseconds
//     var timeStamp int64
    timeStamp := time.Now().UnixNano() / int64(time.Millisecond)
//     log.Printf("data is: %v %v", data, len(data))
    argobj := procArgs(data)
//     log.Printf("argobj %v", argobj.args)

    if len(argobj.args) != 1 {
        return formatter.BulkString("-ERR wrong number of argument for 'GET' command")
    }

    resultObj, resLen := database.QueryDB(bucketName, []byte(argobj.args[0]))
//     log.Printf("resultObj: %v", resultObj)

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

        exp := m["EXP"].(int64)
        if exp != 0 {
            if exp <= timeStamp {
                database.DeleteKey(bucketName, []byte(argobj.args[0]))
                return "$-1\r\n"
            }
        }


        result, _ := helpers.FindInterface(m,"data")
        if _, ok := result.(string); !ok {
            message := "-ERR WRONGTYPE Operation against a key holding the wrong kind of value"
            return formatter.BulkString(message)
        }
        return formatter.BulkString(fmt.Sprintf("%v", result))
    }
}

// func Hset(data []string, bucketName []byte) string {
//     timeStamp := time.Now().UnixNano() / int64(time.Millisecond)
//
//     //     setup a data container
//     var exp int64
//     exp = 0
//     dataObj := map[string]interface{}{
//         "created": timeStamp,
//         "EXP": exp,
//         "type": "$",
//         "data": "",
//     }
//
//     return ""
// }

func Keys(data []string, bucketName []byte) string {
    keys := database.Keys(bucketName)

    var matched []string
    var g glob.Glob
    g = glob.MustCompile(data[3])

    for _, k := range keys {
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
func Set(data []string, bucketName []byte) string {
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
    database.UpdateDB(bucketName, []byte(data[3]), buf.Bytes())
    return "+OK\r\n"
}

func Select(data []string, bucketName []byte) string {
    return "+OK\r\n"
}
