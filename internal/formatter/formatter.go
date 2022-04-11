package formatter

import (
    "log"
    "strconv"
//     "sync"
//     "unicode/utf8"
)

type ArgObj struct {
    Command string
    Args []string
}

func ProcArgs(data []string) *ArgObj {
    obj := ArgObj{Command: data[1]}

    i := 0
    for _,v := range data {
        if i > 1 {
            if (i%2==0) {
                // pass
            }else {
                obj.Args = append(obj.Args, v)
            }
        }
        i += 1
    }

    return &obj
}

func Integer(data int64) string {
    resData := ":" + strconv.FormatInt(int64(data), 10) + "\r\n"
    return resData
}

func SimpleString(data string) string {
    resData := "+" + data + "\r\n"
    return resData
}

func BulkString(data string) string {
    resData := "$" + strconv.Itoa(len([]rune(data))) + "\r\n" + data + "\r\n"

    return resData
}

func recursionCountDigits(number int) int {
    if number < 10 {
        return 1
    } else {
        return 1 + recursionCountDigits(number / 10)
    }
}

func MapToArray(data map[interface{}]interface{}) string {
    resData := "*" + strconv.Itoa(len(data)) + "\r\n"

    for _, k := range data {
        log.Println(k)
        switch k.(type) {
        case int:
//             intLen := recursionCountDigits(k.(int))
            resData += ":" + strconv.FormatInt(int64(k.(int)), 10) + "\r\n"
        case string:
            resData += "$" + strconv.Itoa(len([]rune(k.(string)))) + "\r\n" + k.(string) + "\r\n"
        default:
            log.Println("unknown")
        }
    }
    return resData
}

func List(data []string) string {
    resData := "*" + strconv.Itoa(len(data)) + "\r\n"

    for _, k := range data {
        resData += BulkString(k)
        //need to add support for other types of data
    }

    return resData
}

func ListofLists(data [][]string) string {
    resData := "*" + strconv.Itoa(len(data)) + "\r\n"

    for _, k := range data {
        resData += List(k)
        //need to add support for other types of data
    }

    return resData
}
