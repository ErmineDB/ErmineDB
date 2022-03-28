package connection

import (
//     "log"
    "strings"
    "github.com/ErmineDB/ErmineDB/internal/helpers"
    "github.com/ErmineDB/ErmineDB/cmd/ermined/protocol"
    "github.com/ErmineDB/ErmineDB/cmd/ermined/pubsub"
)

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
    if helpers.Contains(dataTypes, dataC1) {
        // This must be a single command
//         log.Printf("Parsedata, data %v", data)
        splitData = strings.Split(data[4:], "\r\n")
        if splitData[len(splitData) - 1] == "" {
            splitData = helpers.RemoveIndex(splitData, len(splitData) - 1)
        }
    }
    //else {
        // This must be a pipeline or bad data
    //}
//     log.Printf("splitData in connection %v", splitData)
//     log.Printf("%v", len(splitData))
    return splitData
}

// func ProcessCommand(data []string, db map[string]interface{}) string {
func ProcessCommand(data []string, client helpers.Client) {
//     log.Printf("data in conneciton %v", data)
//     var writeData string
    if len(data) > 1 {
        commandRequested :=  strings.ToLower(data[1])
        commandRequested =  strings.Title(commandRequested)
//         log.Printf("upper data in conneciton %v", commandRequested)
        if helpers.Contains(protocol.Commands(), commandRequested) {
            resData := protocol.Call(commandRequested, data)
//             writeData = resData
            client.Socket.Write([]byte(resData))
        } else if helpers.Contains(pubsub.Commands(), commandRequested) {
            pubsub.Call(commandRequested, client, data)
        } else {
//             writeData = "$-1\r\n"
            client.Socket.Write([]byte("$-1\r\n"))
        }
    } else {
//         writeData = "$-1\r\n"
        client.Socket.Write([]byte("$-1\r\n"))
    }

    return
}
