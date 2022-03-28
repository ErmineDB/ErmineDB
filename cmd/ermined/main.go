package main

import (
    "bufio"
    "flag"
    "log"
    "net"
    "github.com/ErmineDB/ErmineDB/internal/helpers"
    "github.com/ErmineDB/ErmineDB/cmd/ermined/database"
    "github.com/ErmineDB/ErmineDB/cmd/ermined/connection"
    "github.com/google/uuid"
)

var (
    addr = flag.String("addr", "127.0.0.1", "listen address without port")
    port = flag.String("port", "8888", "listen port")
    raftPort = flag.String("raftPort", "18001", "this servers raft port")
)

func init() {
    database.OpenDB()
}

func main() {

    flag.Parse()

    addrPort := *addr + ":" + *port
    log.Println("Server is running on: ", addrPort)
    server, err := net.Listen("tcp", addrPort)
    if err != nil {
        log.Fatalln(err)
    }
    defer server.Close()



    for {
        conn, err := server.Accept()
        if err != nil {
            log.Println("Failed to accept conn.", err)
            continue
        }
//         log.Println("New connection")
        id := uuid.New()
        client := helpers.Client{Socket: conn, Uuid: id}

        helpers.Manager.Store(id, client)

        go handleConn(conn, client)
    }
}


func handleConn(conn net.Conn, client helpers.Client) {
    defer conn.Close()
//     defer log.Printf("connection dropped")
//     defer pubsub.RemoveFromChannels(client)
//     defer delete(helpers.Manager, client.Uuid)
    helpers.Manager.Delete(client.Uuid)
    for {
        reader := bufio.NewReaderSize(conn, 4096)

        var data []byte
        var err error

        data, err = reader.Peek(6) // *123\r\n
        var byteSize = reader.Buffered()

        if err != nil {
//             log.Println(err)
            break
        }
//         log.Printf("data: %v", string(data[:]))
//         log.Printf("size: %v", byteSize)

        data = make([]byte, byteSize)
        reader.Read(data)
        log.Printf("data: %v", string(data[:]))

        dataString := string(data[:])
        if len(dataString) > 0 {
            splitData := connection.Parsedata(string(data[:]))
//             writeData := connection.ProcessCommand(splitData, client)
            connection.ProcessCommand(splitData, client)
//             log.Printf("writting this: %v", writeData)
//             go conn.Write([]byte(writeData))
//             writer.Write([]byte(writeData))
//             writer.Flush()
        }
//         handleConn(conn)
    }

}

