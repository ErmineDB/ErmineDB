package main

import (
    "bufio"
    "flag"
//     logd "log"
    "os"
    "net"
    "strings"
    "sync"
    "io/ioutil"
    "github.com/ErmineDB/ErmineDB/internal/helpers"
    "github.com/ErmineDB/ErmineDB/cmd/ermined/protocol"
    "github.com/ErmineDB/ErmineDB/cmd/ermined/pubsub"
//     "github.com/ErmineDB/ErmineDB/cmd/ermined/connection"
    "github.com/ErmineDB/ErmineDB/cmd/ermined/eRaft"
    "github.com/google/uuid"
    "github.com/hashicorp/go-hclog"
    "github.com/rs/zerolog"
    zlog "github.com/rs/zerolog/log"
)

const (
    version = "0.0.1"
)

var (
    addr = flag.String("addr", "127.0.0.1", "listen address without port")
    port = flag.String("port", "8888", "Redis listen port")
    raftPort = flag.String("raftPort", "12001", "Raft listen port")
    dataDir = flag.String("data", "./data", "Data storage directory")
    join = flag.String("join", "127.0.0.1:12001:8888", "Comma-separated list of host:raftPort:RedisPort cluster nodes")
    log     = hclog.New(&hclog.LoggerOptions{
        Name: "erminedb",
        Level: hclog.LevelFromString("DEBUG"),
    })
    lock = sync.RWMutex{}
)


func main() {
    if _, trace := os.LookupEnv("ERMINE_TRACE"); trace {
        log.SetLevel(hclog.Trace)
    } else if _, debug := os.LookupEnv("ERMINE_TRACE"); debug {
        log.SetLevel(hclog.Debug)
    }
    zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

//     log.Info().
//         Msg("version: " + version)
    splash, _ := ioutil.ReadFile("./cmd/ermined/ascii-art.txt")
    log.Info(string(splash))
//     logd.Println(string(splash))

    flag.Parse()

    addrRedisportRaftPort := *addr + ":" + *raftPort + ":" + *port
    log.Info("join list:", "join", *join)
    srv := eRaft.NewServer(*dataDir, addrRedisportRaftPort, strings.Split(*join, ","))
    if err := srv.Start(); err != nil {
        log.Error("failed to start server", "peerId", addrRedisportRaftPort, "error", err)
//         log.Error().Err(err)
    }


    hdl := protocol.NewProtoHandler(srv)

    addrPort := *addr + ":" + *port
    redisServer, err := net.Listen("tcp", addrPort)
    if err != nil {
        log.Error("There was an error: ", "error", err)
//         log.Error().Err(err)
    }
    defer redisServer.Close()



    for {
        conn, err := redisServer.Accept()
        if err != nil {
            log.Error("Failed to accept conn.", "error", err)
//             log.Error().Err(err)
            continue
        }
//         log.Info("New connection", "conn", conn)
        var remoteAddr string
        if addr, ok := conn.RemoteAddr().(*net.TCPAddr); ok {
            remoteAddr = addr.String()
            zlog.Info().
                Str("New connection", addr.String()).Send()
        }
        id := uuid.New()
        client := helpers.Client{Socket: conn, SessionId: id, Db: "bucket0", Addr: remoteAddr}

        lock.Lock()
        helpers.ClientManager[id] = client
        lock.Unlock()

        go handleConn(conn, client, *hdl)
    }
}


func handleConn(conn net.Conn, client helpers.Client, hdl protocol.ProtoHandler) {
    defer conn.Close()
    defer lock.Unlock()
    defer delete(helpers.ClientManager, client.SessionId)
    defer lock.Lock()

    for {
        reader := bufio.NewReaderSize(conn, 4096)

        var data []byte
        var err error

        data, err = reader.Peek(6) // *123\r\n
        var byteSize = reader.Buffered()

        if err != nil {
            break
        }

        data = make([]byte, byteSize)
        reader.Read(data)

        dataString := string(data[:])
        if len(dataString) > 0 {
            splitData := helpers.Parsedata(string(data[:]))
            if len(splitData) > 1 {
                commandRequested :=  strings.ToLower(splitData[1])
                commandRequested =  strings.Title(commandRequested)
                if helpers.Contains(protocol.Commands(), commandRequested, false) {
//                     zlog.Info().
//                         Str("command", string(data)).
//                         Str("user", client.User).
//                         Str("remoteAddr", client.Addr).Send()
                    lock.Lock()
                    cDB := helpers.ClientManager[client.SessionId]
                    lock.Unlock()
                    resData := protocol.Call(commandRequested, splitData, hdl, cDB)
                    client.Socket.Write([]byte(resData))
                } else if helpers.Contains(pubsub.Commands(), commandRequested, false) {
                    pubsub.Call(commandRequested, client, splitData)
                } else {
                    client.Socket.Write([]byte("$-1\r\n"))
                }
            } else {
                client.Socket.Write([]byte("$-1\r\n"))
            }
        }
    }
}

