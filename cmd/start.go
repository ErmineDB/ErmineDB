package main

import (
	"flag"
	"net"
	"os"
	"strings"

	"github.com/hashicorp/go-hclog"
	zlog "github.com/rs/zerolog/log"

	"github.com/ErmineDB/ErmineDB/cmd/erminedb"
)

var (
	// addr = flag.String("addr", "127.0.0.1", "listen address without port")
	// port = flag.String("port", "8888", "Redis listen port")

	addr     = flag.String("addr", "127.0.0.1", "listen address without port")
	port     = flag.String("port", "8888", "Redis listen port")
	raftPort = flag.String("raftPort", "12001", "Raft listen port")
	dataDir  = flag.String("data", "/tmp", "Data storage directory")
	join     = flag.String("join", "127.0.0.1:12001:8888", "Comma-separated list of host:raftPort:RedisPort cluster nodes")
	log      = hclog.New(&hclog.LoggerOptions{
		Name:  "erminedb",
		Level: hclog.LevelFromString("DEBUG"),
	})
)

func main() {
	zlog.Info().Msg("ErmindeDB starting")

	if _, trace := os.LookupEnv("ERMINE_TRACE"); trace {
		log.SetLevel(hclog.Trace)
	} else if _, debug := os.LookupEnv("ERMINE_TRACE"); debug {
		log.SetLevel(hclog.Debug)
	}

	flag.Parse()

	addrRedisportRaftPort := *addr + ":" + *raftPort + ":" + *port
	log.Info("join list:", "join", *join)
	srv := erminedb.NewServer(*dataDir, addrRedisportRaftPort, strings.Split(*join, ","))
	if err := srv.Start(); err != nil {
		log.Error("failed to start server", "peerId", addrRedisportRaftPort, "error", err)
		//         log.Error().Err(err)
	}

	hdl := erminedb.NewProtoHandler(srv)

	addrPort := *addr + ":" + *port
	ermineServer, err := net.Listen("tcp", addrPort)
	if err != nil {
		log.Error("There was an error: ", "error", err)
		//         log.Error().Err(err)
	}
	defer ermineServer.Close()

	for {
		conn, err := ermineServer.Accept()
		if err != nil {
			zlog.Error().Err(err).Msg("Failed to accept")
			continue
		}
		go erminedb.HandleConnection(conn, hdl.S.Store)
		// var remoteAddr string
		// if addr, ok := conn.RemoteAddr().(*net.TCPAddr); ok {
		// 	remoteAddr = addr.String()
		// 	zlog.Info().
		// 		Str("New connection", addr.String()).Send()
		// }
		// id := uuid.New()
		// client := helpers.Client{Socket: conn, SessionId: id, Db: "bucket0", Addr: remoteAddr}

		// lock.Lock()
		// helpers.ClientManager[id] = client
		// lock.Unlock()

		// go handleConn(conn, client, *hdl)
	}

	// old
	// config := &erminedb.Config{Path: "/tmp", EvictionInterval: 120, NoSync: true}
	// // config := &erminedb.Config{Path: "", EvictionInterval: 10}
	// db, err := erminedb.New(config)
	// if err != nil {
	// 	zlog.Fatal().Err(err).Msg("Failed to create ErmineDB")
	// }
	// defer db.Close()

	// addrPort := *addr + ":" + *port
	// ermineServer, err := net.Listen("tcp", addrPort)
	// if err != nil {
	// 	zlog.Fatal().Err(err).Msg("Failed to listen")
	// }
	// defer ermineServer.Close()

	// for {
	// 	conn, err := ermineServer.Accept()
	// 	if err != nil {
	// 		zlog.Error().Err(err).Msg("Failed to accept")
	// 		continue
	// 	}

	// 	go erminedb.HandleConnection(conn, db)
	// }
}
