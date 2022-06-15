package main

import (
	"flag"
	"net"

	"github.com/rs/zerolog/log"

	"github.com/ErmineDB/ErmineDB/cmd/erminedb"
)

var (
	addr = flag.String("addr", "127.0.0.1", "listen address without port")
	port = flag.String("port", "8888", "Redis listen port")
)

func main() {
	log.Info().Msg("ErmindeDB starting")

	config := &erminedb.Config{Path: "/tmp", EvictionInterval: 10}
	db, err := erminedb.New(config)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create ErmineDB")
	}
	defer db.Close()

	addrPort := *addr + ":" + *port
	ermineServer, err := net.Listen("tcp", addrPort)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to listen")
	}
	defer ermineServer.Close()

	for {
		conn, err := ermineServer.Accept()
		if err != nil {
			log.Error().Err(err).Msg("Failed to accept")
			continue
		}

		go erminedb.HandleConnection(conn, db)
	}
}
