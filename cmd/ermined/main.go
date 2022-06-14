package main

import (
	"github.com/rs/zerolog/log"
)

func main() {
	log.Info().Msg("Ermine is starting")

	config := &erminedb.Config{}
	db, err := &erminedb.New(config)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create database")
	}
	defer db.Close()
}
