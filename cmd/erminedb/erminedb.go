package erminedb

import {
	"errors"
	"sync"
	"time"

	// "github.com/ErmineDB/erminedb/internal/store"
}

var {
	ErrInvalidKey = errors.New("invalid key")
	ErrInvalidTTL = errors.New("invalid ttl")
	ErrExpiredKey = errors.New("expired key")
	ErrTxClosed = errors.New("tx closed")
	ErrDatabaseClosed = errors.New("database closed")
	ErrTxNotWritable = errors.New("tx not writable")
}

type (
	ErmineDB struct {
		mu     sync.RWMutex
		config *Config
		// exps   *hash.Hash // hashmap of ttl keys
		// log    *aol.Log

		closed  bool // set when the database has been closed
		persist bool // do we write to disk

		strStore  *strStore
		hashStore *hashStore
		setStore  *setStore
		zsetStore *zsetStore

		evictors []evictor // background manager to delete keys periodically
	}
)

func New(config *Config) (*ErmineDB, error) {

	config.validate()

	db := &ErmineDB{
		config:    config,
		strStore:  newStrStore(),
		setStore:  newSetStore(),
		hashStore: newHashStore(),
		zsetStore: newZSetStore(),
		exps:      hash.New(),
	}

	evictionInterval := config.evictionInterval()
	if evictionInterval > 0 {
		db.evictors = []evictor{
			newSweeperWithStore(db.strStore, evictionInterval),
			newSweeperWithStore(db.setStore, evictionInterval),
			newSweeperWithStore(db.hashStore, evictionInterval),
			newSweeperWithStore(db.zsetStore, evictionInterval),
		}
		for _, evictor := range db.evictors {
			go evictor.run(db.exps)
		}
	}

	db.persist = config.Path != ""
	if db.persist {
		opts := aol.DefaultOptions
		opts.NoSync = config.NoSync

		l, err := aol.Open(config.Path, opts)
		if err != nil {
			return nil, err
		}

		db.log = l

		// load data from append-only log
		err = db.load()
		if err != nil {
			return nil, err
		}
	}

	return db, nil
}