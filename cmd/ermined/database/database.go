package database

import (
//     "bufio"
//     "io"
//     "os"
//     "time"
    "log"
//     "fmt"
    badger "github.com/dgraph-io/badger/v3"
)

var (
    db *badger.DB
)

func OpenDB() *badger.DB{
    var err error

    opts := badger.DefaultOptions("")
    opts.Dir = "bdb.db"
    opts.ValueDir = "bdb.db"
//     opts.InMemory = true
//     opts.IndexCacheSize = 100
//     opts.SyncWrites = true
    db, err = badger.Open(opts)
//     db, err = badger.Open(badger.DefaultOptions("bdb.db"))

    if err != nil {
		log.Fatal(err)
	}
	return db
}

func Keys(bucketName []byte) []string {
    var keys []string
    err := db.View(func(txn *badger.Txn) error {
        opts := badger.DefaultIteratorOptions
        opts.PrefetchValues = false
        it := txn.NewIterator(opts)
        defer it.Close()
        for it.Rewind(); it.Valid(); it.Next() {
            item := it.Item()
//             k := item.Key()
//             fmt.Printf("key=%s\n", k)
            keys = append(keys, string(item.Key()))
        }
        return nil
    })
    if err != nil {
        log.Printf("%v", err)
    }
    return keys
}

func Size(bucketName []byte) int {
    count := 0
    err := db.View(func(txn *badger.Txn) error {
        opts := badger.DefaultIteratorOptions
        opts.PrefetchValues = false
        it := txn.NewIterator(opts)
        defer it.Close()
        for it.Rewind(); it.Valid(); it.Next() {
//             item := it.Item()
//             k := item.Key()
//             fmt.Printf("key=%s\n", k)
            count += 1
        }
        return nil
    })
    if err != nil {
        log.Printf("%v", err)
    }
    return count
}

func UpdateDB(bucketName, key, value []byte) {
    _ = bucketName

    txn := db.NewTransaction(true)
    if err := txn.Set([]byte(key),[]byte(value)); err == badger.ErrTxnTooBig {
        _ = txn.Commit()
        txn = db.NewTransaction(true)
        _ = txn.Set([]byte(key),[]byte(value))
    }

    _ = txn.Commit()
}


func QueryDB(bucketName, key []byte) (val []byte, length int) {

	err := db.View(func(txn *badger.Txn) error {
// 		bkt := tx.Bucket(bucketName)
// 		if bkt == nil {
// 			return fmt.Errorf("Bucket %q not found!", bucketName)
// 		}
		item, err := txn.Get([]byte(key))
        if err != nil {
            return nil
        }
        val, _ = item.ValueCopy(nil)
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	return val, len(string(val))
}

func DeleteKey(bucketName, key []byte) {
    _ = bucketName

    txn := db.NewTransaction(true)
    if err := txn.Delete([]byte(key)); err == badger.ErrTxnTooBig {
        _ = txn.Commit()
        txn = db.NewTransaction(true)
        _ = txn.Delete([]byte(key))
    }

    _ = txn.Commit()
}

