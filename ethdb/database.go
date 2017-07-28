// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package ethdb

import (
	"context"
	"io/ioutil"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"

	gometrics "github.com/rcrowley/go-metrics"
	// Imports the Google Cloud Datastore client package.
	googleDataStore "cloud.google.com/go/datastore"
	googleStore "cloud.google.com/go/storage"
	sha3 "golang.org/x/crypto/sha3"
	// googleIterator "google.golang.org/api/iterator"
	b64 "encoding/base64"
	"encoding/hex"
)

type KeyValueBlockchain struct {
	KeyBlokchain []byte
	DataGCS      string
	//Base64       string
}

var OpenFileLimit = 64

type LDBDatabase struct {
	fn string      // filename for reporting
	db *leveldb.DB // LevelDB instance

	//DJ
	ctx       context.Context //context google
	projectID string
	kindGCD   string
	bucketGCD string
	clientGCD *googleDataStore.Client
	clientGCS *googleStore.Client
	isServer  bool
	//DJ termino

	getTimer       gometrics.Timer // Timer for measuring the database get request counts and latencies
	putTimer       gometrics.Timer // Timer for measuring the database put request counts and latencies
	delTimer       gometrics.Timer // Timer for measuring the database delete request counts and latencies
	missMeter      gometrics.Meter // Meter for measuring the missed database get requests
	readMeter      gometrics.Meter // Meter for measuring the database get request data usage
	writeMeter     gometrics.Meter // Meter for measuring the database put request data usage
	compTimeMeter  gometrics.Meter // Meter for measuring the total time spent in database compaction
	compReadMeter  gometrics.Meter // Meter for measuring the data read during compaction
	compWriteMeter gometrics.Meter // Meter for measuring the data written during compaction

	quitLock sync.Mutex      // Mutex protecting the quit channel access
	quitChan chan chan error // Quit channel to stop the metrics collection before closing the database

	log log.Logger // Contextual logger tracking the database path
}

// NewLDBDatabase returns a LevelDB wrapped object.
func NewLDBDatabase(file string, _isServer bool, _projectId string,
	_kind string, _bucket string, cache int, handles int) (*LDBDatabase, error) {
	logger := log.New("database", file)

	logger.Info("bucket: " + _bucket)
	logger.Info("isServer: " + strconv.FormatBool(_isServer))
	logger.Info("projectId: " + _projectId)
	logger.Info("kind: " + _kind)

	if len(_bucket) < 1 || len(_projectId) < 1 || len(_kind) < 1 {
		errorParamsGCS := errors.New("No exist params  GCS")
		return nil, errorParamsGCS
	}

	// Ensure we have some minimal caching and file guarantees
	if cache < 16 {
		cache = 16
	}
	if handles < 16 {
		handles = 16
	}
	logger.Info("Allocated cache and file handles", "cache", cache, "handles", handles)

	// Open the db and recover any potential corruptions
	db, err := leveldb.OpenFile(file, &opt.Options{
		OpenFilesCacheCapacity: handles,
		BlockCacheCapacity:     cache / 2 * opt.MiB,
		WriteBuffer:            cache / 4 * opt.MiB, // Two of these are used internally
		Filter:                 filter.NewBloomFilter(10),
	})
	if _, corrupted := err.(*errors.ErrCorrupted); corrupted {
		db, err = leveldb.RecoverFile(file, nil)
	}
	// (Re)check for errors and abort if opening of the db failed
	if err != nil {
		return nil, err
	}

	//DJ
	ctx := context.Background()

	// Set your Google Cloud Platform project ID.
	projectID := _projectId //"geth2-173819"
	kindGCD := _kind        //"BlockchainEthereum"
	bucketGCD := _bucket    //"raw-blockchain"

	var clientGCD *googleDataStore.Client
	var errGCD error
	var clientGCS *googleStore.Client
	var errGCS error
	clientGCD = nil
	errGCD = nil
	clientGCS = nil
	errGCS = nil

	clientGCD, errGCD = googleDataStore.NewClient(ctx, projectID)

	if errGCD != nil {
		logger.Error("Failed to create client Google DataStore: %v", errGCD)
		logger.Error("so, can not create Google Storage")
		clientGCD = nil

	}

	if clientGCD != nil {
		clientGCS, errGCS = googleStore.NewClient(ctx)
		if errGCS != nil {
			logger.Error("Failed to create client Google Storage: %v", errGCS)
			errGCDClose := clientGCD.Close()
			if errGCDClose == nil {
				logger.Error("ClientGCD closed")
			} else {
				logger.Error("Failed to close ClientGCD", "err", errGCDClose)
			}
			clientGCD = nil
			clientGCS = nil
		}
	}

	if clientGCD == nil || clientGCS == nil {
		errDb := db.Close()
		if err == nil {
			logger.Error("Database closed")
		} else {
			logger.Error("Failed to close database", "err", errDb)
			return nil, errDb
		}
	}

	if clientGCD == nil {
		return nil, errGCD
	}

	if clientGCS == nil {
		return nil, errGCS
	}
	//DJ
	return &LDBDatabase{
		fn:  file,
		db:  db,
		log: logger,
		//DJ
		ctx:       ctx,
		projectID: projectID,
		kindGCD:   kindGCD,
		bucketGCD: bucketGCD,
		clientGCD: clientGCD,
		clientGCS: clientGCS,
		isServer:  _isServer,
		//DJ
	}, nil
}

// Path returns the path to the database directory.
func (db *LDBDatabase) Path() string {
	return db.fn
}

// Put puts the given key / value to the queue
func (db *LDBDatabase) Put(key []byte, value []byte) error {

	// Measure the database put latency, if requested
	if db.putTimer != nil {
		defer db.putTimer.UpdateSince(time.Now())
	}
	// Generate the data to write to disk, update the meter and write
	//value = rle.Compress(value)

	if db.writeMeter != nil {
		db.writeMeter.Mark(int64(len(value)))
	}

	//DJ

	if db.isServer == false {
		return nil
	}
	// Creates a BlockchainEthereumData instance.

	idElementFile := int64(time.Now().Unix())
	h := sha3.New224()
	h.Write(value)
	nameElementFile := hex.EncodeToString(h.Sum(nil))
	nameElementFile = strconv.FormatInt(idElementFile, 10) + nameElementFile[0:len(nameElementFile)/2] // sha3_hash

	//part GCS

	// Creates a Bucket instance.
	bucketGCS := db.clientGCS.Bucket(db.bucketGCD)
	objGCS := bucketGCS.Object(nameElementFile)
	wGCS := objGCS.NewWriter(db.ctx)

	if _, err := wGCS.Write(value); err != nil {
		db.log.Error("Failed to save file to put levelDB clientGCS: %v bucket: %v , nameFile: %v", err, db.bucketGCD, nameElementFile)

		if errClose := wGCS.Close(); errClose != nil {
			db.log.Error("Failed to close file to put levelDB clientGCS: %v bucket: %v , nameFile: %v", errClose, db.bucketGCD, nameElementFile)
			return errClose

		}

		return err

	}

	// Close, just like writing a file.
	if err := wGCS.Close(); err != nil {
		db.log.Error("Failed to close file to put levelDB clientGCS: %v bucket: %v , nameFile: %v", err, db.bucketGCD, nameElementFile)
		return err

	}

	blockchainEthereumData := KeyValueBlockchain{
		KeyBlokchain: key,
		DataGCS:      nameElementFile,
	}

	nameElement := b64.StdEncoding.EncodeToString(key)
	blockchainEthereumKeyNew := googleDataStore.NameKey(db.kindGCD, nameElement, nil)
	isUpdate := true
	kvRestoreBlockchain := &KeyValueBlockchain{}
	if errRestore := db.clientGCD.Get(db.ctx, blockchainEthereumKeyNew, kvRestoreBlockchain); errRestore != nil {
		if errRestore == googleDataStore.ErrNoSuchEntity {
			isUpdate = false
		} else {
			db.log.Error("Failed init rollback to put levelDB clientGCD: %v", errRestore)
			return errRestore
		}

	}

	if _, err := db.clientGCD.Put(db.ctx, blockchainEthereumKeyNew, &blockchainEthereumData); err != nil {
		db.log.Error("Failed to save to put levelDB clientGCD: %v , keyGCD: %v ,keyGCS: %v", err, nameElement, nameElementFile)
		return err
	}

	errDB := db.db.Put(key, value, nil)
	if errDB != nil /* || db.clientGCD == nil*/ {
		if isUpdate {
			if _, err := db.clientGCD.Put(db.ctx, blockchainEthereumKeyNew, &kvRestoreBlockchain); err != nil {
				db.log.Error("Failed to rollbackPut to put levelDB  clientGCD: %v , keyGCD: %v ,keyGCS: %v", err, nameElement, kvRestoreBlockchain.DataGCS)
				return err
			}

		} else {
			//delete service
			if errDelete := db.clientGCD.Delete(db.ctx, blockchainEthereumKeyNew); errDelete != nil {
				db.log.Error("Failed to rollbackDelete to put levelDB  clientGCD : %v , keyGCD: %v", errDelete, nameElement)
				return errDelete
			}
		}

		db.log.Error("Failed to put levelDB: %v , key: %v ", errDB, key)
		return errDB
	}
	//DJ termino
	return errDB
}

// Get returns the given key if it's present.
func (db *LDBDatabase) Get(key []byte) ([]byte, error) {
	// Measure the database get latency, if requested
	if db.getTimer != nil {
		defer db.getTimer.UpdateSince(time.Now())
	}

	existElement := true

	byteGCS, errReadFileGCS := getDataGC(db, key)
	if db.isServer == false {

		if errReadFileGCS != nil {
			if db.missMeter != nil {
				db.missMeter.Mark(1)
			}
			if errReadFileGCS == googleDataStore.ErrNoSuchEntity {
				return nil, errors.ErrNotFound
			}
			return nil, errReadFileGCS
		}
		if db.readMeter != nil {
			db.readMeter.Mark(int64(len(byteGCS)))
		}

		return byteGCS, nil
	}

	if errReadFileGCS == googleDataStore.ErrNoSuchEntity {
		existElement = false
	}

	//part serverMaster
	dat, err := db.db.Get(key, nil)
	if err != nil {
		if existElement == false && err != errors.ErrNotFound {
			return nil, errors.New("difference between levelDB and GCS")
		}
		if db.missMeter != nil {
			db.missMeter.Mark(1)
		}
		return nil, err
	}

	if !compareData(byteGCS, dat) {

		if db.missMeter != nil {
			db.missMeter.Mark(1)
		}

		return nil, errors.New("difference between levelDB and GCS")
	}

	// Otherwise update the actually retrieved amount of data
	if db.readMeter != nil {
		db.readMeter.Mark(int64(len(dat)))
	}

	return dat, nil
	//return rle.Decompress(dat)
}
func getDataGC(db *LDBDatabase, key []byte) ([]byte, error) {
	var byteGCS []byte
	var errReadFileGCS error

	nameElement := b64.StdEncoding.EncodeToString(key)
	blockchainEthereumKeyGet := googleDataStore.NameKey(db.kindGCD, nameElement, nil)
	kvBlockchain := &KeyValueBlockchain{}
	errGetElement := db.clientGCD.Get(db.ctx, blockchainEthereumKeyGet, kvBlockchain)
	if errGetElement != nil {

		if errGetElement != googleDataStore.ErrNoSuchEntity {
			db.log.Error("Failed to get levelDB clientGCD: %v, %v", key, errGetElement)
		}

		//db.log.Error("Failed to get levelDB clientGCD: %v, %v",key,errGetElement)
		//return nil, errGet
		return nil, errGetElement
	}

	// Creates a Bucket instance.
	bucketGCS := db.clientGCS.Bucket(db.bucketGCD)
	objGCS := bucketGCS.Object(kvBlockchain.DataGCS)
	rGCS, errReadGCS := objGCS.NewReader(db.ctx)
	if errReadGCS != nil {

		db.log.Error("Failed to get levelDB clientGCS[read]: %v nameFile: %v", errReadGCS, kvBlockchain.DataGCS)
		return nil, errReadGCS
	}

	byteGCS, errReadFileGCS = ioutil.ReadAll(rGCS)
	if errReadFileGCS != nil {
		db.log.Error("Failed to get levelDB clientGCS[readFile]: %v nameFile: %v", errReadFileGCS, kvBlockchain.DataGCS)
		if errReadCloseGCS := rGCS.Close(); errReadCloseGCS != nil {

			db.log.Error("Failed to get levelDB clientGCS[close]: %v nameFile: %v", errReadCloseGCS, kvBlockchain.DataGCS)
			return nil, errReadCloseGCS
		}

		return nil, errReadFileGCS
	}

	if errReadCloseGCS := rGCS.Close(); errReadCloseGCS != nil {

		db.log.Error("Failed to get levelDB clientGCS[close]: %v nameFile: %v", errReadCloseGCS, kvBlockchain.DataGCS)
		return nil, errReadCloseGCS
	}

	return byteGCS, nil

}

func compareData(dataLevelDB []byte, dataGC []byte) bool {
	hGCS := sha3.New224()
	hGCS.Write(dataGC)
	sha3GCS := hex.EncodeToString(hGCS.Sum(nil))

	hLevelDB := sha3.New224()
	hLevelDB.Write(dataLevelDB)
	sha3LevelDB := hex.EncodeToString(hLevelDB.Sum(nil))
	if strings.Compare(sha3LevelDB, sha3GCS) != 0 {
		return false
	}
	return true

}

// Delete deletes the key from the queue and database
func (db *LDBDatabase) Delete(key []byte) error {

	// Measure the database delete latency, if requested
	if db.delTimer != nil {
		defer db.delTimer.UpdateSince(time.Now())
	}
	// Execute the actual operation

	//DJ

	if db.isServer == false {
		return nil
	}
	nameElement := b64.StdEncoding.EncodeToString(key)
	blockchainEthereumKeyDelete := googleDataStore.NameKey(db.kindGCD, nameElement, nil)

	kvRestoreBlockchain := &KeyValueBlockchain{}
	if errRestore := db.clientGCD.Get(db.ctx, blockchainEthereumKeyDelete, kvRestoreBlockchain); errRestore != nil {
		db.log.Error("Failed init rollback to delete levelDB clientGCD: %v , keyGCD: %v", errRestore, nameElement)
		return errRestore
	}

	//delete service
	if errDelete := db.clientGCD.Delete(db.ctx, blockchainEthereumKeyDelete); errDelete != nil {
		db.log.Error("Failed to delete levelDB clientGCD: %v , keyGCD: %v", errDelete, nameElement)
		return errDelete
	}

	errDB := db.db.Delete(key, nil)
	if errDB != nil /*|| db.clientGCD == nil*/ {
		if _, errPut := db.clientGCD.Put(db.ctx, blockchainEthereumKeyDelete, &kvRestoreBlockchain); errPut != nil {
			db.log.Error("Failed to rollbackPut to delete levelDB clientGCD: %v , keyGCD: %v ,keyGCS: %v", errPut, nameElement, kvRestoreBlockchain.DataGCS)
			return errPut
		}
		db.log.Error("Failed to delete levelDB: %v ,key : %v", errDB, key)
		return errDB
	}

	//DJ termino

	return errDB
}

func (db *LDBDatabase) NewIterator() iterator.Iterator {
	return db.db.NewIterator(nil, nil)
}

func (db *LDBDatabase) Close() {
	// Stop the metrics collection to avoid internal database races
	db.quitLock.Lock()
	defer db.quitLock.Unlock()

	if db.quitChan != nil {
		errc := make(chan error)
		db.quitChan <- errc
		if err := <-errc; err != nil {
			db.log.Error("Metrics collection failed", "err", err)
		}
	}
	err := db.db.Close()
	if err == nil {
		db.log.Info("Database closed")
	} else {
		db.log.Error("Failed to close database", "err", err)
	}

	//if db.clientGCD != nil {

	errGCD := db.clientGCD.Close()
	if errGCD == nil {
		db.log.Info("ClientGCD closed")
	} else {
		db.log.Error("Failed to close ClientGCD", "err", errGCD)
	}

	errGCS := db.clientGCS.Close()
	if errGCS == nil {
		db.log.Info("clientGCS closed")
	} else {
		db.log.Error("Failed to close clientGCS", "err", errGCS)
	}
	//}

}

func (db *LDBDatabase) LDB() *leveldb.DB {
	return db.db
}

// Meter configures the database metrics collectors and
func (db *LDBDatabase) Meter(prefix string) {
	// Short circuit metering if the metrics system is disabled
	if !metrics.Enabled {
		return
	}
	// Initialize all the metrics collector at the requested prefix
	db.getTimer = metrics.NewTimer(prefix + "user/gets")
	db.putTimer = metrics.NewTimer(prefix + "user/puts")
	db.delTimer = metrics.NewTimer(prefix + "user/dels")
	db.missMeter = metrics.NewMeter(prefix + "user/misses")
	db.readMeter = metrics.NewMeter(prefix + "user/reads")
	db.writeMeter = metrics.NewMeter(prefix + "user/writes")
	db.compTimeMeter = metrics.NewMeter(prefix + "compact/time")
	db.compReadMeter = metrics.NewMeter(prefix + "compact/input")
	db.compWriteMeter = metrics.NewMeter(prefix + "compact/output")

	// Create a quit channel for the periodic collector and run it
	db.quitLock.Lock()
	db.quitChan = make(chan chan error)
	db.quitLock.Unlock()

	go db.meter(3 * time.Second)
}

// meter periodically retrieves internal leveldb counters and reports them to
// the metrics subsystem.
//
// This is how a stats table look like (currently):
//   Compactions
//    Level |   Tables   |    Size(MB)   |    Time(sec)  |    Read(MB)   |   Write(MB)
//   -------+------------+---------------+---------------+---------------+---------------
//      0   |          0 |       0.00000 |       1.27969 |       0.00000 |      12.31098
//      1   |         85 |     109.27913 |      28.09293 |     213.92493 |     214.26294
//      2   |        523 |    1000.37159 |       7.26059 |      66.86342 |      66.77884
//      3   |        570 |    1113.18458 |       0.00000 |       0.00000 |       0.00000
func (db *LDBDatabase) meter(refresh time.Duration) {
	// Create the counters to store current and previous values
	counters := make([][]float64, 2)
	for i := 0; i < 2; i++ {
		counters[i] = make([]float64, 3)
	}
	// Iterate ad infinitum and collect the stats
	for i := 1; ; i++ {
		// Retrieve the database stats
		stats, err := db.db.GetProperty("leveldb.stats")
		if err != nil {
			db.log.Error("Failed to read database stats", "err", err)
			return
		}
		// Find the compaction table, skip the header
		lines := strings.Split(stats, "\n")
		for len(lines) > 0 && strings.TrimSpace(lines[0]) != "Compactions" {
			lines = lines[1:]
		}
		if len(lines) <= 3 {
			db.log.Error("Compaction table not found")
			return
		}
		lines = lines[3:]

		// Iterate over all the table rows, and accumulate the entries
		for j := 0; j < len(counters[i%2]); j++ {
			counters[i%2][j] = 0
		}
		for _, line := range lines {
			parts := strings.Split(line, "|")
			if len(parts) != 6 {
				break
			}
			for idx, counter := range parts[3:] {
				value, err := strconv.ParseFloat(strings.TrimSpace(counter), 64)
				if err != nil {
					db.log.Error("Compaction entry parsing failed", "err", err)
					return
				}
				counters[i%2][idx] += value
			}
		}
		// Update all the requested meters
		if db.compTimeMeter != nil {
			db.compTimeMeter.Mark(int64((counters[i%2][0] - counters[(i-1)%2][0]) * 1000 * 1000 * 1000))
		}
		if db.compReadMeter != nil {
			db.compReadMeter.Mark(int64((counters[i%2][1] - counters[(i-1)%2][1]) * 1024 * 1024))
		}
		if db.compWriteMeter != nil {
			db.compWriteMeter.Mark(int64((counters[i%2][2] - counters[(i-1)%2][2]) * 1024 * 1024))
		}
		// Sleep a bit, then repeat the stats collection
		select {
		case errc := <-db.quitChan:
			// Quit requesting, stop hammering the database
			errc <- nil
			return

		case <-time.After(refresh):
			// Timeout, gather a new set of stats
		}
	}
}

// TODO: remove this stuff and expose leveldb directly

func (db *LDBDatabase) NewBatch() Batch {
	return &ldbBatch{db: db.db, b: new(leveldb.Batch)}
}

type ldbBatch struct {
	db *leveldb.DB
	b  *leveldb.Batch
}

func (b *ldbBatch) Put(key, value []byte) error {
	b.b.Put(key, value)
	return nil
}

func (b *ldbBatch) Write() error {
	return b.db.Write(b.b, nil)
}

type table struct {
	db     Database
	prefix string
}

// NewTable returns a Database object that prefixes all keys with a given
// string.
func NewTable(db Database, prefix string) Database {
	return &table{
		db:     db,
		prefix: prefix,
	}
}

func (dt *table) Put(key []byte, value []byte) error {
	return dt.db.Put(append([]byte(dt.prefix), key...), value)
}

func (dt *table) Get(key []byte) ([]byte, error) {
	return dt.db.Get(append([]byte(dt.prefix), key...))
}

func (dt *table) Delete(key []byte) error {
	return dt.db.Delete(append([]byte(dt.prefix), key...))
}

func (dt *table) Close() {
	// Do nothing; don't close the underlying DB.
}

type tableBatch struct {
	batch  Batch
	prefix string
}

// NewTableBatch returns a Batch object which prefixes all keys with a given string.
func NewTableBatch(db Database, prefix string) Batch {
	return &tableBatch{db.NewBatch(), prefix}
}

func (dt *table) NewBatch() Batch {
	return &tableBatch{dt.db.NewBatch(), dt.prefix}
}

func (tb *tableBatch) Put(key, value []byte) error {
	return tb.batch.Put(append([]byte(tb.prefix), key...), value)
}

func (tb *tableBatch) Write() error {
	return tb.batch.Write()
}
