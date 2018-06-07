package cmd

import (
	"fmt"
	"github.com/lanfang/go-lib/log"
	"github.com/lanfang/rdb"
	"github.com/lanfang/redis-analyzer/client"
	"github.com/lanfang/redis-analyzer/config"
	"github.com/lanfang/redis-analyzer/utils"
	"github.com/spf13/cobra"
	"io"
	"os"
	"regexp"
	"sort"
)

const (
	ModelKeys   = "keys"
	ModelBigkey = "bigkey"
)

//this is not work with uuid like :bb65f906-4783-4bb0-859e-2f4bca908828
//var expiryKey = regexp.MustCompile(`\d{4}(\-|\/|\.){0,1}\d{1,2}(\-|\/|\.){0,1}\d{1,2}`)
var expiryKey = regexp.MustCompile(`(201[3-9]|202[0-9])(\-|\/|\.){0,1}(0[1-9]|1[0-2]|[1-12])(\-|\/|\.){0,1}(0[1-9]|1[0-9]|2[0-9]|3[0-1])`)
var abnormalKey = regexp.MustCompile(`(\||]|\[|\(|\)|\^|\$|\\|\?|\*|\+){1,}`)

func parseRdbCmd(cmd *cobra.Command, args []string) {
	workModel := ""
	if len(args) > 0 {
		workModel = args[0]
	}
	worker := 1
	runner := NewRunner()
	if config.G_Config.RDBFile != "" {
		job := func(arg interface{}) {
			rdbFile, _ := arg.(string)
			rdbReader, err := os.Open(rdbFile)
			if err != nil {
				log.Error("open file %v, err:%+v", rdbFile, err)
			}
			defer func() {
				if rdbReader != nil {
					rdbReader.Close()
				}
			}()
			rdbRedis := &fakeRedis{
				rdbChan:    make(chan *rdbRecord, 1000),
				showDetail: false,
				dbInfo:     make(map[int]DbInfo),
				curTopType: make(map[redisType]dbResultSort),
				br:         rdbReader,
				workModel:  workModel,
			}
			outupt, err := utils.NewResultWriter(config.G_Config.OutputFile, "")
			if err != nil {
				log.Error("NewResultWriter err:%+v", err)
				return
			}
			rdbRedis.writer = outupt
			rdbRedis.Run()
		}
		runner.AddTask(Task{
			T: job, Arg: config.G_Config.RDBFile,
		})
	} else if len(config.G_Config.RedisConfig) > 0 {
		for _, node := range config.G_Config.RedisConfig {
			job := func(arg interface{}) {
				node, _ := arg.(client.RedisServer)
				rdbRedis := &fakeRedis{
					rdbChan:    make(chan *rdbRecord, 1000),
					showDetail: false,
					dbInfo:     make(map[int]DbInfo),
					curTopType: make(map[redisType]dbResultSort),
					cli:        client.New(&node),
					workModel:  workModel,
				}
				rdbFile := fmt.Sprintf("%v_%v", config.G_Config.RDBOutput, node.Address)
				dumper := NewDumper(node, rdbFile)
				if config.G_Config.RDBOutput != "" {
					//save to G_Config.RDBOutput
					dumper.Run()
					rdbReader, err := os.Open(rdbFile)
					if err != nil {
						log.Error("open file %v, err:%+v", config.G_Config.RDBOutput, err)
					}
					defer rdbReader.Close()
					rdbRedis.br = rdbReader
				} else {
					rdbRedis.br = dumper.GetReader()
				}

				outupt, err := utils.NewResultWriter(config.G_Config.OutputFile, node.Address)
				if err != nil {
					log.Error("NewResultWriter err:%+v", err)
					return
				}
				rdbRedis.writer = outupt
				rdbRedis.Run()
			}
			runner.AddTask(Task{
				T: job, Arg: node,
			})
		}
		//worker = len(config.G_Config.RedisConfig)
		worker = config.G_Config.Concurrent
	} else {
		log.Info("redis server/rdb file is empty")
	}

	runner.Start(worker)
}

type redisType int

const (
	redisString redisType = iota + 1
	redisHash
	redisSet
	redisZset
	redisList
)

func (t redisType) String() string {
	name := "unknown"
	switch t {
	case redisString:
		name = "string"
	case redisHash:
		name = "hash"
	case redisSet:
		name = "set"
	case redisZset:
		name = "zset"
	case redisList:
		name = "list"
	default:

	}
	return name
}

type eventType int

const (
	eventStart eventType = iota + 10
	eventDoing
	eventEnd
	eventSelectDb
)

func (t eventType) String() string {
	name := "unknown"
	switch t {
	case eventStart:
		name = "start"
	case eventDoing:
		name = "doing"
	case eventEnd:
		name = "end"
	default:

	}
	return name
}

type rdbRecord struct {
	db           int
	dataType     redisType
	eventType    eventType
	key          string
	field        string
	fieldSize    int
	val          string
	valSize      int
	elementCount int64
	expiry       int64
}

type element struct {
	val  string
	size int
}
type elementSort []element

func (h elementSort) Len() int           { return len(h) }
func (h elementSort) Less(i, j int) bool { return h[i].size > h[j].size }
func (h elementSort) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

type dbResult struct {
	db           int
	key          string
	dataType     redisType
	expiry       int64
	elementCount int64
	totalSize    int
	topElement   elementSort
}

func (item *dbResult) Format() string {
	return fmt.Sprintf("%v %v %v %v %v %v",
		item.db, item.key, item.dataType, item.expiry, item.elementCount, item.totalSize)
}

type dbResultSort []dbResult

func (h dbResultSort) Len() int           { return len(h) }
func (h dbResultSort) Less(i, j int) bool { return h[i].totalSize > h[j].totalSize }
func (h dbResultSort) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h dbResultSort) Format(db int) []string {
	result := make([]string, len(h)+1)
	result[0] = fmt.Sprintf("\ndatabase %v:\n", db)
	for i := range h {
		item := &h[i]
		result[i+1] = fmt.Sprintf("\t db:%v key:%v type:%v expiry:%v elementCount:%v totalSize:%v\n",
			item.db, item.key, item.dataType, item.expiry, item.elementCount, item.totalSize)
	}
	return result
}

type redisRdbResult []dbResult

var dbEnd *rdbRecord = &rdbRecord{}

type keyInfo struct {
	keys      []string
	totalSize int64
}
type fakeRedis struct {
	cli            *client.Client
	br             io.Reader
	writer         *utils.ResultWriter
	db             int
	rdbChan        chan *rdbRecord
	curDb          int
	curKey         *dbResult
	curDbTop       dbResultSort
	curAbnormalKey keyInfo
	curExpiryKey   keyInfo
	curTopType     map[redisType]dbResultSort
	curDbKeySize   int64
	dbInfo         map[int]DbInfo
	showDetail     bool
	//适用于grep cmd
	workModel string
}

type DbInfo struct {
	dbTopKey   dbResultSort
	dbTopType  map[redisType]dbResultSort
	dbKeys     int64
	abnormaKey keyInfo
	expiryKey  keyInfo
}

// StartRDB is called when parsing of a valid RDB file starts.
func (r *fakeRedis) StartRDB() {

}

// StartDatabase is called when database n starts.
// Once a database starts, another database will not start until EndDatabase is called.
func (r *fakeRedis) StartDatabase(n int, offset int) {
	r.db = n
}

// AUX field
func (r *fakeRedis) Aux(key, value []byte) {
	log.Info("Aux key:%+v, val:%+v", utils.String(key), utils.String(value))
}

// ResizeDB hint
func (r *fakeRedis) ResizeDatabase(dbSize, expiresSize uint32) {

}

// Set is called once for each string key.
func (r *fakeRedis) Set(key, value []byte, expiry int64) {
	r.rdbChan <- &rdbRecord{
		db:           r.db,
		dataType:     redisString,
		eventType:    eventDoing,
		key:          utils.String(key),
		val:          utils.String(value),
		valSize:      len(value),
		elementCount: 1,
		expiry:       expiry,
	}
}

// StartHash is called at the beginning of a hash.
// Hset will be called exactly length times before EndHash.
func (r *fakeRedis) StartHash(key []byte, length, expiry int64) {
	r.rdbChan <- &rdbRecord{
		db:           r.db,
		dataType:     redisHash,
		eventType:    eventStart,
		key:          utils.String(key),
		elementCount: length,
		expiry:       expiry,
	}
}

// Hset is called once for each field=value pair in a hash.
func (r *fakeRedis) Hset(key, field, value []byte) {
	r.rdbChan <- &rdbRecord{
		db:        r.db,
		dataType:  redisHash,
		eventType: eventDoing,
		key:       utils.String(key),
		field:     utils.String(field),
		fieldSize: len(field),
		val:       utils.String(value),
		valSize:   len(value),
	}
}

// EndHash is called when there are no more fields in a hash.
func (r *fakeRedis) EndHash(key []byte) {
	r.rdbChan <- &rdbRecord{
		db:        r.db,
		dataType:  redisHash,
		eventType: eventEnd,
		key:       utils.String(key),
	}
}

// StartSet is called at the beginning of a set.
// Sadd will be called exactly cardinality times before EndSet.
func (r *fakeRedis) StartSet(key []byte, cardinality, expiry int64) {
	r.rdbChan <- &rdbRecord{
		db:           r.db,
		dataType:     redisSet,
		eventType:    eventStart,
		key:          utils.String(key),
		elementCount: cardinality,
		expiry:       expiry,
	}
}

// Sadd is called once for each member of a set.
func (r *fakeRedis) Sadd(key, member []byte) {
	r.rdbChan <- &rdbRecord{
		db:        r.db,
		dataType:  redisSet,
		eventType: eventDoing,
		key:       utils.String(key),
		field:     utils.String(member),
		fieldSize: len(member),
	}
}

// EndSet is called when there are no more fields in a set.
func (r *fakeRedis) EndSet(key []byte) {
	r.rdbChan <- &rdbRecord{
		db:        r.db,
		dataType:  redisSet,
		eventType: eventEnd,
		key:       utils.String(key),
	}
}

// StartList is called at the beginning of a list.
// Rpush will be called exactly length times before EndList.
// If length of the list is not known, then length is -1
func (r *fakeRedis) StartList(key []byte, length, expiry int64) {
	r.rdbChan <- &rdbRecord{
		db:           r.db,
		dataType:     redisList,
		eventType:    eventStart,
		key:          utils.String(key),
		elementCount: length,
		expiry:       expiry,
	}

}

// Rpush is called once for each value in a list.
func (r *fakeRedis) Rpush(key, value []byte) {
	r.rdbChan <- &rdbRecord{
		db:        r.db,
		dataType:  redisList,
		eventType: eventDoing,
		key:       utils.String(key),
		field:     utils.String(value),
		fieldSize: len(value),
	}
}

// EndList is called when there are no more values in a list.
func (r *fakeRedis) EndList(key []byte) {
	r.rdbChan <- &rdbRecord{
		db:        r.db,
		dataType:  redisList,
		eventType: eventEnd,
		key:       utils.String(key),
	}
}

// StartZSet is called at the beginning of a sorted set.
// Zadd will be called exactly cardinality times before EndZSet.
func (r *fakeRedis) StartZSet(key []byte, cardinality, expiry int64) {
	r.rdbChan <- &rdbRecord{
		db:           r.db,
		dataType:     redisZset,
		eventType:    eventStart,
		key:          utils.String(key),
		elementCount: cardinality,
		expiry:       expiry,
	}
}

// Zadd is called once for each member of a sorted set.
func (r *fakeRedis) Zadd(key []byte, score float64, member []byte) {
	r.rdbChan <- &rdbRecord{
		db:        r.db,
		dataType:  redisZset,
		eventType: eventDoing,
		key:       utils.String(key),
		field:     utils.String(member),
		fieldSize: len(member) + 8,
	}
}

// EndZSet is called when there are no more members in a sorted set.
func (r *fakeRedis) EndZSet(key []byte) {
	r.rdbChan <- &rdbRecord{
		db:        r.db,
		dataType:  redisZset,
		eventType: eventEnd,
		key:       utils.String(key),
	}
}

// EndDatabase is called at the end of a database.
func (r *fakeRedis) EndDatabase(n int, offset int) {
	r.rdbChan <- dbEnd
}

// EndRDB is called when parsing of the RDB file is complete.
func (r *fakeRedis) EndRDB(offset int) {
	close(r.rdbChan)
}

func (r *fakeRedis) StartQuickList(key []byte, expiry int64) {
	r.rdbChan <- &rdbRecord{
		db:           r.db,
		dataType:     redisList,
		eventType:    eventStart,
		key:          utils.String(key),
		expiry:       expiry,
		elementCount: int64(-1),
	}
}
func (r *fakeRedis) EndQuickList(key []byte, length int64) {
	r.rdbChan <- &rdbRecord{
		db:           r.db,
		dataType:     redisList,
		eventType:    eventEnd,
		key:          utils.String(key),
		elementCount: length,
	}
}

type dbResultx struct {
	db           int
	key          int
	elementCount int64
	totalSize    int64
	topElement   elementSort
}

func (r *fakeRedis) reset(record *rdbRecord) {
	r.curKey = &dbResult{
		db:           record.db,
		key:          record.key,
		dataType:     record.dataType,
		elementCount: record.elementCount,
		topElement:   make(elementSort, 0),
		totalSize:    0,
		expiry:       record.expiry,
	}
}
func (r *fakeRedis) dealRDB(record *rdbRecord) {
	r.curDb = record.db
	if r.curKey == nil {
		r.curKey = &dbResult{
			db:           record.db,
			key:          record.key,
			dataType:     record.dataType,
			elementCount: record.elementCount,
			topElement:   make(elementSort, 0),
			totalSize:    0,
			expiry:       record.expiry,
		}
	}
	r.curKey.totalSize += record.fieldSize + record.valSize
	if r.showDetail {
		var eleSize int
		if record.dataType == redisHash {
			eleSize = record.valSize
		} else {
			eleSize = record.fieldSize
		}
		r.curKey.topElement = append(r.curKey.topElement, element{
			val:  record.field,
			size: eleSize,
		})
		sort.Sort(r.curKey.topElement)
		if len(r.curKey.topElement) > 10 {
			r.curKey.topElement = r.curKey.topElement[0:10]
		}
	}
	if record.dataType == redisString {
		r.doneKey(record)
	}

}

func (r *fakeRedis) doneKey(record *rdbRecord) {
	r.curDbKeySize++
	if r.curKey.elementCount == -1 {
		//quickList
		r.curKey.elementCount = record.elementCount
	}
	switch r.workModel {
	case ModelKeys:
		r.grepKey(record.key)
	case ModelBigkey:
		r.bigkey(record.key)
	default:
		r.curDbTop = append(r.curDbTop, *r.curKey)
		sort.Sort(r.curDbTop)
		if len(r.curDbTop) > config.G_Config.TopNum {
			r.curDbTop = r.curDbTop[0:config.G_Config.TopNum]
		}

		topType := r.curTopType[record.dataType]
		topType = append(topType, *r.curKey)
		sort.Sort(topType)
		if len(topType) > config.G_Config.TopNum {
			topType = topType[0:config.G_Config.TopNum]
		}
		r.curTopType[record.dataType] = topType
		if r.curKey.expiry == 0 && expiryKey.Match([]byte(record.key)) {
			if len(r.curExpiryKey.keys) < config.G_Config.TopNum {
				r.curExpiryKey.keys = append(r.curExpiryKey.keys, record.key)
			}
			r.curExpiryKey.totalSize++
		}
		if abnormalKey.Match([]byte(record.key)) {
			if len(r.curAbnormalKey.keys) < config.G_Config.TopNum {
				r.curAbnormalKey.keys = append(r.curAbnormalKey.keys, record.key)
			}
			r.curAbnormalKey.totalSize++
		}
	}
	r.curKey = nil
}

func (r *fakeRedis) showTopKey() {
	name := "database top key"
	header := []string{"database(keys)", "top key", "type", "exipry", "elementCount", "totalSize(byte)"}
	rows := [][]string{}
	for i := 0; i < 16; i++ {
		dbResult, ok := r.dbInfo[i]
		if !ok {
			rows = append(rows, []string{fmt.Sprintf("db%v:keys=%v", i, 0), " ", " ", " ", " ", " "})
			continue
		}
		for _, item := range dbResult.dbTopKey {
			rows = append(rows, []string{
				fmt.Sprintf("db%v:keys=%v", item.db, dbResult.dbKeys),
				fmt.Sprintf("%v", item.key),
				fmt.Sprintf("%v", item.dataType),
				fmt.Sprintf("%v", item.expiry),
				fmt.Sprintf("%v", item.elementCount),
				fmt.Sprintf("%v", item.totalSize),
			})
		}
	}
	r.writer.Write(name, header, rows, config.G_Config.Pretty)

}

func (r *fakeRedis) showTopType() {
	name := "database top key by type"
	header := []string{"database(keys)", "top key", "type", "exipry", "elementCount", "totalSize(byte)"}
	rows := [][]string{}
	for i := 0; i < 16; i++ {
		topType, ok := r.dbInfo[i]
		if !ok {
			rows = append(rows, []string{fmt.Sprintf("db%v:keys=%v", i, 0), " ", " ", " ", " ", " "})
			continue
		}
		for j := redisString; j <= redisList; j++ {
			topList, ok := topType.dbTopType[j]
			if !ok {
				continue
			}
			for _, item := range topList {
				rows = append(rows, []string{
					fmt.Sprintf("db%v:keys=%v", item.db, r.dbInfo[i].dbKeys),
					fmt.Sprintf("%v", item.key),
					fmt.Sprintf("%v", item.dataType),
					fmt.Sprintf("%v", item.expiry),
					fmt.Sprintf("%v", item.elementCount),
					fmt.Sprintf("%v", item.totalSize),
				})
			}
		}
	}
	r.writer.Write(name, header, rows, config.G_Config.Pretty)
}

func (r *fakeRedis) showAbnormaKey() {
	name := "database abnormal key"
	header := []string{"database(keys)", "abnormal key"}
	rows := [][]string{}
	for i := 0; i < 16; i++ {
		dbResult, ok := r.dbInfo[i]
		if !ok {
			rows = append(rows, []string{fmt.Sprintf("db%v:abnormal_keys=%v", i, 0), " "})
			continue
		}
		for _, key := range dbResult.abnormaKey.keys {
			rows = append(rows, []string{fmt.Sprintf("db%v:abnormal_keys=%v", i, dbResult.abnormaKey.totalSize), key})
		}
	}
	r.writer.Write(name, header, rows, config.G_Config.Pretty)
}

func (r *fakeRedis) showExpiryKey() {
	name := "database expiry key"
	header := []string{"database(keys)", "expiry key"}
	rows := [][]string{}
	for i := 0; i < 16; i++ {
		dbResult, ok := r.dbInfo[i]
		if !ok {
			rows = append(rows, []string{fmt.Sprintf("db%v:expiry keys=%v", i, 0), " "})
			continue
		}
		for _, key := range dbResult.expiryKey.keys {
			rows = append(rows, []string{fmt.Sprintf("db%v:expiry keys=%v", i, dbResult.expiryKey.totalSize), key})
		}
	}
	r.writer.Write(name, header, rows, config.G_Config.Pretty)
}

func (r *fakeRedis) onDbEnd() {
	r.dbInfo[r.curDb] = DbInfo{
		dbTopKey:   r.curDbTop,
		dbKeys:     r.curDbKeySize,
		dbTopType:  r.curTopType,
		expiryKey:  r.curExpiryKey,
		abnormaKey: r.curAbnormalKey,
	}
	r.curDb = r.db
	r.curKey = nil
	r.curDbTop = make(dbResultSort, 0)
	r.curTopType = make(map[redisType]dbResultSort)
	r.curAbnormalKey = keyInfo{}
	r.curExpiryKey = keyInfo{}
	r.curDbKeySize = 0
}

func (r *fakeRedis) grepKey(key string) {
	if config.G_Config.NoExpiry && r.curKey.expiry == 0 {
		if grepFilter.Match([]byte(key)) {
			r.writer.FormatWrite("db key dataType expiry elementCount totalSize(byte)", r.curKey)
		}
	}

}

func (r *fakeRedis) bigkey(key string) {
	if r.curKey.dataType == redisString {
		if r.curKey.totalSize >= config.G_Config.BigkeySize {
			r.writer.FormatWrite("db key dataType expiry elementCount totalSize(byte)", r.curKey)
		}
	} else if r.curKey.elementCount >= int64(config.G_Config.BigkeyElementNum) {
		r.writer.FormatWrite("db key dataType expiry elementCount totalSize(byte)", r.curKey)
	}

}

func (r *fakeRedis) Run() {
	defer func() {
		r.writer.Close()
	}()
	go func() {
		if err := rdb.Decode(r.br, r); err != nil {
			log.Error("redis[%v], err:%+v", r.cli.GetConf().Address, err)
			r.EndRDB(0)
		}
	}()
Out:
	for {
		select {
		case record, ok := <-r.rdbChan:
			if !ok {
				log.Info("closed")
				break Out
			}
			if record == dbEnd {
				r.onDbEnd()
				continue
			}
			switch record.eventType {
			case eventStart:
				r.reset(record)
			case eventDoing:
				r.dealRDB(record)
			case eventEnd:
				r.doneKey(record)
			}
		case s := <-stop():
			log.Info("get sinal %+v, server stop", s.String())
			break Out
		}
	}
	if r.workModel == "" {
		r.showTopKey()
		r.showTopType()
		r.showExpiryKey()
		r.showAbnormaKey()
	}
}
