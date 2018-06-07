package cmd

import (
	"fmt"
	"github.com/lanfang/go-lib/log"
	"github.com/lanfang/redis-analyzer/client"
	"github.com/lanfang/redis-analyzer/config"
	"github.com/lanfang/redis-analyzer/utils"
	"github.com/spf13/cobra"
	"sort"
	"strconv"
	"strings"
	"time"
)

var (
	MONITOR_BUFFER_SIZE int    = 1000
	MONITOR_CMD         string = "monitor"
)

func monitorCmd(cmd *cobra.Command, args []string) {
	runner := NewRunner()
	for _, node := range config.G_Config.RedisConfig {
		job := func(arg interface{}) {
			node, _ := arg.(client.RedisServer)
			outupt, err := utils.NewResultWriter(config.G_Config.OutputFile, node.Address)
			if err != nil {
				log.Error("NewResultWriter err:%+v", err)
				return
			}
			m := NewMonitor(node)
			m.writer = outupt
			m.Run()
		}
		runner.AddTask(Task{
			T: job, Arg: node,
		})
	}
	runner.Start(config.G_Config.Concurrent)
}

type monitor struct {
	cli    *client.Client
	writer *utils.ResultWriter
	//microsecond
	start       int64
	end         int64
	last        int64
	lineTotal   int64
	topVisitKey map[string]*Pair
	topCommand  map[string]*Pair
	topElapsed  SortMap
	topBigKey   SortMap
	topSize     int
}

func NewMonitor(node client.RedisServer) *monitor {
	size := config.G_Config.TopNum
	return &monitor{
		cli:         client.New(&node),
		start:       time.Now().UnixNano() / 1000,
		end:         time.Now().UnixNano() / 1000,
		topVisitKey: make(map[string]*Pair),
		topCommand:  make(map[string]*Pair),
		topElapsed:  make(SortMap, size),
		topBigKey:   make(SortMap, size),
		topSize:     size,
	}
}

type monitorLog struct {
	TimeStamp int64    `json:"timestamp"`
	Command   string   `json:"command"`
	Params    []string `json:"args"`
	Keys      []string `json:"args"`
	Db        int64    `json:"db"`
	Addr      string   `json:"addr"`
	Elapsed   int64    `json:"elapsed"`
	KeySize   int      `json:"key_size"`
}
type Pair struct {
	Key string
	Val int64
	Raw *monitorLog
}
type SortMap []Pair

func (h SortMap) Len() int           { return len(h) }
func (h SortMap) Less(i, j int) bool { return h[i].Val > h[j].Val }
func (h SortMap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h SortMap) String() string {
	tmp := make([]string, 0)
	for _, item := range h {
		tmp = append(tmp, fmt.Sprintf("\t db[%v]: %v %v", item.Raw.Db, item.Key, item.Val))
	}
	return strings.Join(tmp, "\n")
}

func (h SortMap) Sort(src map[string]*Pair, cnt int) SortMap {
	tmp := make(SortMap, len(src))
	for _, item := range src {
		tmp = append(tmp, *item)
	}
	sort.Sort(tmp)
	if len(tmp) > cnt {
		tmp = tmp[0:cnt]
	}
	return tmp
}

/*
func (h *SortMap) Push(x interface{}) {
	*h = append(*h, x.(Pair))
}

func (h *SortMap) Pop() interface{} {
	old := *h
	size := len(old)
	x := old[size-1]
	*h = old[0 : size-1]
	return x
}
*/
//1526525943.100245 [0 127.0.0.1:38447] "GET" "123"
func ParseLog(l string) *monitorLog {
	result := &monitorLog{}
	si := strings.Index(l, "[")
	ei := strings.Index(l, "]")
	timeInfo := strings.Split(l[0:si-1], ".")
	if len(timeInfo) == 2 {
		secs, _ := strconv.ParseInt(timeInfo[0], 10, 64)
		microsecs, _ := strconv.ParseInt(timeInfo[1], 10, 64)
		result.TimeStamp = secs*1000*1000 + microsecs
	}
	dbInfo := strings.Split(l[si+1:ei], " ")
	if len(dbInfo) == 2 {
		result.Db, _ = strconv.ParseInt(dbInfo[0], 10, 32)
		result.Addr = dbInfo[1]
	}
	cmdPart := strings.Split(l[ei+2:], " ")
	result.Command = strings.ToUpper(strings.Trim(cmdPart[0], "\""))
	parts := cmdPart[1:]
	result.Params = make([]string, len(parts))

	for i, p := range parts {
		result.Params[i] = strings.Trim(p, "\"")
	}
	switch result.Command {
	case "MGET":
		result.Keys = result.Params
		result.KeySize = 0
	case "MSET", "MSETNX":
		for i := range result.Params {
			if i%2 == 0 {
				result.Keys = append(result.Keys, result.Params[i])
			} else {
				result.KeySize += len(result.Params[i])
			}
		}
	default:
		//defaul one key
		if len(result.Params) > 0 {
			result.Keys = append(result.Keys, result.Params[0])
		}
		if len(result.Params) > 1 {
			result.KeySize = len(result.Params[1])
		}
	}
	return result
}

func (mh *monitor) Run() {
	defer func() {
		mh.writer.Close()
	}()
	dataChan := mh.receive(time.Duration(config.G_Config.MonitorSeconds) * time.Second)
Out:
	for {
		select {
		case line, ok := <-dataChan:
			if !ok {
				log.Info("closed")
				break Out
			}
			if line == "OK" {
				continue
			}
			monitorLog := ParseLog(line)
			monitorLog.Elapsed = monitorLog.TimeStamp - mh.last
			//log.Info("%+v, %+v", line, monitorLog)
			mh.recordTop(monitorLog)
		}
	}
	//print
	//mh.showResult()
	mh.showTopVisit()
	mh.showTopCommand()
	mh.showTopElapsed()
	mh.showTopBigKey()
}

func (mh *monitor) receive(duration time.Duration) chan string {
	ch := make(chan string, MONITOR_BUFFER_SIZE)
	go func() {
		timeout := time.After(duration)
		conn := mh.cli.GetConn()
		defer conn.Release()
		conn.Send(MONITOR_CMD)
		conn.Flush()
	Out:
		for {
			select {
			case <-timeout:
				close(ch)
				log.Info("reach the max recive time stopped")
				break Out
			case s := <-stop():
				close(ch)
				log.Info("get sinal %+v, server stop", s.String())
				break Out
			default:
				if raw, err := conn.Receive(); err == nil {
					if r, err2 := client.String(raw, err); err2 != nil {

					} else {
						ch <- r
					}
				} else {
					log.Info("Recive failed try again, err:%+v", err)
					conn.Receive()
					conn = mh.cli.GetConn()
					conn.Send(MONITOR_CMD)
					conn.Flush()
				}
			}
		}
	}()
	return ch
}

func (mh *monitor) recordTop(cmdLog *monitorLog) {
	if mh.last == 0 {
		mh.last = cmdLog.TimeStamp
	}
	mh.lineTotal++
	mh.end = time.Now().UnixNano() / 1000

	//topVisit
	for _, key := range cmdLog.Keys {
		if _, ok := mh.topVisitKey[key]; !ok {
			mh.topVisitKey[key] = &Pair{}
		}
		mh.topVisitKey[key].Key = key
		mh.topVisitKey[key].Raw = cmdLog
		mh.topVisitKey[key].Val++
	}

	//topCommand
	if _, ok := mh.topCommand[cmdLog.Command]; !ok {
		mh.topCommand[cmdLog.Command] = &Pair{}
	}
	mh.topCommand[cmdLog.Command].Key = cmdLog.Command
	mh.topCommand[cmdLog.Command].Raw = cmdLog
	mh.topCommand[cmdLog.Command].Val++

	//topElapsed
	if len(cmdLog.Keys) > 0 {
		mh.topElapsed = append(mh.topElapsed, Pair{Key: cmdLog.Keys[0], Val: (cmdLog.TimeStamp - mh.last), Raw: cmdLog})
	}
	//topBigKey
	if cmdLog.KeySize > 0 && len(cmdLog.Keys) > 0 {
		mh.topBigKey = append(mh.topBigKey, Pair{Key: cmdLog.Keys[0], Val: int64(cmdLog.KeySize), Raw: cmdLog})
	}

	sort.Sort(mh.topElapsed)
	sort.Sort(mh.topBigKey)

	if len(mh.topElapsed) > mh.topSize {
		mh.topElapsed = mh.topElapsed[0:mh.topSize]
	}
	if len(mh.topBigKey) > mh.topSize {
		mh.topBigKey = mh.topBigKey[0:mh.topSize]
	}
	mh.last = cmdLog.TimeStamp
}
func (mh *monitor) showResultx() {
	topSort := SortMap{}
	fmt.Printf("topVisitKey:\n %s\n", topSort.Sort(mh.topVisitKey, mh.topSize))
	fmt.Printf("topCommand:\n %s\n", topSort.Sort(mh.topCommand, mh.topSize))
	fmt.Printf("topElapsed(microsecond):\n %s\n", mh.topElapsed)
	fmt.Printf("topBigKey(byte):\n %s\n", mh.topBigKey)

}
func (mh *monitor) showTopVisit() {
	topVisit := SortMap{}.Sort(mh.topVisitKey, mh.topSize)
	name := "top visity key"
	header := []string{"database", "key", "count"}
	rows := [][]string{}
	for _, item := range topVisit {
		if item.Raw != nil {
			rows = append(rows, []string{fmt.Sprintf("db%v", item.Raw.Db), item.Key, fmt.Sprintf("%v", item.Val)})
		}
	}
	mh.writer.Write(name, header, rows, config.G_Config.Pretty)
}

func (mh *monitor) showTopCommand() {
	topCommand := SortMap{}.Sort(mh.topCommand, mh.topSize)
	name := "top command key"
	header := []string{"database", "command", "count"}
	rows := [][]string{}
	for _, item := range topCommand {
		if item.Raw != nil {
			rows = append(rows, []string{fmt.Sprintf("db%v", item.Raw.Db), item.Key, fmt.Sprintf("%v", item.Val)})
		}
	}
	mh.writer.Write(name, header, rows, config.G_Config.Pretty)

}

func (mh *monitor) showTopElapsed() {
	name := "top elapsed key"
	header := []string{"database", "key", "count"}
	rows := [][]string{}
	for _, item := range mh.topElapsed {
		if item.Raw != nil {
			rows = append(rows, []string{fmt.Sprintf("db%v", item.Raw.Db), item.Key, fmt.Sprintf("%v", item.Val)})
		}
	}
	mh.writer.Write(name, header, rows, config.G_Config.Pretty)
}

func (mh *monitor) showTopBigKey() {
	name := "top big key"
	header := []string{"database", "key", "count"}
	rows := [][]string{}
	for _, item := range mh.topBigKey {
		if item.Raw != nil {
			rows = append(rows, []string{fmt.Sprintf("db%v", item.Raw.Db), item.Key, fmt.Sprintf("%v", item.Val)})
		}
	}
	mh.writer.Write(name, header, rows, config.G_Config.Pretty)
}
