package cmd

import (
	"fmt"
	"github.com/lanfang/go-lib/log"
	"github.com/lanfang/redis-analyzer/client"
	"github.com/lanfang/redis-analyzer/config"
	"github.com/lanfang/redis-analyzer/utils"
	"github.com/spf13/cobra"
	"strings"
	"time"
)

func slowLogCmd(cmd *cobra.Command, args []string) {
	runner := Runner{}
	for _, node := range config.G_Config.RedisConfig {
		job := func(arg interface{}) {
			node, _ := arg.(client.RedisServer)
			sl := NewSlowlog(node)
			outupt, err := utils.NewResultWriter(config.G_Config.OutputFile, node.Address)
			if err != nil {
				log.Error("NewResultWriter err:%+v", err)
				return
			}
			sl.writer = outupt
			sl.Run()
		}
		runner.AddTask(Task{
			T: job, Arg: node,
		})
	}
	runner.Start(config.G_Config.Concurrent)
}

type slowLog struct {
	cli      *client.Client
	size     int
	latestId int64
	writer   *utils.ResultWriter
}

func NewSlowlog(conf client.RedisServer) *slowLog {
	return &slowLog{
		cli:  client.New(&conf),
		size: config.G_Config.TopNum,
	}
}

type slowLogLine struct {
	Id        int64
	Timestamp int64
	Duration  int64
	Command   []string
}

func (sl *slowLog) Run() {
	defer func() {
		sl.cli.Close()
		sl.writer.Close()
	}()
	dataChan := sl.receive()
	sl.deal(dataChan)
}
func (sl *slowLog) deal(ch <-chan []interface{}) {
Out:
	for {
		select {
		case line, ok := <-ch:
			if !ok {
				log.Info("closed")
				break Out
			}
			logs, _ := sl.parse(line)
			valid_logs := sl.latestLogs(logs)
			//log.Info("cur valid_logs logs:%+v", valid_logs)
			if len(valid_logs) > 0 {
				sl.showSlowlog(valid_logs)
			}
		}
	}
}

func (sl *slowLog) showSlowlog(l []slowLogLine) {
	name := "slow log"
	header := []string{"id", "timestamp", "elapsed", "command"}
	rows := [][]string{}
	for _, row := range l {
		rows = append(rows, []string{
			fmt.Sprintf("%v", row.Id),
			fmt.Sprintf("%v", row.Timestamp),
			fmt.Sprintf("%v", row.Duration),
			fmt.Sprintf("%v", strings.Join(row.Command, " ")),
		})
	}
	sl.writer.Write(name, header, rows, config.G_Config.Pretty)
}

func (sl *slowLog) parse(rawData []interface{}) ([]slowLogLine, error) {
	result := make([]slowLogLine, 0)
	for _, info := range rawData {
		if data, ok := info.([]interface{}); ok {
			line := slowLogLine{}
			if _, err := client.Scan(data, &line.Id, &line.Timestamp, &line.Duration, &line.Command); err == nil {
				result = append(result, line)
			} else {
				log.Error("parse log [%+v], err:%+v", line, err)
				continue
			}
		} else {
			log.Error("parseSlowlog err raw data format error")
			continue
		}

	}
	return result, nil
}

func (sl *slowLog) latestLogs(curLogs []slowLogLine) []slowLogLine {
	newLogs := make([]slowLogLine, 0)
	for index := len(curLogs) - 1; index >= 0; index-- {
		if curLogs[index].Id > sl.latestId {
			newLogs = append(newLogs, curLogs[index])
			sl.latestId = curLogs[index].Id
		}
	}
	return newLogs
}

func (sl *slowLog) receive() chan []interface{} {
	ch := make(chan []interface{}, 5)
	go func() {
		timer := time.Tick(time.Duration(config.G_Config.SlowlogInterval) * time.Second)
	Out:
		for {
			select {
			case s := <-stop():
				close(ch)
				log.Info("get sinal %+v, server stop", s.String())
				break Out
			case <-timer:
				if raw, err := sl.cli.Do("slowlog", "get", 11); err == nil {
					if r, err2 := client.Values(raw, err); err2 != nil {

					} else {
						ch <- r
					}
				} else {
					log.Error("Recive failed try again, err:%+v", err)
					sl.latestId = 0
				}
			}
		}
	}()
	return ch
}
