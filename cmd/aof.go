package cmd

import (
	"bufio"
	"fmt"
	"github.com/lanfang/go-lib/log"
	"github.com/lanfang/redis-analyzer/client"
	"github.com/lanfang/redis-analyzer/config"
	"github.com/lanfang/redis-analyzer/utils"
	"github.com/spf13/cobra"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

func parseAofCmd(cmd *cobra.Command, args []string) {
	worker := 1
	runner := NewRunner()
	if config.G_Config.AofFile != "" {
		job := func(arg interface{}) {
			file, _ := arg.(string)
			if aofReader, err := os.Open(file); err == nil {
				defer aofReader.Close()
				outupt, err := utils.NewResultWriter(config.G_Config.OutputFile, "")
				if err != nil {
					log.Error("NewResultWriter err:%+v", err)
					return
				}
				aofParser := &aofParser{br: bufio.NewReaderSize(aofReader, ReaderBufferSize), writer: outupt}
				aofParser.Run()
			} else {
				log.Error("open file %v, err:%v", file, err)
			}
		}
		runner.AddTask(Task{
			T: job, Arg: config.G_Config.AofFile,
		})
	} else if len(config.G_Config.RedisConfig) > 0 {
		for _, node := range config.G_Config.RedisConfig {
			job := func(arg interface{}) {
				node, _ := arg.(client.RedisServer)
				aofParser := &aofParser{
					cli: client.New(&node),
				}
				aofParser.RunEx()
			}
			runner.AddTask(Task{
				T: job, Arg: node,
			})
		}
		//worker = len(config.G_Config.RedisConfig)
		worker = config.G_Config.Concurrent
	}

	runner.Start(worker)
}

type Error string

func (err Error) Error() string { return string(err) }

type aofCommand struct {
	Db         int
	Command    string
	SubCommand string
	Keys       []string
	Params     []string
	Raw        string
}

type aofParser struct {
	cli     *client.Client
	writer  *utils.ResultWriter
	br      *bufio.Reader
	curDb   int
	offset  uint64
	curSize int64
	conn    *client.SingleConn
}

func (h *aofParser) getReplInfo() (runId string, offset int64, err error) {
	conn := h.cli.GetConn()
	defer conn.Release()
	for loop := true; loop; loop = false {
		if infoStr, err2 := client.String(conn.Do("info")); err2 != nil {
			err = err2
			break
		} else {
			lines := strings.Split(infoStr, "\n")
			for _, line := range lines {
				if strings.HasPrefix(line, "master_repl_offset") {
					tmp := strings.Trim(strings.Trim(line, "\r"), "\n")
					offset, _ = strconv.ParseInt(strings.Trim(tmp, "master_repl_offset:"), 10, 64)
					continue
				}
				if strings.HasPrefix(line, "run_id") {
					tmp := strings.Trim(strings.Trim(line, "\r"), "\n")
					runId = strings.Trim(tmp, "run_id:")
					continue
				}
			}
		}
	}
	return
}

func (h *aofParser) RunEx() {
	conn := h.cli.GetConn()
	defer conn.Release()
	runid, offset, err := h.getReplInfo()
	if err != nil {
		log.Error("get repl info err:%+v", err)
	}
	repInfo, err := client.String(conn.Do("PSYNC", runid, offset))
	if strings.HasPrefix(repInfo, "FULLRESYNC") {
		log.Info("master replied with fullresync, wo do not want to do this, exit....")
		return
	}
	if strings.HasPrefix(repInfo, "FULLRESYNC") {
		log.Info("successful partial resynchronization with master")
	}
	h.offset = uint64(offset)
	log.Info("master <-> slave sync: master accepted a partial resynchronization")
	h.br = conn.GetReader()
	h.conn = conn
	h.Run()
}

func (h *aofParser) Run() {
	defer func() {
		h.writer.Close()
	}()
	ticker := time.NewTicker(1 * time.Second)
Out:
	for {
		select {
		case s := <-stop():
			log.Info("get sinal %+v, server stop", s.String())
			break Out
		case <-ticker.C:
			h.conn.Do("REPLCONF", "ACK", h.offset)
		default:
		}
		if command, err := h.readCommand(); err == nil {
			//_ = command
			log.Info("get command: %+v", command.Raw)
		} else if err == io.EOF {
			break Out
		} else {
			log.Error("get command err:%+v", err)
			break Out
		}
	}
}
func (h *aofParser) readCommandArgs() (string, error) {
	arg := ""
	line, err := h.readLine()
	if err != nil {
		return arg, err
	}
	if len(line) == 0 {
		return arg, protocolError("short response line")
	}
	if line[0] == '$' {
		n, err := parseLen(line[1:])
		if n < 0 || err != nil {
			return arg, err
		}
		p := make([]byte, n)
		_, err = io.ReadFull(h.br, p)
		if err != nil {
			return arg, err
		}
		if line, err := h.readLine(); err != nil {
			return arg, err
		} else if len(line) != 0 {
			return arg, protocolError("bad bulk string format")
		}
		arg = utils.String(p)
	}
	return arg, nil
}

func (h *aofParser) readCommand() (*aofCommand, error) {
	h.curSize = 0
	line, err := h.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, protocolError("short response line")
	}
	if line[0] == '*' {
		n, err := parseLen(line[1:])
		if n < 0 || err != nil {
			return nil, err
		}
		r := make([]string, n)
		for i := range r {
			r[i], err = h.readCommandArgs()
			if err != nil {
				return nil, err
			}
		}
		return h.genCommand(r), nil
	} else {
		log.Error("err aof format:%+v", utils.String(line))
	}
	return nil, protocolError("unexpected response line")
}

func (h *aofParser) genCommand(args []string) *aofCommand {
	if len(args) == 0 {
		return nil
	}
	command := &aofCommand{Command: strings.ToUpper(args[0]), Raw: strings.Join(args, " ")}
	args = args[1:]
	if len(args) > 0 {
		switch command.Command {
		case "BITOP":
			if len(args) > 0 {
				command.SubCommand = args[0]
				command.Keys = args[1:]
			}
		case "FLUSHALL", "FLUSHDB":
			break
		case "SELECT":
			h.curDb, _ = strconv.Atoi(args[0])
			command.Params = append(command.Params, args[0])
		case "MSET", "MSETNX":
			for index, item := range args {
				if index%2 == 0 {
					command.Keys = append(command.Keys, item)
				} else {
					command.Params = append(command.Params, item)
				}
			}
		default:
			command.Keys = append(command.Keys, args[0])
			for i := 1; i < len(args); i++ {
				command.Params = append(command.Params, args[i])
			}

		}
	}
	command.Db = h.curDb
	return command
}
func (h *aofParser) readLine() ([]byte, error) {
	for {
		p, err := h.br.ReadSlice('\n')
		if err == bufio.ErrBufferFull {
			return nil, protocolError("long response line")
		}
		if err != nil {
			return nil, err
		}
		if p[0] == '\n' {
			continue
		}
		h.offset += uint64(len(p))
		i := len(p) - 2
		if i < 0 || p[i] != '\r' {
			return nil, protocolError("bad response line terminator")
		}
		return p[:i], nil
	}
	return []byte{}, nil
}

type protocolError string

func (pe protocolError) Error() string {
	return fmt.Sprintf("redigo: %s (possible server error or unsupported concurrent read by application)", string(pe))
}

// parseLen parses bulk string and array lengths.
func parseLen(p []byte) (int, error) {
	if len(p) == 0 {
		return -1, protocolError("malformed length")
	}

	if p[0] == '-' && len(p) == 2 && p[1] == '1' {
		// handle $-1 and $-1 null replies.
		return -1, nil
	}

	var n int
	for _, b := range p {
		n *= 10
		if b < '0' || b > '9' {
			return -1, protocolError("illegal bytes in length")
		}
		n += int(b - '0')
	}

	return n, nil
}
