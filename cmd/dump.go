package cmd

import (
	"bufio"
	"fmt"
	"github.com/lanfang/go-lib/log"
	"github.com/lanfang/redis-analyzer/client"
	"github.com/lanfang/redis-analyzer/config"
	"github.com/spf13/cobra"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	rdbFlagEOF       = 0xff
	ReaderBufferSize = 1024 * 128
)

func dumpCmd(cmd *cobra.Command, args []string) {
	runner := NewRunner()
	for _, node := range config.G_Config.RedisConfig {
		job := func(arg interface{}) {
			node, _ := arg.(client.RedisServer)
			dump := NewDumper(node, fmt.Sprintf("%v_%v", config.G_Config.RDBOutput, node.Address))
			dump.Run()
		}
		runner.AddTask(Task{
			T: job, Arg: node,
		})
	}
	runner.Start(config.G_Config.Concurrent)
}

func NewDumper(node client.RedisServer, file string) *dumper {
	return &dumper{
		cli:     client.New(&node),
		rdbFile: file,
	}
}

type rdbInfo struct {
	sync    string
	runid   string
	offset  int64
	rdbSize int64
}
type dumper struct {
	cli     *client.Client
	rdbFile string
	rdb     rdbInfo
}

func (d *dumper) Run() {
	conn := d.cli.GetConn()
	defer conn.Release()
	var err error
	go func() {
		select {
		case <-stop():
			os.Exit(-1)
		}
	}()
	d.rdb, err = d.getRDBInfo(conn)
	writer, err := os.OpenFile(d.rdbFile, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
	defer writer.Close()
	if err != nil {
		log.Error("open file %+v, err:%+v", d.rdbFile, err)
		return
	}
	dst := bufio.NewWriterSize(writer, ReaderBufferSize)
	n, err := io.CopyN(dst, conn.GetReader(), d.rdb.rdbSize)
	dst.Flush()
	if err != nil {
		log.Error("CopyN err:copy size:%+v, n:%+v", n, err)
	}

}

func (d *dumper) GetReader() *bufio.Reader {
	conn := d.cli.GetConn()
	d.getRDBInfo(conn)
	//go d.ping(conn, rdbInfo)
	return conn.GetReader()
}

func (d *dumper) ping(conn *client.SingleConn, rdbInfo rdbInfo) {
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case s := <-stop():
			log.Info("get sinal %+v, server stop", s.String())
			break
		case <-ticker.C:
			conn.Do("REPLCONF", "ACK", rdbInfo.rdbSize)
		default:
		}
	}
}
func (d *dumper) getRDBInfo(conn *client.SingleConn) (rdbInfo, error) {
	//conn := d.cli.GetConn()
	rdb := rdbInfo{}
	log.Info("dump info start")
	str, err := client.String(conn.Do("PSYNC", "?", "-1"))
	info := strings.Split(str, " ")
	if err == nil && len(info) == 3 {
		rdb.offset, _ = strconv.ParseInt(info[2], 10, 64)
		rdb.sync = info[0]
		rdb.runid = info[1]
	}
	for {
		var p []byte
		p, err = conn.GetReader().ReadBytes('\n')
		if err != nil && err != bufio.ErrBufferFull {
			break
		}
		if p[0] == '\n' {
			continue
		}
		i := len(p) - 2
		if i < 0 || p[i] != '\r' || p[0] != '$' {
			err = fmt.Errorf("bad response line terminator")
			break
		}
		rdb.rdbSize, err = d.parseLen(p[1:i])
		break
	}
	log.Info("dump info %+v", rdb)
	return rdb, err
}

func (d *dumper) parseLen(p []byte) (int64, error) {
	if len(p) == 0 {
		return -1, fmt.Errorf("malformed length")
	}

	if p[0] == '-' && len(p) == 2 && p[1] == '1' {
		// handle $-1 and $-1 null replies.
		return -1, nil
	}

	var n int64
	for _, b := range p {
		n *= 10
		if b < '0' || b > '9' {
			return -1, fmt.Errorf("illegal bytes in length")
		}
		n += int64(b - '0')
	}
	return n, nil
}
