package cmd

import (
	"encoding/json"
	"fmt"
	"github.com/lanfang/go-lib/log"
	"github.com/lanfang/redis-analyzer/config"
	"github.com/spf13/cobra"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

var (
	version string = "0.0.1"
	quit    <-chan os.Signal
)

var cmdMonitor = &cobra.Command{
	Use:   "monitor",
	Short: "A query analyzer that parses redis monitor command",
	Long: `Send monitor command to redis server, parses the monitor result
	and aggregates the top-visit, top-command, top-elapsed, top-big-key`,
	Run: monitorCmd,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		if len(config.G_Config.RedisConfig) == 0 {
			return fmt.Errorf("redis server is emtpy, use --node to provide a redis server")
		}
		pretty_cfg, _ := json.MarshalIndent(config.G_Config, "", "  ")
		log.Info("server config:\n%v", string(pretty_cfg))
		return nil
	},
}

var cmdParseRdbCmd = &cobra.Command{
	Use:   "parse-rdb",
	Short: "Parses rdb file from local or redis server directly",
	Long: `Parses rdb file from local or redis server directly,
	top-big-key, top-big-type, find-key(abnormaKey key, expiry key and any key you want)`,
	Run: parseRdbCmd,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		if len(config.G_Config.RedisConfig) == 0 && config.G_Config.RDBFile == "" {
			return fmt.Errorf("rdbfile and redis server are all emtpy, " +
				"use --node to provide a redis server or --rdb-file to provide a local file")
		}
		pretty_cfg, _ := json.MarshalIndent(config.G_Config, "", "  ")
		log.Info("server config:\n%v", string(pretty_cfg))
		return nil
	},
}

var cmdSLowLog = &cobra.Command{
	Use:   "slowlog",
	Short: "Collect redis server slowlog",
	Long:  `Collect redis server slowlog and send warning messages`,
	Run:   slowLogCmd,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		if len(config.G_Config.RedisConfig) == 0 {
			return fmt.Errorf("redis server is emtpy, use --node to provide a redis server")
		}
		pretty_cfg, _ := json.MarshalIndent(config.G_Config, "", "  ")
		log.Info("server config:\n%v", string(pretty_cfg))
		return nil
	},
}

var cmdDump = &cobra.Command{
	Use:   "dump",
	Short: "Dump rdb file from redis server",
	Long:  `Dump rdb file from redis server`,
	Run:   dumpCmd,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		if len(config.G_Config.RedisConfig) == 0 {
			return fmt.Errorf("redis server is emtpy, use --node to provide a redis server")
		}
		if config.G_Config.RDBOutput == "" {
			return fmt.Errorf("rdbouput is empty")
		}
		pretty_cfg, _ := json.MarshalIndent(config.G_Config, "", "  ")
		log.Info("server config:\n%v", string(pretty_cfg))
		return nil
	},
}

var cmdParseAofCmd = &cobra.Command{
	Use:   "parse-aof",
	Short: "Parses aof(append-only file) file from local or redis server directly",
	Long:  `Parses aof(append-only file) file from local or redis server directly`,
	Run:   parseAofCmd,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		if len(config.G_Config.RedisConfig) == 0 && config.G_Config.AofFile == "" {
			return fmt.Errorf("aof file and redis server are all emtpy, " +
				"use --node to provide a redis server or --aof-file to provide a local file")
		}
		pretty_cfg, _ := json.MarshalIndent(config.G_Config, "", "  ")
		log.Info("server config:\n%v", string(pretty_cfg))
		return nil
	},
}

var cmdKeys = &cobra.Command{
	Use:   "keys",
	Short: "Grep key through the golang regular",
	Long:  `Grep key through the golang regular, like redis keys comannd`,
	Run:   keysCmd,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		if len(config.G_Config.RedisConfig) == 0 && config.G_Config.RDBFile == "" {
			return fmt.Errorf("rdbfile and redis server are all emtpy, " +
				"use --node to provide a redis server or --rdb-file to provide a local file")
		}
		if config.G_Config.Filter == "" {
			return fmt.Errorf("filter is empty")
		}
		pretty_cfg, _ := json.MarshalIndent(config.G_Config, "", "  ")
		log.Info("server config:\n%v", string(pretty_cfg))
		return nil
	},
}

var cmdBigkey = &cobra.Command{
	Use:   "bigkey",
	Short: "Find the key over the specified size",
	Long:  `Find the key over the specified size`,
	Run:   bigkeyCmd,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		if len(config.G_Config.RedisConfig) == 0 && config.G_Config.RDBFile == "" {
			return fmt.Errorf("rdbfile and redis server are all emtpy, " +
				"use --node to provide a redis server or --rdb-file to provide a local file")
		}
		pretty_cfg, _ := json.MarshalIndent(config.G_Config, "", "  ")
		log.Info("server config:\n%v", string(pretty_cfg))
		//convert KB to byte
		config.G_Config.BigkeySize *= 1024
		return nil
	},
}

var rootCmd = &cobra.Command{
	Use:     "redis-analyzer",
	Version: "0.0.1",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		log.Gen(config.SERVERNAME, config.G_Config.LogFile)
		return nil
	},
}

var genConf = &cobra.Command{
	Use:   "gen-conf",
	Short: "Generate example json config file",
	Run: func(cmd *cobra.Command, args []string) {
		//only show config
	},
}

func init() {
	rootCmd.AddCommand(cmdMonitor, cmdParseRdbCmd, cmdParseAofCmd, cmdSLowLog, cmdDump, cmdKeys, cmdBigkey, genConf)
}

func listenSignal(signals ...os.Signal) <-chan os.Signal {
	sig := make(chan os.Signal, 1)
	if len(signals) == 0 {
		signals = append(signals, os.Kill, os.Interrupt, syscall.SIGTERM, syscall.SIGUSR2)
	}
	signal.Notify(sig, signals...)
	return sig
}
func stop() <-chan os.Signal {
	return quit
}
func Execute() {
	quit = listenSignal()
	rootCmd.Execute()
}

type empty struct {
}
type Task struct {
	T   func(interface{})
	Arg interface{}
}

type Runner struct {
	worker  int
	pending []Task
}

func NewRunner() *Runner {
	return &Runner{}
}

func (r *Runner) AddTask(t Task) {
	r.pending = append(r.pending, t)
}

func (r *Runner) Start(n int) {
	r.worker = n
	wg := sync.WaitGroup{}
	concurrent := make(chan empty, r.worker)
	for _, task := range r.pending {
		concurrent <- empty{}
		wg.Add(1)
		go func(t Task) {
			defer func() {
				wg.Done()
				<-concurrent
			}()
			t.T(t.Arg)
		}(task)
	}
	wg.Wait()
}
