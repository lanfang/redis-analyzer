package cmd

import (
	"github.com/lanfang/redis-analyzer/client"
	"github.com/lanfang/redis-analyzer/config"
	"github.com/spf13/cobra"
	"strconv"
)

var (
	cmdConfig config.ServerConfig = config.ServerConfig{}
	redisNode client.RedisServer
	nodes     []string
)

func init() {
	cobra.OnInitialize(func() {
		initConfig()
	})
	rootCmd.PersistentFlags().StringVarP(&config.G_conf_file, "conf", "c", "", "json config file")
	rootCmd.PersistentFlags().StringSliceVarP(&nodes, "node", "n", []string{}, "redis server master or slave, support multiple address --node \"add1,add2,addr1\"")
	rootCmd.PersistentFlags().BoolVarP(&redisNode.OnMaster, "on-master", "", false, "execute the comand on the master node (default false)")
	rootCmd.PersistentFlags().BoolVarP(&cmdConfig.Pretty, "pretty", "", false, "pretty output result (default false)")
	rootCmd.PersistentFlags().StringVarP(&cmdConfig.OutputFile, "output", "o", "", "the result output file (default stdout)")
	rootCmd.PersistentFlags().IntVarP(&cmdConfig.TopNum, "top-num", "t", config.DefaultTopNum, "the result top key size")
	rootCmd.PersistentFlags().IntVarP(&cmdConfig.Concurrent, "worker-num", "w", config.DefaultConcurrent, "the concurrent worker when get multiple redis server")
	rootCmd.PersistentFlags().StringVarP(&cmdConfig.LogFile, "log-file", "l", "", "log file (default stdout)")
	cmdParseRdbCmd.Flags().StringVarP(&cmdConfig.RDBFile, "rdb-file", "", "", "the rdbfile to parse")
	cmdParseRdbCmd.Flags().StringVarP(&cmdConfig.RDBOutput, "rdb-output", "", "", "save rdbfile to rdb-output file")
	cmdDump.Flags().StringVarP(&cmdConfig.RDBOutput, "rdb-output", "", "", "save rdbfile to rdb-output file")
	cmdParseAofCmd.Flags().StringVarP(&cmdConfig.AofFile, "aof-file", "", "", "the aoffile to parse")
	cmdKeys.Flags().StringVarP(&cmdConfig.Filter, "filter", "", "", "filter for grep cmd(regexp)")
	cmdKeys.Flags().BoolVarP(&cmdConfig.NoExpiry, "no-expiry", "", true, "find the key without set expiry time")
	cmdBigkey.Flags().IntVarP(&cmdConfig.BigkeySize, "bigkey-size", "", config.DefaultBigkeySize, "bigkey size KB")
	cmdBigkey.Flags().IntVarP(&cmdConfig.BigkeyElementNum, "element-num", "", config.DefaultBigkeyElementNum, "element-num")
	cmdMonitor.Flags().IntVarP(&cmdConfig.MonitorSeconds, "monitorseconds", "", config.DefaultMonitorSeconds, "how long to execute the monitor command (seconds)")
	cmdSLowLog.Flags().IntVarP(&cmdConfig.SlowlogInterval, "slogloginterval", "", config.DefaultSlowlogInterval, "the slowlog collection interval")
}

func initConfig() {
	if rootCmd.Flags().Changed("etcd-addr") || rootCmd.Flags().Changed("conf") {
		config.G_Config = config.LoadConfig()
	}
	if rootCmd.Flags().Changed("node") {
		for _, addr := range nodes {
			config.G_Config.RedisConfig = append(config.G_Config.RedisConfig, client.RedisServer{Address: addr, OnMaster: redisNode.OnMaster})
		}
	} else if len(config.G_Config.RedisConfig) == 0 {
		/*
			config.G_Config.RedisConfig = append(config.G_Config.RedisConfig, client.RedisServer{
				Address: "127.0.0.1:6379",
			})
		*/
	}
	if rootCmd.Flags().Changed("output") {
		config.G_Config.OutputFile = cmdConfig.OutputFile
	} else if config.G_Config.OutputFile == "" {
		config.G_Config.OutputFile = rootCmd.Flags().Lookup("output").DefValue
	}
	if rootCmd.Flags().Changed("top-num") {
		config.G_Config.TopNum = cmdConfig.TopNum
	} else if config.G_Config.TopNum == 0 {
		config.G_Config.TopNum, _ = strconv.Atoi(rootCmd.Flags().Lookup("top-num").DefValue)
	}
	if rootCmd.Flags().Changed("worker-num") {
		config.G_Config.Concurrent = cmdConfig.Concurrent
	} else if config.G_Config.Concurrent == 0 {
		config.G_Config.Concurrent, _ = strconv.Atoi(rootCmd.Flags().Lookup("worker-num").DefValue)
	}
	if cmdParseRdbCmd.Flags().Changed("rdb-file") {
		config.G_Config.RDBFile = cmdConfig.RDBFile
	} else if config.G_Config.RDBFile == "" {
		config.G_Config.RDBFile = cmdParseRdbCmd.Flags().Lookup("rdb-file").DefValue
	}
	if cmdParseAofCmd.Flags().Changed("aof-file") {
		config.G_Config.AofFile = cmdConfig.AofFile
	} else if config.G_Config.AofFile == "" {
		config.G_Config.AofFile = cmdParseAofCmd.Flags().Lookup("aof-file").DefValue
	}

	if cmdMonitor.Flags().Changed("monitorseconds") {
		config.G_Config.MonitorSeconds = cmdConfig.MonitorSeconds
	} else if config.G_Config.MonitorSeconds == 0 {
		config.G_Config.MonitorSeconds, _ = strconv.Atoi(cmdMonitor.Flags().Lookup("monitorseconds").DefValue)
	}
	if cmdSLowLog.Flags().Changed("slogloginterval") {
		config.G_Config.SlowlogInterval = cmdConfig.SlowlogInterval
	} else if config.G_Config.SlowlogInterval == 0 {
		config.G_Config.SlowlogInterval, _ = strconv.Atoi(cmdSLowLog.Flags().Lookup("slogloginterval").DefValue)
	}
	if rootCmd.Flags().Changed("log-file") {
		config.G_Config.LogFile = cmdConfig.LogFile
	} else if config.G_Config.LogFile == "" {
		config.G_Config.LogFile = rootCmd.Flags().Lookup("log-file").DefValue
	}

	if rootCmd.Flags().Changed("pretty") {
		config.G_Config.Pretty = cmdConfig.Pretty
	}

	if cmdParseRdbCmd.Flags().Changed("rdb-output") {
		config.G_Config.RDBOutput = cmdConfig.RDBOutput
	}
	if cmdKeys.Flags().Changed("filter") {
		config.G_Config.Filter = cmdConfig.Filter
	}
	if cmdKeys.Flags().Changed("no-expiry") {
		config.G_Config.NoExpiry = cmdConfig.NoExpiry
	} else {
		if cmdKeys.Flags().Lookup("no-expiry").DefValue == "true" {
			config.G_Config.NoExpiry = true
		} else {
			config.G_Config.NoExpiry = false
		}
	}

	if cmdDump.Flags().Changed("rdb-output") {
		config.G_Config.RDBOutput = cmdConfig.RDBOutput
	}
	if cmdBigkey.Flags().Changed("bigkey-size") {
		config.G_Config.BigkeySize = cmdConfig.BigkeySize
	} else if config.G_Config.BigkeySize == 0 {
		config.G_Config.BigkeySize, _ = strconv.Atoi(cmdBigkey.Flags().Lookup("bigkey-size").DefValue)
	}

	if cmdBigkey.Flags().Changed("element-num") {
		config.G_Config.BigkeyElementNum = cmdConfig.BigkeyElementNum
	} else if config.G_Config.BigkeyElementNum == 0 {
		config.G_Config.BigkeyElementNum, _ = strconv.Atoi(cmdBigkey.Flags().Lookup("element-num").DefValue)
	}
}
