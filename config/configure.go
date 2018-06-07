package config

import (
	"encoding/json"
	"fmt"
	"github.com/lanfang/redis-analyzer/client"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

var G_conf_file string
var G_Config ServerConfig = ServerConfig{}

var SERVERNAME string

const (
	DefaultMonitorSeconds  = 180
	DefaultSlowlogInterval = 10
	DefaultTopNum          = 10
	DefaultConcurrent      = 16
	//KB
	DefaultBigkeySize       = 30
	DefaultBigkeyElementNum = 500000
)

func init() {
	if i := strings.LastIndex(os.Args[0], "/"); i >= 0 {
		i++
		SERVERNAME = os.Args[0][i:]
	}
}

type AlertConfig struct {
	Host         string
	Token        string
	DepartmentId []string
}

type ServerConfig struct {
	//redis node config
	RedisConfig []client.RedisServer `json:"nodes"`
	//monitor seconds
	MonitorSeconds int `json:"monitorseconds"`
	//sloglog collection frequency
	SlowlogInterval int `json:"slogloginterval"`
	//rdb file path
	RDBFile string `json:"rdb-file"`
	//save rdb file to RDBOutput
	RDBOutput string `json:"rdb-output"`
	//aof file path
	AofFile string `json:"rdb-file"`
	//result to write
	OutputFile string `json:"output"`
	//top key size
	TopNum int `json:"top-num"`
	//big key size
	BigkeySize int `json:"bigkey-size"`
	//big key element count
	BigkeyElementNum int `json:"element-num"`
	//filter for grep cmd
	Filter     string `json:"filter"`
	Concurrent int    `json:"worker-num"`
	LogFile    string `json:"log-file"`
	Pretty     bool   `json:"pretty"`
	NoExpiry   bool   `json:"no-expiry"`
}

func getConfigFromFile(config_file string, config *ServerConfig) error {
	fmt.Println("config file:" + config_file)
	file, err := os.Open(config_file)
	if err != nil {
		return err
	}
	defer file.Close()

	config_str, err := ioutil.ReadAll(file)
	if err != nil {
		return err
	}
	err = json.Unmarshal(config_str, config)
	if err != nil {
		fmt.Println("parase config error for:" + err.Error())
	}
	return err
}

func LoadConfig() ServerConfig {
	tmpConf := ServerConfig{}
	if G_conf_file != "" {
		if err := getConfigFromFile(G_conf_file, &tmpConf); err != nil {
			log.Printf("get config from file %v, err:%+v, exit...", G_conf_file, err)
			os.Exit(-1)
		}
	}
	return tmpConf
}
