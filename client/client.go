package client

import (
	"bufio"
	"container/list"
	"fmt"
	"log"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type RedisServer struct {
	//ip:post
	Address  string `json:"address"`
	Password string `json:"-"`
	//work on master, default slave
	OnMaster       bool `json:"on-master"`
	ConnectTimeout int  `json:"-"`
	ReadTimeout    int  `json:"-"`
	WriteTimeout   int  `json:"-"`
}

func New(conf *RedisServer) *Client {
	client := &Client{
		conf:         conf,
		newConn:      connGenerator(conf),
		conns:        list.New(),
		quit:         make(chan struct{}),
		maxIdleConns: 2,
	}
	target := conf.Address
	if !conf.OnMaster {
		if err := client.connectToSlave(); err != nil {
			log.Printf("target %+v,connect with slave %+v, err:%+v", target, conf.Address, err)
		}
	}
	client.wg.Add(1)
	go client.onCheck()
	return client
}

func connGenerator(conf *RedisServer) func() (*conn, error) {
	return func() (*conn, error) {
		if conf.ConnectTimeout == 0 {
			conf.ConnectTimeout = 3
		}
		var connect_timeout time.Duration = time.Duration(conf.ConnectTimeout) * time.Second
		var read_timeout = time.Duration(conf.ReadTimeout) * time.Second
		var write_timeout = time.Duration(conf.WriteTimeout) * time.Second
		c, err := DialTimeout("tcp", conf.Address, connect_timeout, read_timeout, write_timeout)

		if err != nil {
			return nil, err
		}
		if len(conf.Password) > 0 {
			if _, err := c.Do("AUTH", conf.Password); err != nil {
				c.Close()
				return nil, err
			}
		}

		return c, err
	}
}

type newConn func() (*conn, error)

type node struct {
	Address    string
	ReplOffset int64
}
type slaveList []node

func (h slaveList) Len() int           { return len(h) }
func (h slaveList) Less(i, j int) bool { return h[i].ReplOffset > h[j].ReplOffset }
func (h slaveList) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

type Client struct {
	sync.Mutex
	wg           sync.WaitGroup
	quit         chan struct{}
	maxIdleConns int
	conf         *RedisServer
	newConn      newConn
	conns        *list.List
	slaves       slaveList
}

func (c *Client) GetConf() *RedisServer {
	return c.conf
}

func (c *Client) Close() {
	c.Lock()
	defer c.Unlock()

	close(c.quit)
	c.wg.Wait()

	for c.conns.Len() > 0 {
		e := c.conns.Front()
		co := e.Value.(*conn)
		c.conns.Remove(e)

		co.Close()
	}
}

func (c *Client) Do(cmd string, args ...interface{}) (interface{}, error) {
	var co *conn
	var err error
	var r interface{} = nil
	for i := 0; i < 2; i++ {
		co, err = c.get()
		if err != nil {
			break
		}
		r, err = co.Do(cmd, args...)
		if err != nil {
			co.Close()
			if e, ok := err.(*net.OpError); ok && strings.Contains(e.Error(), "use of closed network connection") {
				//send to a closed connection, try again
				continue
			}
			break
		} else {
			c.put(co)
		}
		break
	}
	return r, err
}

func (c *Client) GetConn() *SingleConn {
	co, err := c.get()
	if err != nil {
		return &SingleConn{nil, c, 0, err}
	}

	return &SingleConn{co, c, 0, nil}
}

func (c *Client) get() (co *conn, err error) {
	c.Lock()
	if c.conns.Len() == 0 {
		c.Unlock()

		co, err = c.newConn()
	} else {
		e := c.conns.Front()
		co = e.Value.(*conn)
		c.conns.Remove(e)

		c.Unlock()
	}

	return
}

func (c *Client) put(newconn *conn) {
	c.Lock()
	defer c.Unlock()

	for c.conns.Len() >= c.maxIdleConns {
		// remove back
		e := c.conns.Back()
		co := e.Value.(*conn)
		c.conns.Remove(e)

		co.Close()
	}

	c.conns.PushFront(newconn)
}

func (c *Client) getIdle() *conn {
	c.Lock()
	defer c.Unlock()

	if c.conns.Len() == 0 {
		return nil
	} else {
		e := c.conns.Back()
		co := e.Value.(*conn)
		c.conns.Remove(e)
		return co
	}
}

func (c *Client) checkIdle() {
	co := c.getIdle()
	if co == nil {
		return
	}

	_, err := co.Do("PING")
	if err != nil {
		co.Close()
	} else {
		c.put(co)
	}
}

func (c *Client) onCheck() {
	t := time.NewTicker(3 * time.Second)

	defer func() {
		t.Stop()
		c.wg.Done()
	}()

	for {
		select {
		case <-t.C:
			c.checkIdle()
		case <-c.quit:
			return
		}
	}
}

func (c *Client) connectToSlave() error {
	var err error
	for loop := true; loop; loop = false {
		co, err2 := c.get()
		if err != nil {
			err = err2
			break
		}
		defer co.Close()
		if infoStr, err2 := String(co.Do("info", "replication")); err2 != nil {
			err = err2
			break
		} else {
			lines := strings.Split(infoStr, "\n")
			for _, line := range lines {
				if strings.HasPrefix(line, "role") && strings.ToLower(c.getInfoValue(line)) == "slave" {
					break
				}
				if strings.HasPrefix(line, "slave") && strings.Contains(line, "state=online") {
					if tmp := strings.Split(c.getInfoValue(line), ","); len(tmp) == 5 {
						address := fmt.Sprintf("%v:%v", strings.Trim(tmp[0], "ip="), strings.Trim(tmp[1], "port="))
						offset, _ := strconv.ParseInt(strings.Trim(tmp[0], "offset="), 10, 64)
						c.slaves = append(c.slaves, node{
							Address:    address,
							ReplOffset: offset,
						})
					}
				}
			}
			if len(c.slaves) > 0 {
				sort.Sort(c.slaves)
				target := c.conf.Address
				c.conf.Address = c.slaves[0].Address
				c.newConn = connGenerator(c.conf)
				log.Printf("switch work node from %v to %v", target, c.conf.Address)
			}
		}
	}
	return err
}
func (c *Client) getInfoValue(str string) string {
	line := strings.Trim(strings.Trim(str, "\r"), "\n")
	kv := strings.Split(line, ":")
	if len(kv) == 2 {
		return kv[1]
	}
	return line
}

type SingleConn struct {
	*conn
	c      *Client
	closed int32
	err    error
}

func (c *SingleConn) Close() {
	if atomic.LoadInt32(&c.closed) == 1 {
		return
	} else {
		atomic.StoreInt32(&c.closed, 1)
		if c.err == nil {
			c.c.put(c.conn)
		}
	}
}
func (c *SingleConn) Release() {
	c.conn.Close()
}

func (c *SingleConn) Send(cmd string, args ...interface{}) error {
	if c.err != nil {
		return c.err
	}
	return c.conn.Send(cmd, args...)
}

func (c *SingleConn) Flush() error {
	if c.err != nil {
		return c.err
	}
	return c.conn.Flush()
}
func (c *SingleConn) Receive() (interface{}, error) {
	if c.err != nil {
		return nil, c.err
	}
	return c.conn.Receive()
}

func (c *SingleConn) GetReader() *bufio.Reader {
	return c.conn.getReader()
}

func (c *SingleConn) GetWriter() *bufio.Writer {
	return c.conn.getWriter()
}
