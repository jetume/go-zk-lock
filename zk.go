//the zk initialization
package DLocker

import (
	"log"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

var (
	zkConn             *zk.Conn
	hosts              []string
	timeout            time.Duration
	maxConnectAttempts = 3
)

func getZkConn() *zk.Conn {
	return zkConn
}

func reConnectZk() {
	EstablishZkConn(hosts, timeout)
}

func EstablishZkConn(_hosts []string, zkTimeOut time.Duration) error {
	var err error
	timeout = zkTimeOut
	hosts = _hosts
	attempts := 0
RECONNECT:
	zkConn = nil
	zkConn, _, err = zk.Connect(hosts, timeout)
	if err != nil {
		time.Sleep(3 * time.Second)
		log.Println("EstablishZkConn  ", err.Error())
		if attempts < maxConnectAttempts {
			attempts++
			goto RECONNECT
		}
	}
	return err
}

func CreatePath(path string) {
	getZkConn().Create(path, []byte(""), int32(0), zk.WorldACL(zk.PermAll))
}

func CloseZkConn() {
	zkConn.Close()
}
