package zookeeper

import (
	"launchpad.net/gozk/zookeeper"
)

type ZooKeeper struct {
	Conn *zookeeper.Conn
}

func NewZooKeeper(zk *zookeeper.Conn) *ZooKeeper {
	return &ZooKeeper{zk}
}

func (zk *ZooKeeper) Close() {
	zk.Close()
}

func (zk *ZooKeeper) Create(p, v string) error {
	if _, err := zk.Conn.Create(p, v, 0, zookeeper.WorldACL(zookeeper.PERM_ALL)); err != nil {
		return err
	}
	return nil
}

func (zk *ZooKeeper) CreateEphemeral(p, v string) error {
	if _, err := zk.Conn.Create(p, v, zookeeper.EPHEMERAL, zookeeper.WorldACL(zookeeper.PERM_ALL)); err != nil {
		return err
	}
	return nil
}

func (zk *ZooKeeper) Delete(p string) error {
	return zk.Conn.Delete(p, -1)
}

func (zk *ZooKeeper) Get(p string) (string, error) {
	data, _, err := zk.Conn.Get(p)
	return data, err
}

func (zk *ZooKeeper) WatchNode(p string, onChange func(string)) error {
	panic("not yet implemented")
}
