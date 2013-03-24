package zookeeper

import (
	"github.com/dforsyth/sprinkles/ds"
	"launchpad.net/gozk/zookeeper"
	"path"
	"log"
)


type ZKMap struct {
	_m          *ds.InterfaceMap
	path        string
	zk          *zookeeper.Conn
	onChange    func(*ZKMap)
	deserialize func(string) interface{}
}

func (m *ZKMap) Get(key string) interface{} {
	return m._m.Get(key)
}

func (m *ZKMap) Contains(key string) bool {
	return m._m.Contains(key)
}

func (m *ZKMap) Keys() []string {
	return m._m.Keys()
}

func NewZKMap(zk *zookeeper.Conn, path string, deserialize func(string) interface{}, onChange func(*ZKMap)) (*ZKMap, error) {
	m := &ZKMap{
		_m:          ds.NewInterfaceMap(nil),
		path:        path,
		zk:          zk,
		onChange:    onChange,
		deserialize: deserialize,
	}
	if err := m.watchChildren(); err != nil {
		return nil, err
	}
	return m, nil
}

// XXX this should return an error channel
func (m *ZKMap) watchChildren() error {
	children, _, watch, err := m.zk.ChildrenW(m.path)
	if err != nil {
		return err
	}

	go func() {
		event := <-watch
		switch event.Type {
		case zookeeper.EVENT_CHILD:
			if err := m.watchChildren(); err != nil {
				// XXX log this and try again
				panic("failed to watch children")
			}
		default:
			panic(event.String())
		}
	}()

	m.updateEntries(ds.NewStringSet(children))
	return nil
}

func (m *ZKMap) updateEntries(children *ds.StringSet) {
	keys := ds.NewStringSet(m._m.Keys())
	added := children.Difference(keys)
	removed := keys.Difference(children)
	for _, k := range added {
		// TODO pull this all out into its own method
		p := path.Join(m.path, k)
		d, _, err := m.zk.Get(path.Join(p))
		if err != nil {
			log.Println(err)
			continue
		}

		// unmashal and stuff it into the safemap
		m._m.Put(k, m.deserialize(d))
	}
	for _, k := range removed {
		m._m.Delete(k)
	}
	if len(added) > 0 || len(removed) > 0 {
		m.onChange(m)
	}
}
