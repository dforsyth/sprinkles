package zookeeper

import (
	"errors"
	"fmt"
	"path"
	"sync/atomic"
)

const (
	OutsideBarrier = iota
	InsideBarrier
	DestroyedBarrier
)

// A very simple (double) barrier implementation
type Barrier struct {
	zk          *ZooKeeper
	barrierPath string
	barrierName string
	entryName   string
	entryPath   string
	size        int
	state       int32
	cancel      chan struct{}
}

// Create a barrier
func (zk *ZooKeeper) CreateBarrier(barrierName, entryName string, size int) (*Barrier, error) {
	barrierPath := path.Join("/", barrierName)
	zk.Create(barrierPath, "")

	return &Barrier{
		zk:          zk,
		barrierPath: barrierPath,
		entryName:   entryName,
		barrierName: barrierName,
		entryPath:   path.Join(barrierPath, entryName),
		size:        size,
		state:       OutsideBarrier,
	}, nil
}

func (b *Barrier) Cancel() {
	select {
	case b.cancel <- struct{}{}:
	default:
	}
	// TODO cleanup and set state
}

// Barrier teardown
func (b *Barrier) Destroy() {
	b.Cancel()
	b.zk.Conn.Delete(b.barrierPath, -1)
	atomic.StoreInt32(&b.state, DestroyedBarrier)
}

// XXX The watches in these methods do not currently monitor the events they recieve.
// Errors will not be reported from events.

// XXX It might be nice to have this return a BarrierEntry type, and then call
// Leave from that...
func (b *Barrier) Enter() error {
	if atomic.LoadInt32(&b.state) != OutsideBarrier {
		return errors.New("State is not OutsideBarrier")
	}

	if err := b.zk.Create(b.entryPath, ""); err != nil {
		return errors.New(fmt.Sprintf("Could not enter barrier: %s", err))
	}

	if !atomic.CompareAndSwapInt32(&b.state, OutsideBarrier, InsideBarrier) {
		return errors.New("Could not move state from OutsideBarrier to InsideBarrier")
	}

	// XXX I believe the correct pattern is to wait on a ready node to appear within barrier... but i did not do that.
	for {
		children, _, watch, err := b.zk.Conn.ChildrenW(b.barrierPath)
		if err != nil {
			return err
		}
		if len(children) >= b.size {
			return nil
		}

		select {
		case <-watch:
		case <-b.cancel:
			return errors.New("Got cancel")
		}
	}

	return nil
}

func (b *Barrier) Leave() error {
	// TODO: delete a specific version
	b.zk.Conn.Delete(b.entryPath, -1)
	for {
		children, _, watch, err := b.zk.Conn.ChildrenW(b.barrierPath)
		if err != nil {
			return err
		}
		if len(children) == 0 {
			return nil
		}

		select {
		case <-watch:
		case <-b.cancel:
			return errors.New("Got cancel")
		}
	}
}
