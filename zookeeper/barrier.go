package zookeeper

import (
	"errors"
	"fmt"
	"log"
	"path"
	"sort"
	"sync/atomic"
)

const (
	OutsideBarrier = iota
	EnteringBarrier
	InsideBarrier
	LeavingBarrier
	DestroyedBarrier
)

// A very simple (double) barrier implementation
type Barrier struct {
	zk               *ZooKeeper
	barrierName      string
	barrierPath      string
	barrierReadyPath string
	entryName        string
	entryPath        string
	size             int
	state            int32
	cancel           chan struct{}
}

// Create a barrier
func (zk *ZooKeeper) CreateBarrier(barrierName, entryName string, size int) (*Barrier, error) {
	barrierPath := path.Join("/", barrierName)
	zk.Create(barrierPath, "")

	if size <= 0 {
		return nil, errors.New("size must be > 0")
	}

	return &Barrier{
		zk:               zk,
		barrierName:      barrierName,
		barrierPath:      barrierPath,
		barrierReadyPath: path.Join(barrierPath, "READY"),
		entryName:        entryName,
		entryPath:        path.Join(barrierPath, entryName),
		size:             size,
		state:            OutsideBarrier,
		cancel:           make(chan struct{}, 1),
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

	if err := b.zk.CreateEphemeral(b.entryPath, ""); err != nil {
		return errors.New(fmt.Sprintf("Could not enter barrier: %s", err))
	}

	if !atomic.CompareAndSwapInt32(&b.state, OutsideBarrier, EnteringBarrier) {
		return errors.New("Could not move state from OutsideBarrier to EnteringBarrier")
	}

	_, watch, watchErr := b.zk.Conn.ExistsW(b.barrierReadyPath)
	if watchErr != nil {
		return watchErr
	}

	children, _, childrenErr := b.zk.Conn.Children(b.barrierPath)
	if childrenErr != nil {
		return childrenErr
	}

	if len(children) < b.size {
		select {
		case <-watch:
			// TODO: make sure this is a created event
		case <-b.cancel:
			return errors.New("Got cancel")
		}
	} else if err := b.zk.CreateEphemeral(b.barrierReadyPath, ""); err != nil {
		return err
	}

	if !atomic.CompareAndSwapInt32(&b.state, EnteringBarrier, InsideBarrier) {
		return errors.New("Could not move state from EnteringBarrier to InsideBarrier")
	}
	return nil
}

func (b *Barrier) Leave() error {
	if !atomic.CompareAndSwapInt32(&b.state, InsideBarrier, LeavingBarrier) {
		return errors.New("Could not move state from InsideBarrier to LeavingBarrier")
	}
	b.zk.Conn.Delete(b.barrierReadyPath, -1)
	for {
		children, _, err := b.zk.Conn.Children(b.barrierPath)
		if len(children) == 0 {
			break
		}

		if len(children) == 1 && children[0] == b.entryName {
			b.zk.Conn.Delete(b.entryPath, -1)
			break
		}

		sort.Strings(children)

		var waitPath string
		if children[0] == b.entryName {
			waitPath = path.Join(b.barrierPath, children[len(children)-1])
		} else {
			b.zk.Conn.Delete(b.entryPath, -1)
			waitPath = path.Join(b.barrierPath, children[0])
		}

		_, watch, err := b.zk.Conn.ExistsW(waitPath)
		if err != nil {
			log.Println(err)
			continue
		}

		select {
		case <-watch:
			// TODO make sure this is a deleted event
			continue
		case <-b.cancel:
			return errors.New("Got cancel")
		}
	}

	if !atomic.CompareAndSwapInt32(&b.state, LeavingBarrier, OutsideBarrier) {
		return errors.New("Could not move state from LeavingBarrier to OutsideBarrier")
	}
	return nil
}
