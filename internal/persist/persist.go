// Package persist mirrors a cluster.Persister to disk so raft state and
// snapshots survive a container restart. Persister only keeps state in
// memory, so a background poller copies it out periodically; a node also
// flushes once on SIGTERM/SIGINT for a clean shutdown.
package persist

import (
	"bytes"
	"os"
	"path/filepath"
	"time"

	"kvapp/internal/cluster"
)

const (
	raftFile = "raftstate.bin"
	snapFile = "snapshot.bin"
)

// Load reads a previously saved raft state and snapshot from dir, if any.
func Load(dir string) (raftstate, snapshot []byte) {
	raftstate, _ = os.ReadFile(filepath.Join(dir, raftFile))
	snapshot, _ = os.ReadFile(filepath.Join(dir, snapFile))
	return
}

func writeAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// Flush writes the persister's current raft state and snapshot to dir,
// unconditionally.
func Flush(dir string, persister *cluster.Persister) error {
	if err := writeAtomic(filepath.Join(dir, raftFile), persister.ReadRaftState()); err != nil {
		return err
	}
	return writeAtomic(filepath.Join(dir, snapFile), persister.ReadSnapshot())
}

// Watch polls persister and writes to dir whenever the raft state or
// snapshot changes, until done is closed.
func Watch(dir string, persister *cluster.Persister, interval time.Duration, done <-chan struct{}) {
	lastRaft, lastSnap := Load(dir)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-done:
			return
		case <-t.C:
			rs := persister.ReadRaftState()
			if !bytes.Equal(rs, lastRaft) {
				if writeAtomic(filepath.Join(dir, raftFile), rs) == nil {
					lastRaft = rs
				}
			}
			ss := persister.ReadSnapshot()
			if !bytes.Equal(ss, lastSnap) {
				if writeAtomic(filepath.Join(dir, snapFile), ss) == nil {
					lastSnap = ss
				}
			}
		}
	}
}
