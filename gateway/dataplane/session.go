package dataplane

import (
	"sync"
	"time"

	"github.com/nosway/namrbd/protocol/wirev2"
)

// Session holds wire v2 authenticated session state (Phase C3).
type Session struct {
	ID           uint64
	VolumeID     uint64
	AttachmentID string
	Generation   uint64
	HostID       string
	DeviceID     uint32
	PathID       uint32
	ExpiresAt    time.Time
	SessionKey   []byte
	Seq          wirev2.SeqChecker
}

// SessionTable stores active wire v2 sessions by session ID.
type SessionTable struct {
	mu     sync.RWMutex
	byID   map[uint64]*Session
	nextID uint64
}

// NewSessionTable creates an empty session table.
func NewSessionTable() *SessionTable {
	return &SessionTable{byID: make(map[uint64]*Session), nextID: 1}
}

// Add stores a new session and returns it. Caller must not modify the session after Add.
func (t *SessionTable) Add(s *Session) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for t.byID[t.nextID] != nil {
		t.nextID++
	}
	s.ID = t.nextID
	t.byID[s.ID] = s
	t.nextID++
}

// Get returns the session by ID or nil if not found or expired.
func (t *SessionTable) Get(id uint64) *Session {
	t.mu.RLock()
	s := t.byID[id]
	t.mu.RUnlock()
	if s == nil {
		return nil
	}
	if time.Now().UTC().After(s.ExpiresAt) {
		t.Remove(id)
		return nil
	}
	return s
}

// Remove removes a session by ID.
func (t *SessionTable) Remove(id uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.byID, id)
}

// RevokeByVolume removes all sessions for the given volume ID (e.g. on detach or generation bump).
func (t *SessionTable) RevokeByVolume(volumeID uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for id, s := range t.byID {
		if s.VolumeID == volumeID {
			delete(t.byID, id)
		}
	}
}
