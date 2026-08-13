package dataplane

import (
	"testing"
	"time"

	"github.com/nosway/namrbd/protocol/wirev2"
)

func TestSessionTableAddGetRemove(t *testing.T) {
	tbl := NewSessionTable()
	s := &Session{
		VolumeID:     101,
		AttachmentID: "att-00000065-0001",
		Generation:   1,
		ExpiresAt:    time.Now().UTC().Add(5 * time.Minute),
		SessionKey:   []byte("key-32-bytes-long!!!!!!!!!!!!!!!!"),
	}
	tbl.Add(s)
	if s.ID == 0 {
		t.Fatal("Add should set session ID")
	}
	got := tbl.Get(s.ID)
	if got == nil || got.VolumeID != 101 || got.AttachmentID != "att-00000065-0001" {
		t.Fatalf("Get: %+v", got)
	}
	tbl.Remove(s.ID)
	if tbl.Get(s.ID) != nil {
		t.Fatal("Get after Remove should return nil")
	}
}

func TestSessionTableRevokeByVolume(t *testing.T) {
	tbl := NewSessionTable()
	for _, vol := range []uint64{101, 102, 101} {
		s := &Session{
			VolumeID:   vol,
			ExpiresAt:  time.Now().UTC().Add(time.Hour),
			SessionKey: []byte("key-32-bytes-long!!!!!!!!!!!!!!!!"),
		}
		tbl.Add(s)
	}
	tbl.RevokeByVolume(101)
	if tbl.Get(1) != nil || tbl.Get(3) != nil {
		t.Fatal("sessions for volume 101 should be removed")
	}
	got := tbl.Get(2)
	if got == nil || got.VolumeID != 102 {
		t.Fatalf("volume 102 session should remain: %+v", got)
	}
}

func TestSessionSeqChecker(t *testing.T) {
	s := &Session{}
	if err := s.Seq.CheckNext(1); err != nil {
		t.Fatalf("first seq 1: %v", err)
	}
	if err := s.Seq.CheckNext(2); err != nil {
		t.Fatalf("seq 2: %v", err)
	}
	if err := s.Seq.CheckNext(2); err != wirev2.ErrReplaySeq {
		t.Fatalf("duplicate seq should be replay: %v", err)
	}
}
