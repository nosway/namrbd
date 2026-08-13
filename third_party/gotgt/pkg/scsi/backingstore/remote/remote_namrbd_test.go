package remote

import (
	"errors"
	"testing"

	"github.com/gostor/gotgt/pkg/api"
)

type unmapCall struct {
	offset int64
	length int64
}

type zeroCall struct {
	offset int64
	length int64
}

type recordingRemoteBackingStore struct {
	unmapCalls []unmapCall
	unmapErr   error
	zeroCalls  []zeroCall
	zeroErr    error
}

func (r *recordingRemoteBackingStore) ReadAt(_ []byte, _ int64) (int, error) {
	return 0, nil
}

func (r *recordingRemoteBackingStore) WriteAt(p []byte, _ int64) (int, error) {
	return len(p), nil
}

func (r *recordingRemoteBackingStore) Sync() (int, error) {
	return 0, nil
}

func (r *recordingRemoteBackingStore) Unmap(offset, length int64) (int, error) {
	if r.unmapErr != nil {
		return 0, r.unmapErr
	}
	r.unmapCalls = append(r.unmapCalls, unmapCall{offset: offset, length: length})
	return 0, nil
}

func (r *recordingRemoteBackingStore) Zero(offset, length int64) (int, error) {
	if r.zeroErr != nil {
		return 0, r.zeroErr
	}
	r.zeroCalls = append(r.zeroCalls, zeroCall{offset: offset, length: length})
	return 0, nil
}

type noZeroRemoteBackingStore struct {
	api.RemoteBackingStore
}

func TestNAMRBDRemoteBackingStoreUnmapForwardsDescriptors(t *testing.T) {
	remote := &recordingRemoteBackingStore{}
	bs := &RemBackingStore{RemBs: remote}

	err := bs.Unmap([]api.UnmapBlockDescriptor{
		{Offset: 4096, TL: 8192},
		{Offset: 12288, TL: 0},
		{Offset: 16384, TL: 4096},
	})
	if err != nil {
		t.Fatalf("Unmap returned error: %v", err)
	}
	if len(remote.unmapCalls) != 2 {
		t.Fatalf("unmap call count=%d, want 2", len(remote.unmapCalls))
	}
	if remote.unmapCalls[0] != (unmapCall{offset: 4096, length: 8192}) {
		t.Fatalf("first unmap call=%+v", remote.unmapCalls[0])
	}
	if remote.unmapCalls[1] != (unmapCall{offset: 16384, length: 4096}) {
		t.Fatalf("second unmap call=%+v", remote.unmapCalls[1])
	}
}

func TestNAMRBDRemoteBackingStoreUnmapPropagatesError(t *testing.T) {
	wantErr := errors.New("discard failed")
	bs := &RemBackingStore{RemBs: &recordingRemoteBackingStore{unmapErr: wantErr}}

	if err := bs.Unmap([]api.UnmapBlockDescriptor{{Offset: 4096, TL: 4096}}); !errors.Is(err, wantErr) {
		t.Fatalf("Unmap error=%v, want %v", err, wantErr)
	}
}

func TestNAMRBDRemoteBackingStoreUnmapRejectsMissingRemote(t *testing.T) {
	bs := &RemBackingStore{}

	if err := bs.Unmap([]api.UnmapBlockDescriptor{{Offset: 4096, TL: 4096}}); err == nil {
		t.Fatalf("Unmap unexpectedly succeeded without remote backing store")
	}
}

func TestNAMRBDRemoteBackingStoreZeroForwardsRange(t *testing.T) {
	remote := &recordingRemoteBackingStore{}
	bs := &RemBackingStore{RemBs: remote}

	if err := bs.Zero(4096, 8192); err != nil {
		t.Fatalf("Zero returned error: %v", err)
	}
	if len(remote.zeroCalls) != 1 {
		t.Fatalf("zero call count=%d, want 1", len(remote.zeroCalls))
	}
	if remote.zeroCalls[0] != (zeroCall{offset: 4096, length: 8192}) {
		t.Fatalf("zero call=%+v", remote.zeroCalls[0])
	}
}

func TestNAMRBDRemoteBackingStoreZeroPropagatesError(t *testing.T) {
	wantErr := errors.New("zero failed")
	bs := &RemBackingStore{RemBs: &recordingRemoteBackingStore{zeroErr: wantErr}}

	if err := bs.Zero(4096, 4096); !errors.Is(err, wantErr) {
		t.Fatalf("Zero error=%v, want %v", err, wantErr)
	}
}

func TestNAMRBDRemoteBackingStoreZeroRejectsMissingRemote(t *testing.T) {
	bs := &RemBackingStore{}

	if err := bs.Zero(4096, 4096); err == nil {
		t.Fatalf("Zero unexpectedly succeeded without remote backing store")
	}
}

func TestNAMRBDRemoteBackingStoreZeroRejectsUnsupportedRemote(t *testing.T) {
	bs := &RemBackingStore{RemBs: noZeroRemoteBackingStore{RemoteBackingStore: &recordingRemoteBackingStore{}}}

	if err := bs.Zero(4096, 4096); err == nil {
		t.Fatalf("Zero unexpectedly succeeded without zero-capable remote backing store")
	}
}
