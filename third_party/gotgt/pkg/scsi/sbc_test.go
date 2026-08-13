/*
Copyright 2015 The GoStor Authors All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// SCSI block command processing
package scsi

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/gostor/gotgt/pkg/api"
)

func TestSBCModeSelect(t *testing.T) {
}

func TestSBCModeSense(t *testing.T) {
}

func TestSBCFormatUnit(t *testing.T) {
}

func TestSBCUnmap(t *testing.T) {
}

func TestSBCReadWrite(t *testing.T) {
}

type namrbdWriteSameRange struct {
	offset int64
	length int64
}

type namrbdWriteSameStore struct {
	zeroCalls  []namrbdWriteSameRange
	unmapCalls []api.UnmapBlockDescriptor
	writeErr   error
	zeroErr    error
	unmapErr   error
}

func (s *namrbdWriteSameStore) Open(*api.SCSILu, string) error {
	return nil
}

func (s *namrbdWriteSameStore) Close(*api.SCSILu) error {
	return nil
}

func (s *namrbdWriteSameStore) Init(*api.SCSILu, string) error {
	return nil
}

func (s *namrbdWriteSameStore) Exit(*api.SCSILu) error {
	return nil
}

func (s *namrbdWriteSameStore) Size(*api.SCSILu) uint64 {
	return 64 * 1024
}

func (s *namrbdWriteSameStore) Read(int64, int64) ([]byte, error) {
	return nil, nil
}

func (s *namrbdWriteSameStore) Write([]byte, int64) error {
	if s.writeErr != nil {
		return s.writeErr
	}
	return nil
}

func (s *namrbdWriteSameStore) DataSync(int64, int64) error {
	return nil
}

func (s *namrbdWriteSameStore) DataAdvise(int64, int64, uint32) error {
	return nil
}

func (s *namrbdWriteSameStore) Unmap(desc []api.UnmapBlockDescriptor) error {
	if s.unmapErr != nil {
		return s.unmapErr
	}
	s.unmapCalls = append(s.unmapCalls, desc...)
	return nil
}

func (s *namrbdWriteSameStore) Zero(offset, length int64) error {
	if s.zeroErr != nil {
		return s.zeroErr
	}
	s.zeroCalls = append(s.zeroCalls, namrbdWriteSameRange{offset: offset, length: length})
	return nil
}

type namrbdWriteSameStoreWithoutZero struct {
	unmapCalls []api.UnmapBlockDescriptor
}

func (s *namrbdWriteSameStoreWithoutZero) Open(*api.SCSILu, string) error {
	return nil
}

func (s *namrbdWriteSameStoreWithoutZero) Close(*api.SCSILu) error {
	return nil
}

func (s *namrbdWriteSameStoreWithoutZero) Init(*api.SCSILu, string) error {
	return nil
}

func (s *namrbdWriteSameStoreWithoutZero) Exit(*api.SCSILu) error {
	return nil
}

func (s *namrbdWriteSameStoreWithoutZero) Size(*api.SCSILu) uint64 {
	return 64 * 1024
}

func (s *namrbdWriteSameStoreWithoutZero) Read(int64, int64) ([]byte, error) {
	return nil, nil
}

func (s *namrbdWriteSameStoreWithoutZero) Write([]byte, int64) error {
	return nil
}

func (s *namrbdWriteSameStoreWithoutZero) DataSync(int64, int64) error {
	return nil
}

func (s *namrbdWriteSameStoreWithoutZero) DataAdvise(int64, int64, uint32) error {
	return nil
}

func (s *namrbdWriteSameStoreWithoutZero) Unmap(desc []api.UnmapBlockDescriptor) error {
	s.unmapCalls = append(s.unmapCalls, desc...)
	return nil
}

func TestNAMRBDWriteSameZeroPatternForwardsZero(t *testing.T) {
	store := &namrbdWriteSameStore{}
	cmd := namrbdWriteSameCommand(api.WRITE_SAME, 0, 8, 4, make([]byte, 512), store)

	if got := SBCReadWrite(0, cmd); got.Stat != api.SAMStatGood.Stat {
		t.Fatalf("SBCReadWrite status=%#v, want good", got)
	}
	if len(store.zeroCalls) != 1 {
		t.Fatalf("zero call count=%d, want 1", len(store.zeroCalls))
	}
	if store.zeroCalls[0] != (namrbdWriteSameRange{offset: 4096, length: 2048}) {
		t.Fatalf("zero call=%+v", store.zeroCalls[0])
	}
	if len(store.unmapCalls) != 0 {
		t.Fatalf("unexpected unmap calls: %+v", store.unmapCalls)
	}
}

func TestNAMRBDWriteSameUnmapBitForwardsDiscard(t *testing.T) {
	store := &namrbdWriteSameStore{}
	cmd := namrbdWriteSameCommand(api.WRITE_SAME_16, 0x08, 8, 4, make([]byte, 512), store)

	if got := SBCReadWrite(0, cmd); got.Stat != api.SAMStatGood.Stat {
		t.Fatalf("SBCReadWrite status=%#v, want good", got)
	}
	if len(store.unmapCalls) != 1 {
		t.Fatalf("unmap call count=%d, want 1", len(store.unmapCalls))
	}
	if store.unmapCalls[0] != (api.UnmapBlockDescriptor{Offset: 4096, TL: 2048}) {
		t.Fatalf("unmap call=%+v", store.unmapCalls[0])
	}
	if len(store.zeroCalls) != 0 {
		t.Fatalf("unexpected zero calls: %+v", store.zeroCalls)
	}
}

func TestNAMRBDWriteSameRejectsNonZeroPattern(t *testing.T) {
	store := &namrbdWriteSameStore{}
	pattern := make([]byte, 512)
	pattern[0] = 0x5a
	cmd := namrbdWriteSameCommand(api.WRITE_SAME, 0, 8, 4, pattern, store)

	if got := SBCReadWrite(0, cmd); got.Stat != api.SAMStatCheckCondition.Stat {
		t.Fatalf("SBCReadWrite status=%#v, want check condition", got)
	}
	if len(store.unmapCalls) != 0 {
		t.Fatalf("unexpected backend unmap calls=%+v", store.unmapCalls)
	}
}

func TestNAMRBDWriteSameRejectsUnsupportedLBDATA(t *testing.T) {
	store := &namrbdWriteSameStore{}
	cmd := namrbdWriteSameCommand(api.WRITE_SAME, 0x02, 8, 4, make([]byte, 512), store)

	if got := SBCReadWrite(0, cmd); got.Stat != api.SAMStatCheckCondition.Stat {
		t.Fatalf("SBCReadWrite status=%#v, want check condition", got)
	}
	if len(store.unmapCalls) != 0 {
		t.Fatalf("unexpected backend unmap calls=%+v", store.unmapCalls)
	}
}

func TestNAMRBDWriteSameRejectsMissingZeroHook(t *testing.T) {
	store := &namrbdWriteSameStoreWithoutZero{}
	cmd := namrbdWriteSameCommand(api.WRITE_SAME, 0, 8, 4, make([]byte, 512), store)

	if got := SBCReadWrite(0, cmd); got.Stat != api.SAMStatCheckCondition.Stat {
		t.Fatalf("SBCReadWrite status=%#v, want check condition", got)
	}
	if len(store.unmapCalls) != 0 {
		t.Fatalf("unexpected backend unmap calls=%+v", store.unmapCalls)
	}
}

func TestNAMRBDWriteSameZeroBackendErrorReturnsBusy(t *testing.T) {
	wantErr := errors.New("zero backend failed")
	store := &namrbdWriteSameStore{zeroErr: wantErr}
	cmd := namrbdWriteSameCommand(api.WRITE_SAME, 0, 8, 4, make([]byte, 512), store)

	if got := SBCReadWrite(0, cmd); got.Stat != api.SAMStatBusy.Stat {
		t.Fatalf("SBCReadWrite status=%#v, want busy", got)
	}
}

type namrbdBackendSenseError struct{}

func (namrbdBackendSenseError) Error() string {
	return "standby path write rejected"
}

func (namrbdBackendSenseError) GotgtSense() (byte, SCSISubError, bool) {
	return DATA_PROTECT, ASC_WRITE_PROTECT, true
}

func TestNAMRBDWriteDataProtectBackendErrorReturnsCheckCondition(t *testing.T) {
	store := &namrbdWriteSameStore{writeErr: namrbdBackendSenseError{}}
	cmd := namrbdWriteSameCommand(api.WRITE_10, 0, 8, 1, make([]byte, 512), store)

	if got := SBCReadWrite(0, cmd); got.Stat != api.SAMStatCheckCondition.Stat {
		t.Fatalf("SBCReadWrite status=%#v, want check condition", got)
	}
	if cmd.SenseBuffer == nil {
		t.Fatalf("expected sense buffer for data protect backend error")
	}
}

func namrbdWriteSameCommand(opcode api.SCSICommandType, flags byte, lba uint64, blocks uint32, pattern []byte, store api.BackingStore) *api.SCSICommand {
	scbLen := 10
	if opcode == api.WRITE_SAME_16 {
		scbLen = 16
	}
	scb := make([]byte, scbLen)
	scb[0] = byte(opcode)
	scb[1] = flags
	if opcode == api.WRITE_SAME_16 {
		binary.BigEndian.PutUint64(scb[2:10], lba)
		binary.BigEndian.PutUint32(scb[10:14], blocks)
	} else {
		binary.BigEndian.PutUint32(scb[2:6], uint32(lba))
		binary.BigEndian.PutUint16(scb[7:9], uint16(blocks))
	}
	return &api.SCSICommand{
		SCB:       scb,
		SCBLength: scbLen,
		Device: &api.SCSILu{
			Size:       64 * 1024,
			BlockShift: 9,
			Attrs: api.SCSILuPhyAttribute{
				ThinProvisioning: true,
			},
			Storage: store,
		},
		OutSDBBuffer: &api.SCSIDataBuffer{
			Buffer: pattern,
			Length: uint32(len(pattern)),
		},
	}
}

func TestSBCReserve(t *testing.T) {
}

func TestSBCRelease(t *testing.T) {
}

func TestSBCReadCapacity(t *testing.T) {
}

func TestSBCVerify(t *testing.T) {
}

func TestSBCReadCapacity16(t *testing.T) {
}

func TestSBCGetLbaStatus(t *testing.T) {
}

func TestSBCSyncCache(t *testing.T) {
}
