/*
Copyright 2017 The GoStor Authors All rights reserved.

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

// SCSI primary command processing test
package scsi

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/gostor/gotgt/pkg/api"
)

// Test SPCReportLuns function
func TestSPCReportLuns(t *testing.T) {
	// make a fake REPORT_LUNS command
	cmd := new(api.SCSICommand)
	device := new(api.SCSILu)
	cmd.Device = device
	lu := new(api.SCSILu)
	target := new(api.SCSITarget)
	target.Devices = map[uint64]*api.SCSILu{0: lu}
	cmd.Target = target
	scb := &bytes.Buffer{}
	cmd.InSDBBuffer = &api.SCSIDataBuffer{}
	cmd.InSDBBuffer.Length = 16
	cmd.InSDBBuffer.Buffer = []byte{}
	scb.WriteByte(byte(api.REPORT_LUNS))
	for i := 0; i < 5; i++ {
		scb.WriteByte(0x00)
	}
	binary.Write(scb, binary.BigEndian, uint32(16))
	cmd.SCB = scb.Bytes()

	if err := SPCReportLuns(0, cmd); err.Err != nil {
		t.Errorf("Expected not error, but got %v", err)
	}

	scb = &bytes.Buffer{}
	scb.WriteByte(byte(api.REPORT_LUNS))
	for i := 0; i < 5; i++ {
		scb.WriteByte(0x00)
	}
	binary.Write(scb, binary.BigEndian, uint32(10))
	cmd.SCB = scb.Bytes()
	if err := SPCReportLuns(0, cmd); err.Err == nil {
		t.Error("Expected error, but got nothing")
	}
}

func TestSPCReportTargetPortGroups(t *testing.T) {
	cmd := new(api.SCSICommand)
	cmd.Device = new(api.SCSILu)
	cmd.Target = &api.SCSITarget{
		TargetPortGroups: []*api.TargetPortGroup{
			{
				GroupID:               1,
				AsymmetricAccessState: 0x00,
				Preferred:             true,
				ImplicitALUASupported: true,
				TargetPortGroup: []*api.SCSITargetPort{
					{RelativeTargetPortID: 1, TargetPortName: "iqn.test,t,0x01"},
				},
			},
			{
				GroupID:               2,
				AsymmetricAccessState: 0x02,
				ImplicitALUASupported: true,
				TargetPortGroup: []*api.SCSITargetPort{
					{RelativeTargetPortID: 2, TargetPortName: "iqn.test,t,0x02"},
				},
			},
		},
	}
	cmd.InSDBBuffer = &api.SCSIDataBuffer{
		Length: 64,
		Buffer: make([]byte, 64),
	}
	scb := &bytes.Buffer{}
	scb.WriteByte(byte(api.MAINT_PROTOCOL_IN))
	scb.WriteByte(0x0a)
	for i := 0; i < 4; i++ {
		scb.WriteByte(0x00)
	}
	binary.Write(scb, binary.BigEndian, uint32(64))
	for i := 0; i < 2; i++ {
		scb.WriteByte(0x00)
	}
	cmd.SCB = scb.Bytes()

	if err := SPCReportTargetPortGroups(0, cmd); err.Err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if got := binary.BigEndian.Uint32(cmd.InSDBBuffer.Buffer[0:4]); got != 24 {
		t.Fatalf("returned data length=%d want 24", got)
	}
	if got := cmd.InSDBBuffer.Buffer[4]; got != 0x80 {
		t.Fatalf("group 1 state byte=0x%x want preferred active/optimized", got)
	}
	if got := binary.BigEndian.Uint16(cmd.InSDBBuffer.Buffer[6:8]); got != 1 {
		t.Fatalf("group 1 id=%d want 1", got)
	}
	if got := binary.BigEndian.Uint16(cmd.InSDBBuffer.Buffer[14:16]); got != 1 {
		t.Fatalf("group 1 relative port id=%d want 1", got)
	}
	if got := cmd.InSDBBuffer.Buffer[16]; got != 0x02 {
		t.Fatalf("group 2 state byte=0x%x want standby", got)
	}
	if got := binary.BigEndian.Uint16(cmd.InSDBBuffer.Buffer[18:20]); got != 2 {
		t.Fatalf("group 2 id=%d want 2", got)
	}
	if got := binary.BigEndian.Uint16(cmd.InSDBBuffer.Buffer[26:28]); got != 2 {
		t.Fatalf("group 2 relative port id=%d want 2", got)
	}
}

func TestInquiryPage83FallsBackToFirstTargetPort(t *testing.T) {
	cmd := &api.SCSICommand{
		Device: &api.SCSILu{},
		Target: &api.SCSITarget{
			Name: "iqn.test",
			TargetPortGroups: []*api.TargetPortGroup{
				{GroupID: 0},
				{
					GroupID: 1,
					TargetPortGroup: []*api.SCSITargetPort{
						{RelativeTargetPortID: 1, TargetPortName: "iqn.test,t,0x01"},
					},
				},
			},
		},
	}

	buf, pageLength := InquiryPage0x83(0, cmd)
	if pageLength == 0 || buf.Len() == 0 {
		t.Fatalf("empty page 0x83 response len=%d buffer=%d", pageLength, buf.Len())
	}
	if !bytes.Contains(buf.Bytes(), []byte("iqn.test,t,0x01")) {
		t.Fatalf("page 0x83 did not use first target port fallback: %x", buf.Bytes())
	}
}

func TestInquiryPage83SynthesizesMissingTargetPort(t *testing.T) {
	cmd := &api.SCSICommand{
		Device: &api.SCSILu{},
		Target: &api.SCSITarget{
			Name: "iqn.test",
		},
	}

	buf, pageLength := InquiryPage0x83(0, cmd)
	if pageLength == 0 || buf.Len() == 0 {
		t.Fatalf("empty page 0x83 response len=%d buffer=%d", pageLength, buf.Len())
	}
	if !bytes.Contains(buf.Bytes(), []byte("iqn.test,t,0x00")) {
		t.Fatalf("page 0x83 did not synthesize a target port name: %x", buf.Bytes())
	}
}

func TestSPCStartStop(t *testing.T) {
}

func TestSPCTestUnit(t *testing.T) {
}

func TestSPCPreventAllowMediaRemoval(t *testing.T) {
}
