package driver

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCommandNodeHelperAcceptsZeroDeviceID(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "commands.log")
	namrbdctlPath := filepath.Join(dir, "namrbdctl")
	script := `#!/bin/sh
echo "$@" >> "$NAMRBD_FAKE_LOG"
case "$1" in
create-device)
	printf '{"device_id":0,"disk_name":"namrbd0"}\n'
	;;
attach)
	printf '{"attachment_id":"att-zero-0001","generation":1}\n'
	;;
detach|destroy-device)
	printf 'ok\n'
	;;
volume-reload-size)
	printf '{"result":"ok"}\n'
	;;
*)
	echo "unexpected command: $1" >&2
	exit 1
	;;
esac
`
	if err := os.WriteFile(namrbdctlPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake namrbdctl: %v", err)
	}
	t.Setenv("NAMRBD_FAKE_LOG", logPath)

	helper := NewCommandNodeHelper(CommandNodeHelperConfig{
		NodeID:        "ubuntu12",
		GatewayURL:    "http://gateway.example",
		NamrbdctlPath: namrbdctlPath,
	})
	attachment, err := helper.Attach(context.Background(), NodeAttachRequest{
		VolumeID: "00a1b2c3",
		NodeID:   "ubuntu12",
	})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	if attachment.DeviceID != 0 || attachment.DevicePath != "/dev/namrbd0" || attachment.AttachmentID != "att-zero-0001" {
		t.Fatalf("attachment=%+v", attachment)
	}
	if err := helper.ReloadSize(context.Background(), NodeReloadSizeRequest{
		VolumeID:   "00a1b2c3",
		DeviceID:   attachment.DeviceID,
		DevicePath: attachment.DevicePath,
	}); err != nil {
		t.Fatalf("ReloadSize: %v", err)
	}
	if err := helper.Detach(context.Background(), NodeDetachRequest{
		VolumeID:     "00a1b2c3",
		DeviceID:     attachment.DeviceID,
		AttachmentID: attachment.AttachmentID,
		Generation:   attachment.Generation,
	}); err != nil {
		t.Fatalf("Detach: %v", err)
	}

	logBytes, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read command log: %v", err)
	}
	logText := string(logBytes)
	for _, want := range []string{
		"attach --device 0 --host ubuntu12 --volume 00a1b2c3 --gateway http://gateway.example",
		"volume-reload-size --device 0 --host ubuntu12 --volume 00a1b2c3 --gateway http://gateway.example",
		"detach --device 0 --host ubuntu12 --volume 00a1b2c3 --gateway http://gateway.example",
		"destroy-device --device 0",
	} {
		if !strings.Contains(logText, want) {
			t.Fatalf("command log missing %q in:\n%s", want, logText)
		}
	}
}

func TestCommandNodeHelperRejectsMissingDeviceID(t *testing.T) {
	dir := t.TempDir()
	namrbdctlPath := filepath.Join(dir, "namrbdctl")
	script := `#!/bin/sh
case "$1" in
create-device)
	printf '{"disk_name":"namrbd0"}\n'
	;;
*)
	echo "unexpected command: $1" >&2
	exit 1
	;;
esac
`
	if err := os.WriteFile(namrbdctlPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake namrbdctl: %v", err)
	}

	helper := NewCommandNodeHelper(CommandNodeHelperConfig{
		NodeID:        "ubuntu12",
		GatewayURL:    "http://gateway.example",
		NamrbdctlPath: namrbdctlPath,
	})
	_, err := helper.Attach(context.Background(), NodeAttachRequest{
		VolumeID: "00a1b2c3",
		NodeID:   "ubuntu12",
	})
	if err == nil || !strings.Contains(err.Error(), "missing device_id or disk_name") {
		t.Fatalf("Attach err=%v want missing device_id", err)
	}
}

func TestCommandNodeHelperLookupAttachmentFromListDevices(t *testing.T) {
	dir := t.TempDir()
	namrbdctlPath := filepath.Join(dir, "namrbdctl")
	script := `#!/bin/sh
case "$1" in
list-devices)
	cat <<'JSON'
[
  {"device_id":3,"disk_name":"namrbd3","volume_id":"other","attached":true,"generation":1},
  {"device_id":0,"disk_name":"namrbd0","volume_id":"00a1b2c3","attached":true,"generation":9}
]
JSON
	;;
*)
	echo "unexpected command: $1" >&2
	exit 1
	;;
esac
`
	if err := os.WriteFile(namrbdctlPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake namrbdctl: %v", err)
	}

	helper := NewCommandNodeHelper(CommandNodeHelperConfig{NamrbdctlPath: namrbdctlPath})
	attachment, err := helper.LookupAttachment(context.Background(), NodeLookupAttachmentRequest{VolumeID: "00a1b2c3"})
	if err != nil {
		t.Fatalf("LookupAttachment: %v", err)
	}
	if attachment.DeviceID != 0 || attachment.DevicePath != "/dev/namrbd0" || attachment.Generation != 9 {
		t.Fatalf("attachment=%+v", attachment)
	}
}

func TestCommandNodeHelperLookupAttachmentRejectsAmbiguousDevices(t *testing.T) {
	dir := t.TempDir()
	namrbdctlPath := filepath.Join(dir, "namrbdctl")
	script := `#!/bin/sh
case "$1" in
list-devices)
	cat <<'JSON'
[
  {"device_id":3,"disk_name":"namrbd3","volume_id":"00a1b2c3","attached":true},
  {"device_id":4,"disk_name":"namrbd4","volume_id":"00a1b2c3","attached":true}
]
JSON
	;;
*)
	echo "unexpected command: $1" >&2
	exit 1
	;;
esac
`
	if err := os.WriteFile(namrbdctlPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake namrbdctl: %v", err)
	}

	helper := NewCommandNodeHelper(CommandNodeHelperConfig{NamrbdctlPath: namrbdctlPath})
	_, err := helper.LookupAttachment(context.Background(), NodeLookupAttachmentRequest{VolumeID: "00a1b2c3"})
	if err == nil || !strings.Contains(err.Error(), errNodeAttachmentAmbiguous.Error()) {
		t.Fatalf("LookupAttachment err=%v want ambiguous", err)
	}
}

func TestCommandNodeHelperFormatDisablesDiscard(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "commands.log")
	for name, script := range map[string]string{
		"blkid": `#!/bin/sh
echo "$0 $@" >> "$NAMRBD_FAKE_LOG"
exit 2
`,
		"mkfs.ext4": `#!/bin/sh
echo "$0 $@" >> "$NAMRBD_FAKE_LOG"
`,
		"mkfs.xfs": `#!/bin/sh
echo "$0 $@" >> "$NAMRBD_FAKE_LOG"
`,
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(script), 0o755); err != nil {
			t.Fatalf("write fake %s: %v", name, err)
		}
	}
	t.Setenv("NAMRBD_FAKE_LOG", logPath)
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	helper := NewCommandNodeHelper(CommandNodeHelperConfig{})
	if err := helper.FormatIfNeeded(context.Background(), NodeFormatRequest{
		DevicePath: "/dev/namrbd0",
		FSType:     "ext4",
	}); err != nil {
		t.Fatalf("FormatIfNeeded ext4: %v", err)
	}
	if err := helper.FormatIfNeeded(context.Background(), NodeFormatRequest{
		DevicePath: "/dev/namrbd1",
		FSType:     "xfs",
	}); err != nil {
		t.Fatalf("FormatIfNeeded xfs: %v", err)
	}

	logBytes, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read command log: %v", err)
	}
	logText := string(logBytes)
	for _, want := range []string{
		filepath.Join(dir, "blkid") + " /dev/namrbd0",
		filepath.Join(dir, "mkfs.ext4") + " -F -E nodiscard /dev/namrbd0",
		filepath.Join(dir, "blkid") + " /dev/namrbd1",
		filepath.Join(dir, "mkfs.xfs") + " -f -K /dev/namrbd1",
	} {
		if !strings.Contains(logText, want) {
			t.Fatalf("command log missing %q in:\n%s", want, logText)
		}
	}
}
