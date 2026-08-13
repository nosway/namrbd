//go:build !linux

package netlinkclient

import (
	"errors"

	"github.com/nosway/namrbd/control/netlinktlv"
)

var errLinuxOnly = errors.New("netlink client is supported only on linux")

type stubClient struct{}

func Dial() (Client, error)     { return nil, errLinuxOnly }
func (stubClient) Close() error { return nil }
func (stubClient) CreateDevice() (netlinktlv.CreateDeviceResponse, error) {
	return netlinktlv.CreateDeviceResponse{}, errLinuxOnly
}
func (stubClient) DestroyDevice(uint32) error                            { return errLinuxOnly }
func (stubClient) ConfigREST(netlinktlv.ConfigRESTRequest) error         { return errLinuxOnly }
func (stubClient) AttachVolume(netlinktlv.AttachRequest) error           { return errLinuxOnly }
func (stubClient) DetachVolume(netlinktlv.DetachRequest) error           { return errLinuxOnly }
func (stubClient) AttachManifest(netlinktlv.AttachManifestRequest) error { return errLinuxOnly }
func (stubClient) ReconfigureDataPaths(netlinktlv.AttachManifestRequest) error {
	return errLinuxOnly
}
func (stubClient) DetachLocal(netlinktlv.DetachLocalRequest) error       { return errLinuxOnly }
func (stubClient) UpdatePathPlan(netlinktlv.UpdatePathPlanRequest) error { return errLinuxOnly }
func (stubClient) ResizeDevice(netlinktlv.ResizeDeviceRequest) error     { return errLinuxOnly }
func (stubClient) GetStatus(uint32) (netlinktlv.DeviceStatus, error) {
	return netlinktlv.DeviceStatus{}, errLinuxOnly
}
func (stubClient) ListDevices() ([]netlinktlv.DeviceStatus, error) {
	return nil, errLinuxOnly
}
