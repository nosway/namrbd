package netlinkclient

import "github.com/nosway/namrbd/control/netlinktlv"

type Client interface {
	Close() error
	CreateDevice() (netlinktlv.CreateDeviceResponse, error)
	DestroyDevice(deviceID uint32) error
	ConfigREST(req netlinktlv.ConfigRESTRequest) error
	AttachVolume(req netlinktlv.AttachRequest) error
	DetachVolume(req netlinktlv.DetachRequest) error
	AttachManifest(req netlinktlv.AttachManifestRequest) error
	ReconfigureDataPaths(req netlinktlv.AttachManifestRequest) error
	DetachLocal(req netlinktlv.DetachLocalRequest) error
	UpdatePathPlan(req netlinktlv.UpdatePathPlanRequest) error
	ResizeDevice(req netlinktlv.ResizeDeviceRequest) error
	GetStatus(deviceID uint32) (netlinktlv.DeviceStatus, error)
	ListDevices() ([]netlinktlv.DeviceStatus, error)
}
