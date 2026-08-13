package wirev1

import "testing"

func TestHelloPayloadRoundTrip(t *testing.T) {
	in := Hello{
		HostID:       11,
		BootIDHash:   22,
		SessionNonce: 33,
		ClientCaps:   0xA5,
	}
	payload := EncodeHelloPayload(in)
	out, err := DecodeHelloPayload(payload)
	if err != nil {
		t.Fatalf("DecodeHelloPayload failed: %v", err)
	}
	if out != in {
		t.Fatalf("hello mismatch: got=%+v want=%+v", out, in)
	}
}

func TestPathCapabilityRoundTrip(t *testing.T) {
	in := PathCapability{
		MaxIOSize:           1 << 20,
		MaxSegments:         128,
		SupportedOpsMask:    (1 << 1) | (1 << 2),
		Features:            FeatureCompression | FeatureIntegrity,
		MaxInflightRequests: 1024,
		MaxInflightBytes:    256 << 20,
		MaxZeroLikeIOSize:   256 << 20,
	}
	payload := EncodePathCapabilityPayload(in)
	out, err := DecodePathCapabilityPayload(payload)
	if err != nil {
		t.Fatalf("DecodePathCapabilityPayload failed: %v", err)
	}
	if out != in {
		t.Fatalf("path capability mismatch: got=%+v want=%+v", out, in)
	}
}

func TestPathCapabilityDecodesLegacyPayload(t *testing.T) {
	payload := EncodePathCapabilityPayload(PathCapability{
		MaxIOSize:           1 << 20,
		MaxSegments:         128,
		SupportedOpsMask:    (1 << 1) | (1 << 2),
		Features:            FeatureCompression | FeatureIntegrity,
		MaxInflightRequests: 1024,
		MaxInflightBytes:    256 << 20,
		MaxZeroLikeIOSize:   256 << 20,
	})[:32]
	out, err := DecodePathCapabilityPayload(payload)
	if err != nil {
		t.Fatalf("DecodePathCapabilityPayload failed: %v", err)
	}
	if out.MaxZeroLikeIOSize != out.MaxIOSize {
		t.Fatalf("legacy zero-like max=%d want data max=%d", out.MaxZeroLikeIOSize, out.MaxIOSize)
	}
}

func TestWritePayloadRoundTrip(t *testing.T) {
	inTag := WriteTag{HostID: 10, BootIDHash: 20, SequenceNo: 30}
	inData := []byte("write-body")
	payload := EncodeWritePayload(inTag, inData)

	outTag, outData, err := DecodeWritePayload(payload)
	if err != nil {
		t.Fatalf("DecodeWritePayload failed: %v", err)
	}
	if outTag != inTag {
		t.Fatalf("write tag mismatch: got=%+v want=%+v", outTag, inTag)
	}
	if string(outData) != string(inData) {
		t.Fatalf("write data mismatch: got=%q want=%q", outData, inData)
	}
}

func TestErrorDetailRoundTrip(t *testing.T) {
	in := ErrorDetail{
		RetryAfterMS: 250,
		DetailCode:   9001,
		Message:      "draining for upgrade",
	}
	payload, err := EncodeErrorDetailPayload(in)
	if err != nil {
		t.Fatalf("EncodeErrorDetailPayload failed: %v", err)
	}
	out, err := DecodeErrorDetailPayload(payload)
	if err != nil {
		t.Fatalf("DecodeErrorDetailPayload failed: %v", err)
	}
	if out != in {
		t.Fatalf("error detail mismatch: got=%+v want=%+v", out, in)
	}
}
