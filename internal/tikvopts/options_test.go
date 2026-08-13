package tikvopts

import "testing"

func TestValidate(t *testing.T) {
	if err := Validate(Options{}); err == nil {
		t.Fatal("expected error for missing PD endpoints")
	}
	if err := Validate(Options{
		PDEndpoints: []string{"127.0.0.1:2379"},
		APIVersion:  "bad",
	}); err == nil {
		t.Fatal("expected error for invalid API version")
	}
	if err := Validate(Options{
		PDEndpoints: []string{"127.0.0.1:2379"},
		APIVersion:  APIVersionV1,
	}); err != nil {
		t.Fatalf("Validate valid options: %v", err)
	}
}
