package platform

import "testing"

func TestValidateLoopbackProviderURL(t *testing.T) {
	for _, value := range []string{
		"http://127.0.0.1:8080",
		"http://[::1]:8080/v10",
	} {
		if err := ValidateLoopbackProviderURL(value); err != nil {
			t.Fatalf("valid local provider URL %q failed: %v", value, err)
		}
	}
	for _, value := range []string{
		"https://127.0.0.1:8080",
		"http://localhost:8080",
		"http://192.168.1.2:8080",
		"http://127.0.0.1",
		"http://user@127.0.0.1:8080",
		"http://127.0.0.1:8080?token=value",
	} {
		if err := ValidateLoopbackProviderURL(value); err == nil {
			t.Fatalf("unsafe local provider URL %q passed", value)
		}
	}
}
