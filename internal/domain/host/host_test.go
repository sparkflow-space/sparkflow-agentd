package host_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/sparkflow-space/sparkflow-agentd/internal/domain/host"
)

func TestDeploymentFor_MirrorsSparkflowSync(t *testing.T) {
	if got := host.DeploymentFor(false); got.ServerURL != "https://sparkflow.space" || got.IssuerURL != "https://login.sparkflow.space" {
		t.Errorf("the default must be production: %+v", got)
	}
	if got := host.DeploymentFor(true); got.ServerURL != "https://sparkflow.ddns.net" || got.IssuerURL != "https://login.sparkflow.ddns.net" {
		t.Errorf("--debug-dev must select dev, server and issuer together: %+v", got)
	}
	if err := host.Production.Validate(); !errors.Is(err, host.ErrNotProvisioned) {
		t.Errorf("an empty client id must say 'not provisioned', got %v", err)
	}
}

func TestSuggestName(t *testing.T) {
	cases := map[[2]string]string{
		{"Ubuntu", "Germany"}:   "Ubuntu · Germany",
		{" macOS ", " Spain "}:  "macOS · Spain",
		{"Ubuntu", ""}:          "Ubuntu",
		{"", ""}:                "Host",
		{"Arch  Linux", "Peru"}: "Arch Linux · Peru",
	}
	for in, want := range cases {
		if got := host.SuggestName(in[0], in[1]); got != want {
			t.Errorf("SuggestName(%q,%q) = %q, want %q", in[0], in[1], got, want)
		}
	}
	long := host.SuggestName(strings.Repeat("я", 80), "X")
	if n := len([]rune(long)); n > host.MaxNameRunes {
		t.Errorf("name is %d runes, cap is %d", n, host.MaxNameRunes)
	}
}

func TestReEnrolID_OneHostPerDevice(t *testing.T) {
	stored := &host.Credentials{HostID: "h1", OwnerSub: "A"}
	if id, err := host.ReEnrolID(nil, "A", false); id != "" || err != nil {
		t.Errorf("first run: %q %v", id, err)
	}
	if id, err := host.ReEnrolID(stored, "A", false); id != "h1" || err != nil {
		t.Errorf("the same person re-running init must keep the host: %q %v", id, err)
	}
	if _, err := host.ReEnrolID(stored, "B", false); !errors.Is(err, host.ErrOtherOwner) {
		t.Errorf("another person must be refused without --new: %v", err)
	}
	if id, err := host.ReEnrolID(stored, "B", true); id != "" || err != nil {
		t.Errorf("--new enrols afresh: %q %v", id, err)
	}
}

func TestCredentials_Validate(t *testing.T) {
	ok := host.Credentials{
		Deployment:    host.Deployment{ServerURL: "s", IssuerURL: "i", ClientID: "c"},
		TokenEndpoint: "t", HostID: "h", OwnerSub: "A", RefreshToken: "r",
	}
	if err := ok.Validate(); err != nil {
		t.Fatal(err)
	}
	bad := ok
	bad.RefreshToken = ""
	if err := bad.Validate(); err == nil || !strings.Contains(err.Error(), "refresh_token") {
		t.Errorf("missing refresh token: %v", err)
	}
	if strings.Contains(bad.Validate().Error(), "r\"") {
		t.Error("the error must name fields, never values")
	}
}
