// Copyright 2024 Block, Inc.

package blip_test

import (
	"context"
	"testing"

	"github.com/cashapp/blip"
)

func TestDefaultPasswordSecretParser(t *testing.T) {
	tests := []struct {
		name           string
		payload        []byte
		expectUsername string
		expectPassword string
		expectErr      bool
	}{
		{
			name:           "username and password",
			payload:        []byte(`{"username":"secret-user","password":"secret-pass"}`),
			expectUsername: "secret-user",
			expectPassword: "secret-pass",
		},
		{
			name:           "password only",
			payload:        []byte(`{"password":"secret-pass"}`),
			expectUsername: "config-user",
			expectPassword: "secret-pass",
		},
		{
			name:           "non-string username falls back to config",
			payload:        []byte(`{"username":123,"password":"secret-pass"}`),
			expectUsername: "config-user",
			expectPassword: "secret-pass",
		},
		{
			name:      "missing password",
			payload:   []byte(`{"username":"secret-user"}`),
			expectErr: true,
		},
		{
			name:      "non-string password",
			payload:   []byte(`{"username":"secret-user","password":123}`),
			expectErr: true,
		},
		{
			name:      "malformed JSON",
			payload:   []byte(`{`),
			expectErr: true,
		},
		{
			name:      "null literal",
			payload:   []byte(`null`),
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := blip.ConfigMonitor{Username: "config-user"}
			secret := blip.Secret{}
			err := blip.DefaultPasswordSecretParser(context.Background(), cfg, tt.payload, &secret)
			if tt.expectErr {
				if err == nil {
					t.Fatal("got nil error, expected non-nil error")
				}
				return
			}
			if err != nil {
				t.Fatalf("got error %s, expected nil", err)
			}
			if secret.Username != tt.expectUsername {
				t.Errorf("Username=%q, expected %q", secret.Username, tt.expectUsername)
			}
			if secret.Password != tt.expectPassword {
				t.Errorf("Password=%q, expected %q", secret.Password, tt.expectPassword)
			}
		})
	}
}

func TestDefaultPasswordSecretParserNilSecret(t *testing.T) {
	err := blip.DefaultPasswordSecretParser(context.Background(), blip.ConfigMonitor{}, []byte(`{}`), nil)
	if err == nil {
		t.Fatal("got nil error, expected non-nil error")
	}
}
