// Copyright 2024 Block, Inc.

package dbconn

import (
	"context"
	"errors"
	"testing"

	"github.com/cashapp/blip"
)

type testPasswordSecret struct {
	payload []byte
	err     error
}

func (s testPasswordSecret) GetSecretPayload(context.Context) ([]byte, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.payload, nil
}

func TestPasswordSecretCredentialFuncDefaultParser(t *testing.T) {
	f := factory{}
	credentialFunc := f.passwordSecretCredentialFunc(
		blip.ConfigMonitor{Username: "config-user"},
		testPasswordSecret{payload: []byte(`{"password":"secret-pass"}`)},
	)

	creds, err := credentialFunc(context.Background())
	if err != nil {
		t.Fatalf("got error %s, expected nil", err)
	}
	if creds.Username != "config-user" {
		t.Errorf("Username=%q, expected config-user", creds.Username)
	}
	if creds.Password != "secret-pass" {
		t.Errorf("Password=%q, expected secret-pass", creds.Password)
	}
}

func TestPasswordSecretCredentialFuncCustomParser(t *testing.T) {
	called := false
	f := factory{
		passwordSecretParser: func(_ context.Context, cfg blip.ConfigMonitor, payload []byte, secret *blip.Secret) error {
			called = true
			if cfg.Username != "config-user" {
				t.Errorf("cfg.Username=%q, expected config-user", cfg.Username)
			}
			if secret.Username != "config-user" {
				t.Errorf("pre-populated Username=%q, expected config-user", secret.Username)
			}
			if string(payload) != "secret-user:secret-pass" {
				t.Errorf("payload=%q, expected secret-user:secret-pass", string(payload))
			}
			secret.Username = "secret-user"
			secret.Password = "secret-pass"
			return nil
		},
	}
	credentialFunc := f.passwordSecretCredentialFunc(
		blip.ConfigMonitor{Username: "config-user"},
		testPasswordSecret{payload: []byte("secret-user:secret-pass")},
	)

	creds, err := credentialFunc(context.Background())
	if err != nil {
		t.Fatalf("got error %s, expected nil", err)
	}
	if !called {
		t.Fatal("custom parser was not called")
	}
	if creds.Username != "secret-user" {
		t.Errorf("Username=%q, expected secret-user", creds.Username)
	}
	if creds.Password != "secret-pass" {
		t.Errorf("Password=%q, expected secret-pass", creds.Password)
	}
}

func TestPasswordSecretCredentialFuncParserError(t *testing.T) {
	parseErr := errors.New("parse secret")
	f := factory{
		passwordSecretParser: func(context.Context, blip.ConfigMonitor, []byte, *blip.Secret) error {
			return parseErr
		},
	}
	credentialFunc := f.passwordSecretCredentialFunc(
		blip.ConfigMonitor{Username: "config-user"},
		testPasswordSecret{payload: []byte("secret-pass")},
	)

	_, err := credentialFunc(context.Background())
	if !errors.Is(err, parseErr) {
		t.Fatalf("got error %v, expected %v", err, parseErr)
	}
}

func TestPasswordSecretCredentialFuncGetSecretError(t *testing.T) {
	getErr := errors.New("get secret")
	f := factory{}
	credentialFunc := f.passwordSecretCredentialFunc(
		blip.ConfigMonitor{Username: "config-user"},
		testPasswordSecret{err: getErr},
	)

	_, err := credentialFunc(context.Background())
	if !errors.Is(err, getErr) {
		t.Fatalf("got error %v, expected %v", err, getErr)
	}
}
