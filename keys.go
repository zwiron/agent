package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"

	"github.com/zwiron/pkg/logger"
)

const keyBits = 4096

// KeyPair holds the agent's RSA key pair.
type KeyPair struct {
	Private   *rsa.PrivateKey
	PublicPEM string
}

// LoadOrGenerateKeys loads an existing RSA private key from path, or generates
// a new 4096-bit key pair and saves it.
func LoadOrGenerateKeys(path string, log *logger.Logger) (*KeyPair, error) {
	if _, err := os.Stat(path); err == nil {
		return loadKeys(path)
	}

	log.Info(nil, "agent.keys.generating", "bits", keyBits, "path", path)

	priv, err := rsa.GenerateKey(rand.Reader, keyBits)
	if err != nil {
		return nil, fmt.Errorf("generate RSA key: %w", err)
	}

	privBytes := x509.MarshalPKCS1PrivateKey(priv)
	privPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: privBytes})

	if err := os.WriteFile(path, privPEM, 0600); err != nil {
		return nil, fmt.Errorf("write private key: %w", err)
	}

	return keyPairFromPrivate(priv)
}

func loadKeys(path string) (*KeyPair, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read private key: %w", err)
	}

	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("failed to decode PEM block from %s", path)
	}

	priv, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse private key: %w", err)
	}

	return keyPairFromPrivate(priv)
}

func keyPairFromPrivate(priv *rsa.PrivateKey) (*KeyPair, error) {
	pubBytes, err := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("marshal public key: %w", err)
	}

	pubPEM := string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubBytes}))

	return &KeyPair{
		Private:   priv,
		PublicPEM: pubPEM,
	}, nil
}
