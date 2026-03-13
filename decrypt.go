package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"github.com/zwiron/connector"
)

// DecryptConfig decrypts an RSA-OAEP-SHA256 encrypted config blob using
// the agent's private key and returns a connector.Config map.
func DecryptConfig(priv *rsa.PrivateKey, ciphertext []byte) (connector.Config, error) {
	plaintext, err := rsa.DecryptOAEP(sha256.New(), rand.Reader, priv, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt config: %w", err)
	}

	var cfg connector.Config
	if err := json.Unmarshal(plaintext, &cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	return cfg, nil
}
