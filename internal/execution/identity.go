package execution

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

type Execution struct {
	StepID       string
	Payload      string
	ModelVersion string
	Hash         string
}

func GenerateHash(stepID, payload, modelVersion string) string {
	hash := sha256.New()

	fmt.Fprintf(hash, "%s\x00%s\x00%s", stepID, payload, modelVersion)

	return hex.EncodeToString(hash.Sum(nil))
}

func NewExecution(stepID, payload, modelVersion string) *Execution {
	return &Execution{
		StepID:       stepID,
		Payload:      payload,
		ModelVersion: modelVersion,
		Hash:         GenerateHash(stepID, payload, modelVersion),
	}
}
