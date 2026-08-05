package cache_client

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewClient_PanicOnInvalidOrMissingMaxMsgSizeMb(t *testing.T) {
	origVal, exists := os.LookupEnv("GRPC_MAX_MESSAGE_SIZE_MB")
	defer func() {
		if exists {
			_ = os.Setenv("GRPC_MAX_MESSAGE_SIZE_MB", origVal)
		} else {
			_ = os.Unsetenv("GRPC_MAX_MESSAGE_SIZE_MB")
		}
	}()

	t.Run("MissingEnvVar_Panics", func(t *testing.T) {
		_ = os.Unsetenv("GRPC_MAX_MESSAGE_SIZE_MB")
		assert.Panics(t, func() {
			_, _, _ = NewClient("localhost:12345")
		})
	})

	t.Run("ZeroMaxMsgSize_Panics", func(t *testing.T) {
		_ = os.Setenv("GRPC_MAX_MESSAGE_SIZE_MB", "0")
		assert.PanicsWithError(t,
			"[Context: cache_client/NewCacheClient] Invalid maxMsgSizeMb: 0, must be > 0",
			func() {
				_, _, _ = NewClient("localhost:12345")
			},
		)
	})

	t.Run("NegativeMaxMsgSize_Panics", func(t *testing.T) {
		_ = os.Setenv("GRPC_MAX_MESSAGE_SIZE_MB", "-5")
		assert.PanicsWithError(t,
			"[Context: cache_client/NewCacheClient] Invalid maxMsgSizeMb: -5, must be > 0",
			func() {
				_, _, _ = NewClient("localhost:12345")
			},
		)
	})

	t.Run("ValidMaxMsgSize_Succeeds", func(t *testing.T) {
		_ = os.Setenv("GRPC_MAX_MESSAGE_SIZE_MB", "4")
		client, conn, err := NewClient("localhost:12345")
		assert.NoError(t, err)
		assert.NotNil(t, client)
		assert.NotNil(t, conn)
		_ = conn.Close()
	})
}
