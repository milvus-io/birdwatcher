package oss

import (
	"io"
	"os"
	"testing"
)

func TestNewMinioClientKeepsStdoutClean(t *testing.T) {
	readPipe, writePipe, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe() error = %v", err)
	}
	oldStdout := os.Stdout
	os.Stdout = writePipe
	defer func() {
		os.Stdout = oldStdout
		_ = readPipe.Close()
		_ = writePipe.Close()
	}()

	param := MinioClientParam{
		Addr:          "localhost:9000",
		CloudProvider: CloudProviderAWS,
		BucketName:    "test-bucket",
	}
	WithSkipCheckBucket(true)(&param)
	if _, err := NewMinioClient(t.Context(), param); err != nil {
		t.Fatalf("NewMinioClient() error = %v", err)
	}
	if err := writePipe.Close(); err != nil {
		t.Fatalf("close stdout capture: %v", err)
	}
	os.Stdout = oldStdout

	output, err := io.ReadAll(readPipe)
	if err != nil {
		t.Fatalf("read stdout capture: %v", err)
	}
	if len(output) != 0 {
		t.Fatalf("NewMinioClient() stdout = %q, want empty", output)
	}
}
