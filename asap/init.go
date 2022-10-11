package asap

import "os"

func init() {
	os.Setenv("GOLANG_PROTOBUF_REGISTRATION_CONFLICT", "ignore")
}
