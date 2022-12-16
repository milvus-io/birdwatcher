package common

import (
	"errors"
	"strconv"
	"strings"
)

func PathPartInt64(p string, idx int) (int64, error) {
	part, err := PathPart(p, idx)
	if err != nil {
		return 0, err
	}
	v, err := strconv.ParseInt(part, 10, 64)
	if err != nil {
		return 0, err
	}
	return v, nil
}

func PathPart(p string, idx int) (string, error) {
	parts := strings.Split(p, "/")
	// -1 means last part
	if idx < 0 {
		idx = len(parts) + idx
	}
	if idx < 0 && idx >= len(parts) {
		return "", errors.New("out of index")
	}
	return parts[idx], nil
}
