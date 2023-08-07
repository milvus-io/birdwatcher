package common

// ExitErr is the error indicates user needs to exit application.
var ExitErr = exitErr{}

// exitErr internal err type for comparing.
type exitErr struct{}

// Error implements error.
func (e exitErr) Error() string {
	return "exited"
}
