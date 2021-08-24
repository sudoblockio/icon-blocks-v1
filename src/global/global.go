package global

import (
	"os"
	"os/signal"
	"syscall"
)

// Version - service version
const Version = "v0.1.0"

// WaitShutdownSig - wait for system shutdown signal
func WaitShutdownSig() {
	// Listen for close sig
	// Register for interupt (Ctrl+C) and SIGTERM (docker)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan
}
