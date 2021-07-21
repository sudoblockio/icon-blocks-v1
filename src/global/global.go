package global

// Version - service version
const Version = "v0.1.0"

// ShutdownChan - channel to start shutdown
var ShutdownChan = make(chan int)
