package log

import "log"

const (
	DEBUG = "debug"
	INFO  = "info"
)

const (
	pattern = DEBUG
)

func Printf(format string, args ...interface{}) {
	if pattern == INFO {
		log.Printf(format, args...)
	}
}
