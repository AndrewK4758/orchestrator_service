package logger

import (
	"fmt"
	"log"
)

const (
	ColorReset   = "\033[0m"
	ColorRed     = "\033[31m"
	ColorGreen   = "\033[32m"
	ColorYellow  = "\033[33m"
	ColorBlue    = "\033[34m"
	ColorMagenta = "\033[35m"
	ColorCyan    = "\033[36m"
	ColorWhite   = "\033[37m"
)

func Info(component string, format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	log.Printf("%sINFO: [%s]%s %s", ColorCyan, component, ColorReset, msg)
}

func Cache(component string, format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	log.Printf("%sCACHE: [%s]%s %s", ColorBlue, component, ColorReset, msg)
}

func Warn(component string, format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	log.Printf("%sWARN: [%s]%s %s", ColorYellow, component, ColorReset, msg)
}

func Error(component string, format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	log.Printf("%sERROR: [%s]%s %s", ColorRed, component, ColorReset, msg)
}

func Debug(component string, format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	log.Printf("%sDEBUG: [%s]%s %s", ColorMagenta, component, ColorReset, msg)
}

func Success(component string, format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	log.Printf("%sSUCCESS: [%s]%s %s", ColorGreen, component, ColorReset, msg)
}
