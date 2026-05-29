package cli

import (
	"fmt"
	"os"

	"github.com/logrusorgru/aurora"
)

var Verbose = false

func Show(format string, a ...any) {
	fmt.Print(fmt.Sprintf(format+"\n", a...))
}

func Bold(format string, a ...any) {
	fmt.Print(aurora.Bold(fmt.Sprintf(format+"\n", a...)))
}

func Success(format string, a ...any) {
	fmt.Print(aurora.Green(fmt.Sprintf(format+"\n", a...)))
}

func Error(format string, a ...any) {
	fmt.Print(aurora.Red(fmt.Sprintf(format+"\n", a...)))
}

func Debug(format string, a ...any) {
	if Verbose {
		fmt.Print(fmt.Sprintf("[DEBUG] "+format+"\n", a...))
	}
}

func Exit(err error) {
	if err != nil {
		fmt.Println("ERROR:", err)
		os.Exit(1)
	} else {
		os.Exit(0)
	}
}