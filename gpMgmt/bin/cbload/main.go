package main

import (
	"github.com/cloudberrydb/cbload/loader"
	"github.com/spf13/cobra"
)

func main() {
	l := loader.NewLoader()

	var rootCmd = &cobra.Command{
		Use:     "cbload",
		Short:   "load file(s) into Cloudberry Database",
		Version: l.GetVersion(),
		Run:     l.Run,
	}

	l.Initialize(rootCmd)
	rootCmd.Execute()
}
