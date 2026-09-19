// draft-projection-study emits or verifies the synthetic v1 projection study.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/aminkbi/d-raft/internal/projectionstudy"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, out io.Writer) error {
	flags := flag.NewFlagSet("draft-projection-study", flag.ContinueOnError)
	verify := flags.String("verify", "", "regenerate and verify a published result")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments")
	}
	if *verify != "" {
		file, err := os.Open(*verify)
		if err != nil {
			return err
		}
		defer file.Close()
		raw, err := io.ReadAll(io.LimitReader(file, 1<<20))
		if err != nil {
			return err
		}
		if err := projectionstudy.Verify(raw); err != nil {
			return err
		}
		_, err = fmt.Fprintln(out, "verified", *verify)
		return err
	}
	report, err := projectionstudy.Run()
	if err != nil {
		return err
	}
	raw, err := projectionstudy.Encode(report)
	if err != nil {
		return err
	}
	_, err = out.Write(raw)
	return err
}
