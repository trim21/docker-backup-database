package helper

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"golang.org/x/sync/errgroup"
)

// Pipeline strings together the given exec.Cmd commands in a similar fashion
// to the Unix pipeline.  Each command's standard output is connected to the
// standard input of the next command, and the output of the final command in
// the pipeline is returned, along with the collected standard error of all
// commands and the first error found (if any).
//
// To provide input to the pipeline, assign an io.Reader to the first's Stdin.
// ref: https://gist.github.com/kylelemons/1525278
func Pipeline(cmds ...*exec.Cmd) (pipeLineOutput, collectedStandardError []byte, pipeLineError error) {
	// Require at least one command
	if len(cmds) < 1 {
		return nil, nil, nil
	}

	// Collect the output from the command(s)
	var output bytes.Buffer
	var stderr bytes.Buffer

	last := len(cmds) - 1
	for i, cmd := range cmds[:last] {
		var err error
		// Connect each command's stdin to the previous command's stdout
		if cmds[i+1].Stdin, err = cmd.StdoutPipe(); err != nil {
			return nil, nil, err
		}
		// Connect each command's stderr to a buffer
		cmd.Stderr = &stderr
	}

	// Connect the output and error for the last command
	cmds[last].Stdout, cmds[last].Stderr = &output, &stderr

	// Start each command
	for _, cmd := range cmds {
		if err := cmd.Start(); err != nil {
			return output.Bytes(), stderr.Bytes(), err
		}
	}

	// Wait for each command to complete
	for _, cmd := range cmds {
		if err := cmd.Wait(); err != nil {
			return output.Bytes(), stderr.Bytes(), err
		}
	}

	// Return the pipeline output and the collected standard error
	return output.Bytes(), stderr.Bytes(), nil
}

func WriteOutputToFileWithGzip(ctx context.Context, cmd string, args []string, envs []string, outputFile string) error {
	c := exec.CommandContext(ctx, cmd, args...)
	c.Env = envs

	r, w := io.Pipe()
	c.Stdout = w
	c.Stderr = os.Stderr

	f, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create dump output file: %w", err)
	}
	defer f.Close()

	// Create a gzip writer wrapping the output file
	gzWriter := gzip.NewWriter(f)
	defer gzWriter.Close()

	trace(c)
	// Start the command
	if err := c.Start(); err != nil {
		w.Close()
		return fmt.Errorf("failed to start command: %w", err)
	}
	// Use errgroup to manage goroutines and errors
	var eg errgroup.Group

	// Copy command output to gzip writer in a goroutine
	eg.Go(func() error {
		_, err := io.Copy(gzWriter, r)
		return err
	})

	// Start a goroutine to wait for the command
	eg.Go(func() error {
		cmdErr := c.Wait()
		w.Close() // Close the writer to signal EOF to the reader
		return cmdErr
	})

	// Wait for all goroutines to finish
	if err := eg.Wait(); err != nil {
		return err
	}

	return nil
}

// trace prints the command to the stdout.
func trace(cmd *exec.Cmd) {
	fmt.Printf("$ %s\n", strings.Join(cmd.Args, " "))
}
