package postgres

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)

// Dump provides dump execution arguments.
type Dump struct {
	Host     string
	Username string
	Password string
	Name     string
	Opts     string
	DumpName string
}

func getHostPort(h string) (string, string) {
	data := strings.Split(h, ":")
	host := data[0]
	port := "5432"
	if len(data) > 1 {
		port = data[1]
	}

	return host, port
}

// Exec for dump command
func (d Dump) Exec(ctx context.Context) error {
	envs := os.Environ()

	// Print the version number for the command line tools
	cmd := exec.CommandContext(ctx, "pg_dump", "--version")
	cmd.Env = envs
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	trace(cmd)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to get pg_dump version: %w", err)
	}

	flags := []string{}
	host, port := getHostPort(d.Host)
	if host != "" {
		flags = append(flags, "-h", host)
	}
	if port != "" {
		flags = append(flags, "-p", port)
	}

	if d.Username != "" {
		flags = append(flags, "-U", d.Username)
	}

	if d.Opts != "" {
		flags = append(flags, d.Opts)
	}

	if d.Name != "" {
		flags = append(flags, d.Name)
	}

	if d.Password != "" {
		envs = append(envs, "PGPASSWORD="+d.Password)
	}

	prog := "pg_dump"

	cmd = exec.CommandContext(ctx, prog, flags...)
	cmd.Env = envs

	r, w := io.Pipe()
	cmd.Stdout = w
	cmd.Stderr = os.Stderr

	f, err := os.Create(d.DumpName)
	if err != nil {
		return fmt.Errorf("failed to create dump output file: %w", err)
	}
	defer f.Close()

	trace(cmd)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start %s: %w", prog, err)
	}

	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("%s failed: %w", prog, err)
	}

	if _, err := io.Copy(gzip.NewWriter(f), r); err != nil {
		return fmt.Errorf("failed to write dump to file: %w", err)
	}

	return nil
}

// trace prints the command to the stdout.
func trace(cmd *exec.Cmd) {
	fmt.Printf("$ %s\n", strings.Join(cmd.Args, " "))
}

// NewEngine struct
func NewEngine(host, username, password, name, dumpName, opts string) *Dump {
	return &Dump{
		Host:     host,
		Username: username,
		Password: password,
		Name:     name,
		Opts:     opts,
		DumpName: dumpName,
	}
}
