package postgres

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/appleboy/docker-backup-database/pkg/helper"
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
		options, err := helper.SplitArgs(d.Opts)
		if err != nil {
			return err
		}

		flags = append(flags, options...)
	}

	if d.Name != "" {
		flags = append(flags, d.Name)
	}

	if d.Password != "" {
		envs = append(envs, "PGPASSWORD="+d.Password)
	}

	return helper.WriteOutputToFileWithGzip(ctx, "pg_dump", flags, envs, d.DumpName)
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
