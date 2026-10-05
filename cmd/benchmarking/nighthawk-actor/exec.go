// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"errors"
	"io"
	"net"
	"os/exec"
	"syscall"
	"time"
)

// execRunner starts real child processes.
type execRunner struct{}

func (execRunner) Start(name string, args []string, out io.Writer) (process, error) {
	cmd := exec.Command(name, args...)
	cmd.Stdout = out
	cmd.Stderr = out
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	p := &execProcess{cmd: cmd, done: make(chan struct{})}
	go func() {
		err := cmd.Wait()
		p.code = cmd.ProcessState.ExitCode()
		var exitErr *exec.ExitError
		if err != nil && !errors.As(err, &exitErr) {
			p.err = err
		}
		close(p.done)
	}()
	return p, nil
}

type execProcess struct {
	cmd  *exec.Cmd
	done chan struct{}
	// code and err are written once before done is closed.
	code int
	err  error
}

func (p *execProcess) Wait() (int, error) {
	<-p.done
	return p.code, p.err
}

func (p *execProcess) Stop(grace time.Duration) {
	select {
	case <-p.done:
		return
	default:
	}
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	t := time.NewTimer(grace)
	defer t.Stop()
	select {
	case <-p.done:
	case <-t.C:
		_ = p.cmd.Process.Kill()
		<-p.done
	}
}

// dialReady polls addr until a TCP connection succeeds or ctx ends.
func dialReady(ctx context.Context, addr string) error {
	var d net.Dialer
	for {
		conn, err := d.DialContext(ctx, "tcp", addr)
		if err == nil {
			return conn.Close()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}
