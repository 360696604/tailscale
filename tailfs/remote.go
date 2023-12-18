// Copyright (c) Tailscale Inc & AUTHORS
// SPDX-License-Identifier: BSD-3-Clause

package tailfs

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/tailscale/gowebdav"
	"golang.org/x/net/webdav"
	"tailscale.com/safesocket"
	"tailscale.com/tailfs/compositefs"
	"tailscale.com/tailfs/webdavfs"
	"tailscale.com/types/logger"
	"tailscale.com/util/pathutil"
)

// Share represents a folder that's shared with remote Tailfs nodes.
type Share struct {
	// Name is how this share appears on remote nodes.
	Name string `json:"name"`
	// Path is the path to the directory on this machine that's being shared.
	Path string `json:"path"`
	// As is the UNIX or Windows username of the local account used for this
	// share. File read/write permissions are enforced based on this username.
	As string `json:"who"`
	// Readers is a list of Tailscale principals that are allowed to read this
	// share.
	Readers []string `json:"readers,omitempty"`
	// Writers is a list of Tailscale principals that are allowed to write to
	// this share.
	Writers []string `json:"writers,omitempty"`
}

// AllowedShares is a map of allowed share names.
type AllowedShares map[string]bool

func (s AllowedShares) allowed(name string) bool {
	return s[name] || s["*"]
}

// ForRemote is the TailFS filesystem exposed to remote nodes. It provides a
// unified WebDAV interface to local directories that have been shared.
type ForRemote interface {
	// SetFileServerAddr sets the address of the file server to which we
	// should proxy. This is used on platforms like Windows and MacOS
	// sandboxed where we can't spawn user-specific sub-processes and instead
	// rely on the UI application that's already running as an unprivileged
	// user to access the filesystem for us.
	SetFileServerAddr(addr string)

	// SetShares sets the complete set of shares exposed by this node. If
	// AllowShareAs() is true, we will use one subprocess per user to access
	// the filesystem (see userServer). Otherwise, we will use the file server
	// configured via SetFileServerAddr.
	SetShares(shares map[string]*Share)

	// ServeHTTP behaves like the similar method from http.Handler but also
	// accepts a Permissions map that captures the permissions of the connecting
	// node.
	ServeHTTP(permissions Permissions, w http.ResponseWriter, r *http.Request)

	// Close() stops serving the WebDAV content
	Close() error
}

func NewFileSystemForRemote(logf logger.Logf) ForRemote {
	fs := &fileSystemForRemote{
		logf:        logf,
		lockSystem:  webdav.NewMemLS(),
		userServers: make(map[string]*userServer),
	}
	return fs
}

type fileSystemForRemote struct {
	logf           logger.Logf
	lockSystem     webdav.LockSystem
	fileServerAddr string
	shares         map[string]*Share
	userServers    map[string]*userServer
	mx             sync.RWMutex
}

func (s *fileSystemForRemote) SetFileServerAddr(addr string) {
	s.mx.Lock()
	s.fileServerAddr = addr
	s.mx.Unlock()
}

func (s *fileSystemForRemote) SetShares(shares map[string]*Share) {
	if !AllowShareAs() {
		s.mx.Lock()
		s.shares = shares
		s.mx.Unlock()
		return
	}

	// set up one server per user
	userServers := make(map[string]*userServer)
	for _, share := range shares {
		p, found := userServers[share.As]
		if !found {
			p = &userServer{
				logf: s.logf,
			}
			userServers[share.As] = p
		}
		p.shares = append(p.shares, share)
	}
	for _, p := range userServers {
		go p.runLoop()
	}
	s.mx.Lock()
	s.shares = shares
	oldUserServers := s.userServers
	s.userServers = userServers
	s.mx.Unlock()

	// stop old user servers
	for _, server := range oldUserServers {
		if err := server.Close(); err != nil {
			s.logf("error closing old tailfs user server: %v", err)
		}
	}
}

func (s *fileSystemForRemote) ServeHTTP(permissions Permissions, w http.ResponseWriter, r *http.Request) {
	isWrite := writeMethods[r.Method]
	if isWrite {
		share := pathutil.Split(r.URL.Path)[0]
		switch permissions.For(share) {
		case PermissionNone:
			// If we have no permissions to this share, treat it as not found
			// to avoid leaking any information abou the share's existence.
			http.Error(w, "not found", http.StatusNotFound)
			return
		case PermissionReadOnly:
			http.Error(w, "permission denied", http.StatusForbidden)
			return
		}
	}

	s.mx.RLock()
	sharesMap := s.shares
	userServers := s.userServers
	fileServerAddr := s.fileServerAddr
	s.mx.RUnlock()

	children := make(map[string]webdav.FileSystem, len(sharesMap))
	for _, share := range sharesMap {
		// exclude shares to which the connecting principal has no access
		if permissions.For(share.Name) == PermissionNone {
			continue
		}
		var addr string
		if !AllowShareAs() {
			addr = fileServerAddr
		} else {
			userServer, found := userServers[share.As]
			if found {
				userServer.mx.RLock()
				addr = userServer.addr
				userServer.mx.RUnlock()
			}
		}

		if addr == "" {
			s.logf("no server found for user %v, skipping share %v", share.As, share.Name)
			continue
		}

		children[share.Name] = webdavfs.New(&webdavfs.Opts{
			Client: gowebdav.New(&gowebdav.Opts{
				URI: fmt.Sprintf("http://safesocket/%v", share.Name),
				Transport: &http.Transport{
					Dial: func(_, _ string) (net.Conn, error) {
						_, err := netip.ParseAddrPort(addr)
						if err == nil {
							// this is a regular network address, dial normally
							return net.Dial("tcp", addr)
						}
						// assume this is a safesocket address
						return safesocket.Connect(safesocket.DefaultConnectionStrategy(addr))
					},
				},
			}),
			Logf: s.logf,
		})
	}
	cfs := compositefs.New(s.logf)
	cfs.SetChildren(children)
	h := webdav.Handler{
		FileSystem: cfs,
		LockSystem: s.lockSystem,
	}
	h.ServeHTTP(w, r)
}

func (s *fileSystemForRemote) Close() error {
	s.mx.Lock()
	oldUserServers := s.userServers
	s.mx.Unlock()

	for _, server := range oldUserServers {
		if err := server.Close(); err != nil {
			s.logf("error closing old tailfs user server: %v", err)
		}
	}

	return nil
}

// userServer runs tailscaled serve-tailfs to serve webdav content for the
// given Shares. All Shares are assumed to have the same Share.As, and the
// content is served as that Share.As user.
type userServer struct {
	logf   logger.Logf
	shares []*Share
	closed bool
	cmd    *exec.Cmd
	addr   string
	mx     sync.RWMutex
}

func (s *userServer) Close() error {
	s.mx.Lock()
	cmd := s.cmd
	s.closed = true
	s.mx.Unlock()
	if cmd != nil && cmd.Process != nil {
		return cmd.Process.Kill()
	}
	// not running, that's okay
	return nil
}

func (s *userServer) runLoop() {
	executable, err := os.Executable()
	if err != nil {
		s.logf("can't find executable: %v", err)
		return
	}
	for {
		s.mx.RLock()
		closed := s.closed
		s.mx.RUnlock()
		if closed {
			return
		}

		err := s.run(executable)
		s.logf("user server % v stopped with error %v, will start again", executable, err)
		// TODO(oxtoacart): maybe be smarter about backing off here
		time.Sleep(1 * time.Second)
	}
}

// Run runs the executable (tailscaled). This function only works on UNIX systems,
// but those are the only ones on which we use userServers anyway.
func (s *userServer) run(executable string) error {
	// set up the command
	args := []string{"serve-tailfs"}
	for _, s := range s.shares {
		args = append(args, s.Name, s.Path)
	}
	allArgs := []string{"-u", s.shares[0].As, executable}
	allArgs = append(allArgs, args...)
	cmd := exec.Command("sudo", allArgs...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("stdout pipe: %w", err)
	}
	defer stdout.Close()
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("stderr pipe: %w", err)
	}
	defer stderr.Close()

	err = cmd.Start()
	if err != nil {
		return fmt.Errorf("start: %w", err)
	}
	s.mx.Lock()
	s.cmd = cmd
	s.mx.Unlock()

	// read address
	stdoutScanner := bufio.NewScanner(stdout)
	stdoutScanner.Scan()
	if stdoutScanner.Err() != nil {
		return fmt.Errorf("read addr: %w", stdoutScanner.Err())
	}
	addr := stdoutScanner.Text()
	// send the rest of stdout and stderr to logger to avoid blocking
	go func() {
		for stdoutScanner.Scan() {
			s.logf("tailscaled serve-tailfs stdout: %v", stdoutScanner.Text())
		}
	}()
	stderrScanner := bufio.NewScanner(stderr)
	go func() {
		for stderrScanner.Scan() {
			s.logf("tailscaled serve-tailfs stderr: %v", stderrScanner.Text())
		}
	}()
	s.mx.Lock()
	s.addr = strings.TrimSpace(addr)
	s.mx.Unlock()
	return cmd.Wait()
}

var writeMethods = map[string]bool{
	"PUT":       true,
	"POST":      true,
	"COPY":      true,
	"LOCK":      true,
	"UNLOCK":    true,
	"MKCOL":     true,
	"MOVE":      true,
	"PROPPATCH": true,
}
