// Copyright (c) Tailscale Inc & AUTHORS
// SPDX-License-Identifier: BSD-3-Clause

// package pathutil provides utility functions for working with URL paths.
package pathutil

import "strings"

const (
	sepString = "/"
	sep       = '/'
)

func Split(path string) []string {
	return strings.Split(strings.Trim(path, sepString), sepString)
}

func IsRoot(path string) bool {
	return len(path) == 0 || len(path) == 1 && path[0] == sep
}
