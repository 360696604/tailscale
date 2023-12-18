// Copyright (c) Tailscale Inc & AUTHORS
// SPDX-License-Identifier: BSD-3-Clause

// package pathutil provides utility functions for working with URL paths.
package pathutil

import "strings"

func Split(path string) []string {
	return strings.Split(strings.Trim(path, "/"), "/")
}
