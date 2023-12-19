// Copyright (c) Tailscale Inc & AUTHORS
// SPDX-License-Identifier: BSD-3-Clause

package compositefs

import (
	"context"
	"os"

	"tailscale.com/util/pathutil"
)

func (cfs *compositeFileSystem) Mkdir(ctx context.Context, name string, perm os.FileMode) error {
	if pathutil.IsRoot(name) {
		// root directory already exists, consider this okay
		return nil
	}

	path, onChild, child, err := cfs.pathToChild(name)
	if !onChild {
		// children can't be made
		return nil
	}

	if err != nil {
		return err
	}

	return child.fs.Mkdir(ctx, path, perm)
}
