// Copyright (c) Tailscale Inc & AUTHORS
// SPDX-License-Identifier: BSD-3-Clause

package compositefs

import (
	"context"
	"io/fs"
	"os"

	"golang.org/x/net/webdav"
	"tailscale.com/tailfs/shared"
	"tailscale.com/util/pathutil"
)

func (cfs *compositeFileSystem) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (webdav.File, error) {
	if pathutil.IsRoot(name) {
		// the root directory contains one directory for each child
		return &shared.DirFile{
			Info: shared.ReadOnlyDirInfo(name),
			LoadChildren: func() ([]fs.FileInfo, error) {
				cfs.childrenMu.Lock()
				defer cfs.childrenMu.Unlock()
				children := make([]fs.FileInfo, 0, len(cfs.children))
				for _, c := range cfs.children {
					children = append(children, shared.ReadOnlyDirInfo(c.name))
				}
				return children, nil
			},
		}, nil
	}

	path, onChild, child, err := cfs.pathToChild(name)
	if err != nil {
		return nil, err
	}

	if !onChild {
		// this is the child itself, ask it to open its root
		return child.fs.OpenFile(ctx, "/", flag, perm)
	}

	return child.fs.OpenFile(ctx, path, flag, perm)
}
