// Copyright (c) Tailscale Inc & AUTHORS
// SPDX-License-Identifier: BSD-3-Clause

// package compositefs provides a webdav.FileSystem that is composi
package compositefs

import (
	"io"
	"os"
	"path"
	"reflect"
	"sort"
	"sync"

	"golang.org/x/net/webdav"
	"tailscale.com/types/logger"
	"tailscale.com/util/pathutil"
)

// child represents a child filesystem
type child struct {
	name string
	fs   webdav.FileSystem
}

// childrenByName is a slice of *child sorted in name order
type childrenByName []*child

func (children childrenByName) Len() int           { return len(children) }
func (children childrenByName) Swap(i, j int)      { children[i], children[j] = children[j], children[i] }
func (children childrenByName) Less(i, j int) bool { return children[i].name < children[j].name }

// CompositeFileSystem is a webdav.FileSystem that is composed of multiple
// child webdav.FileSystems. Each child is identified by a name and appears
// as a folder within the root of the CompositeFileSystem, with the children
// sorted alphabetically by name.
//
// Children in a CompositeFileSystem can only be added or removed via calls to
// the CompositeFileSystem's SDK methods. From a file system perspective, the
// root of the CompositeFileSystem acts as read-only, not permitting the
// addition, removal or renaming of folders.
//
// Rename is only supported within a single child. Renaming across children
// is not supported, as it wouldn't be possible to perform it atomically.
type CompositeFileSystem interface {
	webdav.FileSystem

	// AddChild ads a single child with the given name, replacing any existing
	// child with the same name.
	AddChild(name string, fs webdav.FileSystem)
	// RemoveChild removes the child with the given name, if it exists.
	RemoveChild(name string)
	// SetChildren replaces the entire existing set of children with the given
	// ones.
	SetChildren(map[string]webdav.FileSystem)
	// GetChild returns the child with the given name and a boolean indicating
	// whether or not it was found.
	GetChild(name string) (webdav.FileSystem, bool)
}

// New constructs a CompositeFileSystem that logs using the given logf,
// optionally initialized with one or more children.
func New(logf logger.Logf, children ...*child) CompositeFileSystem {
	fs := &compositeFileSystem{
		logf:        logf,
		children:    childrenByName(children),
		childrenMap: make(map[string]*child, len(children)),
	}
	sort.Sort(fs.children)
	for _, c := range children {
		fs.childrenMap[c.name] = c
	}
	return fs
}

type compositeFileSystem struct {
	logf        logger.Logf
	children    childrenByName
	childrenMap map[string]*child
	childrenMu  sync.Mutex
}

func (cfs *compositeFileSystem) AddChild(name string, childFS webdav.FileSystem) {
	c := &child{
		name: name,
		fs:   childFS,
	}

	cfs.childrenMu.Lock()
	defer cfs.childrenMu.Unlock()
	cfs.childrenMap[name] = c
	cfs.rebuildChildren()
}

func (cfs *compositeFileSystem) RemoveChild(name string) {
	cfs.childrenMu.Lock()
	defer cfs.childrenMu.Unlock()
	delete(cfs.childrenMap, name)
	cfs.rebuildChildren()
}

func (cfs *compositeFileSystem) SetChildren(children map[string]webdav.FileSystem) {
	newChildrenMap := make(map[string]*child, len(cfs.children))
	for name, childFS := range children {
		newChildrenMap[name] = &child{
			name: name,
			fs:   childFS,
		}
	}
	cfs.childrenMu.Lock()
	oldChildren := cfs.children
	cfs.childrenMap = newChildrenMap
	cfs.rebuildChildren()
	cfs.childrenMu.Unlock()
	for _, child := range oldChildren {
		cfs.logf("maybe close child of type %v", reflect.TypeOf(child.fs))
		closer, ok := child.fs.(io.Closer)
		if ok {
			_ = closer.Close()
		}
	}
}

func (cfs *compositeFileSystem) GetChild(name string) (webdav.FileSystem, bool) {
	cfs.childrenMu.Lock()
	defer cfs.childrenMu.Unlock()

	child, ok := cfs.childrenMap[name]
	if !ok {
		return nil, false
	}
	return child.fs, true
}

func (cfs *compositeFileSystem) rebuildChildren() {
	cfs.children = make(childrenByName, 0, len(cfs.childrenMap))
	for _, c := range cfs.childrenMap {
		cfs.children = append(cfs.children, c)
	}
}

// pathToChild takes the given name and determines if the path is on a child
// filesystem based on the first path component. If it is, this returns the
// remainder of the path minus the first path component, true, and the
// corresponding child. If it is not, this returns the original name, false,
// and a nil *child.
//
// If the first path component identifies an unknown child, this will return
// os.ErrNotExist.
func (cfs *compositeFileSystem) pathToChild(name string) (string, bool, *child, error) {
	pathComponents := pathutil.Split(name)
	cfs.childrenMu.Lock()
	child, childFound := cfs.childrenMap[pathComponents[0]]
	cfs.childrenMu.Unlock()
	if !childFound {
		return name, false, nil, os.ErrNotExist
	}

	switch len(pathComponents) {
	case 1:
		return name, false, child, nil
	default:
		return path.Join(pathComponents[1:]...), true, child, nil
	}
}
