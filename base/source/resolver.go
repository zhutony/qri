package source

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/qri-io/qfs"
)

// QFSResolver is a link resolver baked by a qfs.Filesystem
type QFSResolver struct {
	FS qfs.Filesystem
}

// Get fetches content for a given address
func (r QFSResolver) Get(ctx context.Context, path string) (val interface{}, err error) {
	f, err := r.FS.Get(ctx, path)
	if err != nil {
		return nil, err
	}

	fmt.Println("media type:", f.MediaType())
	switch f.MediaType() {
	case "application/json":
		var v interface{}
		err = json.NewDecoder(f).Decode(&v)
		return v, err
	}
	return
}
