package source

import (
	"fmt"
	"reflect"

	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsio"
	"github.com/qri-io/value"
)

// Dataset implements dataset as a traversable linked object
type Dataset dataset.Dataset

var _ value.Map = (*Dataset)(nil)

// ValueForKey returns the given
func (ds *Dataset) ValueForKey(k interface{}) (val interface{}, err error) {
	key, ok := k.(string)
	if !ok {
		return nil, fmt.Errorf("source: %v is not a string", k)
	}

	switch key {
	case "body":
		d := &dataset.Dataset{}
		*d = dataset.Dataset(*ds)
		if d.BodyFile() != nil && d.Structure != nil {
			er, err := dsio.NewEntryReader(d.Structure, d.BodyFile())
			if err != nil {
				return nil, err
			}

			return &Iterator{rdr: er}, nil
		}
		return ds.Body, nil
	case "schema":
		if ds.Structure != nil {
			return ds.Structure.Schema, nil
		}
		return nil, nil
	case "bodyBytes":
		return ds.BodyBytes, nil
	case "bodyPath":
		return ds.BodyPath, nil
	case "commit":
		cm := Commit(*ds.Commit)
		return &cm, nil
	case "meta":
		md := Meta(*ds.Meta)
		return &md, nil
	case "name":
		return ds.Name, nil
	case "path":
		return Link(ds.Path), nil
	case "peername":
		return ds.Peername, nil
	case "previousPath":
		return Link(ds.PreviousPath + "/dataset.json"), nil
	case "profileID":
		return ds.ProfileID, nil
	case "numVersions":
		return ds.NumVersions, nil
	case "qri":
		return ds.Qri, nil
	case "structure":
		return ds.Structure, nil
	case "transform":
		return ds.Transform, nil
	case "viz":
		v := Viz(*ds.Viz)
		return &v, nil
	default:
		return nil, fmt.Errorf("key not found: %s", key)
	}
}

// Iterate is a nil shim for now
func (ds *Dataset) Iterate() value.Iterator {
	return nil
}

// Iterator is an iterator created from a dsio.EntryReader
type Iterator struct {
	i   int
	rdr dsio.EntryReader
	ent dsio.Entry
	err error
}

var _ value.Iterator = (*Iterator)(nil)

// Next returns the next value in an entry
func (it *Iterator) Next() bool {
	it.i++
	if it.err != nil {
		return false
	}

	it.ent, it.err = it.rdr.ReadEntry()
	return true
}

// Key returns the current key value for the iterator
func (it *Iterator) Key() interface{} {
	return it.ent.Key
}

// Scan copies the values in the current iteration into the values pointed at by dest
func (it *Iterator) Scan(dest interface{}) error {
	v := reflect.ValueOf(dest)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if !v.CanSet() {
		return fmt.Errorf("expected pointer value for scan")
	}
	v.Set(reflect.ValueOf(it.ent.Value))
	return it.err
}

// IsOrdered returns true if there is a total order to the iterator
func (it *Iterator) IsOrdered() bool {
	return true
}

// Close closes the reader
func (it *Iterator) Close() error {
	return it.rdr.Close()
}

// ValueForIndex returns the value at a given index
func (it *Iterator) ValueForIndex(i int) (v interface{}, err error) {
	defer it.rdr.Close()

	for {
		ent, err := it.rdr.ReadEntry()
		if err != nil {
			return nil, err
		}
		if it.i == i {
			return ent.Value, nil
		}
		it.i++
	}
}

// Link represents a link to something
type Link string

// Path is the path this link points to
func (l Link) Path() string { return string(l) }
