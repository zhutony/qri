package source

import (
	"fmt"

	"github.com/qri-io/dataset"
	"github.com/qri-io/value"
)

// Commit is a wrapper for a dataset meta component to support the map complex
// type
type Commit dataset.Commit

// ValueForKey is a generic field getter
func (cm *Commit) ValueForKey(k interface{}) (val interface{}, err error) {
	key, ok := k.(string)
	if !ok {
		return nil, fmt.Errorf("source: %v is not a string", k)
	}

	switch key {
	case "Author":
		return cm.Author, nil
	case "Message":
		return cm.Message, nil
	case "Path":
		return cm.Path, nil
	case "Qri":
		return cm.Qri, nil
	case "Signature":
		return cm.Signature, nil
	case "Timestamp":
		return cm.Timestamp, nil
	case "Title":
		return cm.Title, nil
	}

	return nil, fmt.Errorf("key not found: %s", key)
}

// Iterate is a nil shim for now
func (cm *Commit) Iterate() value.Iterator {
	return nil
}

// Meta is a wrapper for a dataset meta component to support the value interface
type Meta dataset.Meta

var _ value.Map = (*Meta)(nil)

// ValueForKey is a generic field getter
func (md *Meta) ValueForKey(k interface{}) (val interface{}, err error) {
	key, ok := k.(string)
	if !ok {
		return nil, fmt.Errorf("source: %v is not a string", k)
	}

	switch key {
	case "accessURL":
		return md.AccessURL, nil
	case "accrualPeriodicity":
		return md.AccrualPeriodicity, nil
	case "citations":
		return md.Citations, nil
	case "contributors":
		return md.Contributors, nil
	case "description":
		return md.Description, nil
	case "downloadURL":
		return Link(md.DownloadURL), nil
	case "homeURL":
		return Link(md.HomeURL), nil
	case "identifier":
		return md.Identifier, nil
	case "keywords":
		return md.Keywords, nil
	case "language":
		return md.Language, nil
	case "license":
		return md.License, nil
	case "readmeURL":
		return md.ReadmeURL, nil
	case "title":
		return md.Title, nil
	case "theme":
		return md.Theme, nil
	case "version":
		return md.Version, nil
	}

	return nil, fmt.Errorf("key not found: %s", key)
}

// Iterate is a nil shim for now
func (md *Meta) Iterate() value.Iterator {
	return nil
}

// Viz is a wrapper for a dataset viz component to support the value interface
type Viz dataset.Viz

var _ value.Map = (*Viz)(nil)

// ValueForKey returns the given
func (vz *Viz) ValueForKey(k interface{}) (val interface{}, err error) {
	key, ok := k.(string)
	if !ok {
		return nil, fmt.Errorf("source: %v is not a string", k)
	}

	switch key {
	case "format":
		return vz.Format, nil
	case "script":
		v := dataset.Viz(*vz)
		fmt.Printf("%#v\n", v.ScriptFile())
		return v.ScriptFile(), nil
	}

	return nil, fmt.Errorf("key not found: %s", key)
}

// Iterate is a nil shim for now
func (vz *Viz) Iterate() value.Iterator {
	return nil
}
