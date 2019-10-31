package dsfs

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	crypto "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/multiformats/go-multihash"
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsio"
	"github.com/qri-io/dataset/dsviz"
	"github.com/qri-io/dataset/validate"
	"github.com/qri-io/jsonschema"
	"github.com/qri-io/qfs"
	"github.com/qri-io/qfs/cafs"
	"github.com/qri-io/qri/base/dsdiff"
	"github.com/qri-io/qri/base/fill"
	"github.com/qri-io/value"
)

// LoadDatasetValue fetches dataset contents as a qri runtime value
func LoadDatasetValue(ctx context.Context, fs qfs.Filesystem, path string) (value.Value, error) {
	if strings.HasPrefix(path, "/ipfs/") {
		return nil, fmt.Errorf("can't load qri values from legacy datasets")
	}

	f, err := fs.Get(ctx, path)
	if err != nil {
		return nil, err
	}
	return f.Value(), nil
}

// LoadDataset reads a dataset from a cafs and dereferences structure, transform, and commitMsg if they exist,
// returning a fully-hydrated dataset
func LoadDataset(ctx context.Context, fs qfs.Filesystem, path string) (*dataset.Dataset, error) {
	log.Debugf("LoadDataset: '%s'", path)
	if strings.HasPrefix(path, "/ipfs/") {
		// legacy code support
		if store, ok := fs.(cafs.Filestore); ok {
			return loadIPFSDataset(ctx, store, path)
		}
		return nil, fmt.Errorf("passed in filesystem isn't a cafs.Filestore")
	}

	f, err := fs.Get(ctx, path)
	if err != nil {
		return nil, err
	}

	return datasetFromFile(ctx, fs, f)
}

func loadIPFSDataset(ctx context.Context, store cafs.Filestore, path string) (*dataset.Dataset, error) {
	ds, err := LoadDatasetRefs(ctx, store, path)
	if err != nil {
		log.Debug(err.Error())
		return nil, fmt.Errorf("error loading dataset: %s", err.Error())
	}
	if err := DerefDataset(ctx, store, ds); err != nil {
		log.Debug(err.Error())
		return nil, err
	}
	return ds, nil
}

// LoadDatasetRefs reads a dataset from a content addressed filesystem without dereferencing
// it's components
func LoadDatasetRefs(ctx context.Context, store cafs.Filestore, path string) (*dataset.Dataset, error) {
	ds := dataset.NewDatasetRef(path)

	pathWithBasename := PackageFilepath(store, path, PackageFileDataset)
	data, err := fileBytes(store.Get(ctx, pathWithBasename))
	// if err != nil {
	// 	return nil, fmt.Errorf("error getting file bytes: %s", err.Error())
	// }

	// TODO - for some reason files are sometimes coming back empty from IPFS,
	// every now & then. In the meantime, let's give a second try if data is empty
	if err != nil || len(data) == 0 {
		data, err = fileBytes(store.Get(ctx, pathWithBasename))
		if err != nil {
			log.Debug(err.Error())
			return nil, fmt.Errorf("error getting file bytes: %s", err.Error())
		}
	}

	ds, err = dataset.UnmarshalDataset(data)
	if err != nil {
		log.Debug(err.Error())
		return nil, fmt.Errorf("error unmarshaling %s file: %s", PackageFileDataset.String(), err.Error())
	}

	// assign path to retain internal reference to the
	// path this dataset was read from
	ds.Assign(dataset.NewDatasetRef(path))

	return ds, nil
}

// DerefDataset attempts to fully dereference a dataset
func DerefDataset(ctx context.Context, store cafs.Filestore, ds *dataset.Dataset) error {
	if err := DerefDatasetMeta(ctx, store, ds); err != nil {
		return err
	}
	if err := DerefDatasetStructure(ctx, store, ds); err != nil {
		return err
	}
	if err := DerefDatasetTransform(ctx, store, ds); err != nil {
		return err
	}
	if err := DerefDatasetViz(ctx, store, ds); err != nil {
		return err
	}
	if err := DerefDatasetReadme(ctx, store, ds); err != nil {
		return err
	}
	return DerefDatasetCommit(ctx, store, ds)
}

// DerefDatasetStructure derferences a dataset's structure element if required
// should be a no-op if ds.Structure is nil or isn't a reference
func DerefDatasetStructure(ctx context.Context, store cafs.Filestore, ds *dataset.Dataset) error {
	if ds.Structure != nil && ds.Structure.IsEmpty() && ds.Structure.Path != "" {
		st, err := loadStructure(ctx, store, ds.Structure.Path)
		if err != nil {
			log.Debug(err.Error())
			return fmt.Errorf("error loading dataset structure: %s", err.Error())
		}
		// assign path to retain internal reference to path
		// st.Assign(dataset.NewStructureRef(ds.Structure.Path))
		ds.Structure = st
	}
	return nil
}

// DerefDatasetViz dereferences a dataset's Viz element if required
// should be a no-op if ds.Viz is nil or isn't a reference
func DerefDatasetViz(ctx context.Context, store cafs.Filestore, ds *dataset.Dataset) error {
	if ds.Viz != nil && ds.Viz.IsEmpty() && ds.Viz.Path != "" {
		vz, err := loadViz(ctx, store, ds.Viz.Path)
		if err != nil {
			log.Debug(err.Error())
			return fmt.Errorf("error loading dataset viz: %s", err.Error())
		}
		// assign path to retain internal reference to path
		// vz.Assign(dataset.NewVizRef(ds.Viz.Path))
		ds.Viz = vz
	}
	return nil
}

// DerefDatasetReadme dereferences a dataset's Readme element if required
// should be a no-op if ds.Readme is nil or isn't a reference
func DerefDatasetReadme(ctx context.Context, store cafs.Filestore, ds *dataset.Dataset) error {
	if ds.Readme != nil && ds.Readme.IsEmpty() && ds.Readme.Path != "" {
		rm, err := loadReadme(ctx, store, ds.Readme.Path)
		if err != nil {
			log.Debug(err.Error())
			return fmt.Errorf("error loading dataset readme: %s", err.Error())
		}
		// assign path to retain internal reference to path
		// rm.Assign(dataset.NewVizRef(ds.Readme.Path))
		ds.Readme = rm
	}
	return nil
}

// DerefDatasetTransform derferences a dataset's transform element if required
// should be a no-op if ds.Structure is nil or isn't a reference
func DerefDatasetTransform(ctx context.Context, store cafs.Filestore, ds *dataset.Dataset) error {
	if ds.Transform != nil && ds.Transform.IsEmpty() && ds.Transform.Path != "" {
		t, err := loadTransform(ctx, store, ds.Transform.Path)
		if err != nil {
			log.Debug(err.Error())
			return fmt.Errorf("error loading dataset transform: %s", err.Error())
		}
		// assign path to retain internal reference to path
		// t.Assign(dataset.NewTransformRef(ds.Transform.Path))
		ds.Transform = t
	}
	return nil
}

// DerefDatasetMeta derferences a dataset's transform element if required
// should be a no-op if ds.Structure is nil or isn't a reference
func DerefDatasetMeta(ctx context.Context, store cafs.Filestore, ds *dataset.Dataset) error {
	if ds.Meta != nil && ds.Meta.IsEmpty() && ds.Meta.Path != "" {
		md, err := loadMeta(ctx, store, ds.Meta.Path)
		if err != nil {
			log.Debug(err.Error())
			return fmt.Errorf("error loading dataset metadata: %s", err.Error())
		}
		// assign path to retain internal reference to path
		// md.Assign(dataset.NewMetaRef(ds.Meta.Path))
		ds.Meta = md
	}
	return nil
}

// DerefDatasetCommit derferences a dataset's Commit element if required
// should be a no-op if ds.Structure is nil or isn't a reference
func DerefDatasetCommit(ctx context.Context, store cafs.Filestore, ds *dataset.Dataset) error {
	if ds.Commit != nil && ds.Commit.IsEmpty() && ds.Commit.Path != "" {
		cm, err := loadCommit(ctx, store, ds.Commit.Path)
		if err != nil {
			log.Debug(err.Error())
			return fmt.Errorf("error loading dataset commit: %s", err.Error())
		}
		// assign path to retain internal reference to path
		cm.Assign(dataset.NewCommitRef(ds.Commit.Path))
		ds.Commit = cm
	}
	return nil
}

// CreateDataset places a new dataset in the store. Admittedly, this isn't a simple process.
// Store is where we're going to
// Dataset to be saved
// Pin the dataset if the underlying store supports the pinning interface
// All streaming files (Body, Transform Script, Viz Script) Must be Resolved before calling if data their data is to be saved
func CreateDataset(ctx context.Context, store cafs.Filestore, ds, dsPrev *dataset.Dataset, pk crypto.PrivKey, pin, force, shouldRender bool) (path string, err error) {

	if pk == nil {
		err = fmt.Errorf("private key is required to create a dataset")
		return
	}
	if err = DerefDataset(ctx, store, ds); err != nil {
		log.Debug(err.Error())
		return
	}
	if err = validate.Dataset(ds); err != nil {
		log.Debug(err.Error())
		return
	}

	if dsPrev != nil && !dsPrev.IsEmpty() {
		if err = DerefDataset(ctx, store, dsPrev); err != nil {
			log.Debug(err.Error())
			return
		}
		if err = validate.Dataset(dsPrev); err != nil {
			log.Debug(err.Error())
			return
		}
	}
	_, err = prepareDataset(store, ds, dsPrev, pk, force, shouldRender)
	if err != nil {
		log.Debug(err.Error())
		return
	}

	path, err = WriteDataset(ctx, store, ds)
	if err != nil {
		log.Debug(err.Error())
		err = fmt.Errorf("error writing dataset: %s", err.Error())
	}
	return
}

// Timestamp is an function for getting commit timestamps
// timestamps MUST be stored in UTC time zone
var Timestamp = func() time.Time {
	return time.Now().UTC()
}

// prepareDataset modifies a dataset in preparation for adding to a dsfs
// it returns a new data file for use in WriteDataset
func prepareDataset(store cafs.Filestore, ds, dsPrev *dataset.Dataset, privKey crypto.PrivKey, force, shouldRender bool) (string, error) {
	var (
		err error
		// lock for parallel edits to ds pointer
		mu sync.Mutex
		// accumulate reader into a buffer for shasum calculation & passing out another qfs.File
		buf    bytes.Buffer
		bf     = ds.BodyFile()
		bfPrev qfs.File
	)

	if dsPrev != nil {
		bfPrev = dsPrev.BodyFile()
	}

	if bf == nil && bfPrev == nil {
		return "", fmt.Errorf("bodyfile or previous bodyfile needed")
	}

	if bf == nil {
		bf = bfPrev
	}

	errR, errW := io.Pipe()
	entryR, entryW := io.Pipe()
	hashR, hashW := io.Pipe()
	done := make(chan error)
	tasks := 3
	valChan := make(chan []jsonschema.ValError)

	go setErrCount(ds, qfs.NewMemfileReader(bf.FileName(), errR), &mu, done, valChan)
	go setDepthAndEntryCount(ds, qfs.NewMemfileReader(bf.FileName(), entryR), &mu, done)
	go setChecksumAndLength(ds, qfs.NewMemfileReader(bf.FileName(), hashR), &buf, &mu, done)

	go func() {
		// pipes must be manually closed to trigger EOF
		defer errW.Close()
		defer entryW.Close()
		defer hashW.Close()

		// allocate a multiwriter that writes to each pipe when
		// mw.Write() is called
		mw := io.MultiWriter(errW, entryW, hashW)
		// copy file bytes to multiwriter from input file
		io.Copy(mw, bf)
	}()

	// Get validation errors because trying to join the main tasks.
	var validationErrors []jsonschema.ValError
	validationErrors = <-valChan

	// Join the outstanding tasks, wait until all are cmoplete.
	for i := 0; i < tasks; i++ {
		if err := <-done; err != nil {
			return "", err
		}
	}

	// If in strict mode, fail if there were any errors.
	if ds.Structure.Strict && ds.Structure.ErrCount > 0 {
		fmt.Fprintf(os.Stderr, "\nShowing errors at each /row/column of the dataset body:\n")
		for i, v := range validationErrors {
			fmt.Fprintf(os.Stderr, "%d) %v\n", i, v)
		}
		return "", fmt.Errorf("strict mode: dataset body did not validate against its schema")
	}

	// TODO (ramfox): This whole section can be wrapped:
	// func generateCommit(ds, prev *dataset.Dataset, privKey crypto.PrivKey) error
	// Lots of stuff happening in prepareDataset and the steps to creating the
	// proper commit can be abstracted out
	diffDescription, err := generateCommitMsg(ds, dsPrev, force)
	if err != nil {
		log.Debug(fmt.Errorf("error saving: %s", err))
		return "", fmt.Errorf("error saving: %s", err)
	}

	cleanTitleAndMessage(&ds.Commit.Title, &ds.Commit.Message, diffDescription)

	// TODO (b5): we should check the delta between versions for meaninful changes here,
	// ignoring fields we know will change every time. Can only do this with a proper set
	// of change deltas

	ds.Commit.Timestamp = Timestamp()
	sb, _ := ds.SignableBytes()
	signedBytes, err := privKey.Sign(sb)
	if err != nil {
		log.Debug(err.Error())
		return "", fmt.Errorf("error signing commit title: %s", err.Error())
	}
	ds.Commit.Signature = base64.StdEncoding.EncodeToString(signedBytes)
	ds.SetBodyFile(qfs.NewMemfileBytes("body."+ds.Structure.Format, buf.Bytes()))

	if shouldRender && ds.Viz != nil && ds.Viz.ScriptFile() != nil {
		// render the viz
		renderedFile, err := dsviz.Render(ds)
		if err != nil {
			log.Debug(err.Error())
			return "", fmt.Errorf("error rendering visualization: %s", err.Error())
		}
		ds.Viz.SetRenderedFile(renderedFile)
	}

	return diffDescription, nil
}

// setErrCount consumes sets the ErrCount field of a dataset's Structure
func setErrCount(ds *dataset.Dataset, data qfs.File, mu *sync.Mutex, done chan error, valChan chan []jsonschema.ValError) {
	defer data.Close()

	er, err := dsio.NewEntryReader(ds.Structure, data)
	if err != nil {
		log.Debug(err.Error())
		valChan <- nil
		done <- fmt.Errorf("reading data values: %s", err.Error())
		return
	}

	// Send validation errors immediately, before main thread blocks.
	validationErrors, err := validate.EntryReader(er)
	valChan <- validationErrors

	if err != nil {
		log.Debug(err.Error())
		done <- fmt.Errorf("validating data: %s", err.Error())
		return
	}

	mu.Lock()
	ds.Structure.ErrCount = len(validationErrors)
	mu.Unlock()

	done <- nil
}

// setDepthAndEntryCount set the Entries field of a ds.Structure
func setDepthAndEntryCount(ds *dataset.Dataset, data qfs.File, mu *sync.Mutex, done chan error) {
	defer data.Close()
	er, err := dsio.NewEntryReader(ds.Structure, data)
	if err != nil {
		log.Debug(err.Error())
		done <- fmt.Errorf("error reading data values: %s", err.Error())
		return
	}

	entries := 0

	depth := 0
	var ent dsio.Entry
	for {
		if ent, err = er.ReadEntry(); err != nil {
			log.Debug(err.Error())
			break
		}
		// get the depth of this entry, update depth if larger
		if d := getDepth(ent.Value); d > depth {
			depth = d
		}
		entries++
	}
	if err.Error() != "EOF" {
		done <- fmt.Errorf("error reading values at entry %d: %s", entries, err.Error())
		return
	}

	mu.Lock()
	ds.Structure.Entries = entries
	ds.Structure.Depth = depth + 1 // need to add one for the original enclosure
	mu.Unlock()

	done <- nil
}

// getDepth finds the deepest value in a given interface value
func getDepth(x interface{}) (depth int) {
	switch v := x.(type) {
	case map[string]interface{}:
		for _, el := range v {
			if d := getDepth(el); d > depth {
				depth = d
			}
		}
		return depth + 1
	case []interface{}:
		for _, el := range v {
			if d := getDepth(el); d > depth {
				depth = d
			}
		}
		return depth + 1
	default:
		return depth
	}
}

// setChecksumAndLength
func setChecksumAndLength(ds *dataset.Dataset, data qfs.File, buf *bytes.Buffer, mu *sync.Mutex, done chan error) {
	defer data.Close()

	if _, err := io.Copy(buf, data); err != nil {
		done <- err
		return
	}

	shasum, err := multihash.Sum(buf.Bytes(), multihash.SHA2_256, -1)
	if err != nil {
		log.Debug(err.Error())
		done <- fmt.Errorf("error calculating hash: %s", err.Error())
		return
	}

	mu.Lock()
	ds.Structure.Checksum = shasum.B58String()
	ds.Structure.Length = len(buf.Bytes())
	mu.Unlock()

	done <- nil
}

// returns a commit message based on the diff of the two datasets
// if there is no previous dataset, it returns "created dataset"
// if there is no difference, the func returns an error
func generateCommitMsg(ds, prev *dataset.Dataset, force bool) (string, error) {
	if prev == nil || prev.IsEmpty() {
		return "created dataset", nil
	}

	diffMap, err := dsdiff.DiffDatasets(prev, ds, nil)
	if err != nil {
		err = fmt.Errorf("error diffing datasets: %s", err.Error())
		return "", err
	}

	diffDescription, err := dsdiff.MapDiffsToString(diffMap, "listKeys")
	if err != nil {
		return "", err
	}

	if diffDescription == "" {
		if force {
			return "forced update", nil
		}
		return "", fmt.Errorf("no changes detected")
	}

	return diffDescription, nil
}

// cleanTitleAndMessage adjusts the title to include no more
// than 70 characters and no more than one line.  Text following
// a line break or this limit will be prepended to the message
func cleanTitleAndMessage(sTitle, sMsg *string, diffDescription string) {
	st := *sTitle
	sm := *sMsg
	if st == "" && diffDescription != "" {
		st = diffDescription
	} else if st == "" {
		// if title is *still* blank move pass message up to title
		st = sm
		sm = ""
	}
	//adjust for length
	if len(st) > 70 {
		cutIndex := 66
		lastSpaceIndex := strings.LastIndex(st[:67], " ")
		if lastSpaceIndex > 0 {
			cutIndex = lastSpaceIndex + 1
		}
		if sm == "" {
			sm = fmt.Sprintf("...%s", st[cutIndex:])
		} else {
			sm = fmt.Sprintf("...%s\n%s", st[cutIndex:], sm)
		}
		st = fmt.Sprintf("%s...", st[:cutIndex])
	}
	// adjust for line breaks
	newlineIndex := strings.Index(st, "\n")
	if newlineIndex > 0 {
		sm = fmt.Sprintf("%s\n%s", st[newlineIndex+1:], sm)
		st = st[:newlineIndex]

	}
	*sTitle = st
	*sMsg = sm
}

// WriteDataset writes a dataset to a cafs, replacing subcomponents of a dataset with path references
func WriteDataset(ctx context.Context, fs qfs.Filesystem, ds *dataset.Dataset) (path string, err error) {
	peername, name := ds.Peername, ds.Name
	file, err := PrepareIPFSFile(ds)
	if err != nil {
		return "", err
	}

	if path, err = fs.Put(ctx, file); err != nil {
		return "", err
	}

	ds.Peername = peername
	ds.Name = name
	ds.Path = path
	return path, nil
}

func datasetFromFile(ctx context.Context, fs qfs.Filesystem, f qfs.File) (ds *dataset.Dataset, err error) {
	ds = &dataset.Dataset{}
	if dsv, ok := f.Value().(map[interface{}]interface{}); ok {
		// process meta
		if mdl, ok := dsv["meta"].(value.Link); ok {
			mdv, err := fs.Resolve(ctx, mdl)
			if err != nil {
				return nil, err
			}
			if mdm, ok := mdv.(map[interface{}]interface{}); ok {
				ds.Meta = &dataset.Meta{}
				if err := fill.Struct(toStringMap(mdm), ds.Meta); err != nil {
					return nil, err
				}
			}
		}
	}

	return ds, nil
}

// PrepareIPFSFile creates a graph for writing to IPFS.
// modifies the passed-in dataset
// TODO (b5) - this should be refactored to work with the component API
func PrepareIPFSFile(ds *dataset.Dataset) (qfs.File, error) {
	ds.DropTransientValues()
	prepped := map[string]interface{}{}

	if ds.Commit != nil {
		prepped["commit"] = map[string]interface{}{
			"title":   ds.Commit.Title,
			"message": ds.Commit.Message,
		}
	}
	if ds.Meta != nil {
		prepped["meta"] = value.NewResolvedLink("meta", ds.Meta)
	}
	if ds.Transform != nil {
		// TODO (b5) - components should have a Value function that performs this
		prepped["transform"] = map[string]interface{}{
			"syntax": ds.Transform.Syntax,
			"script": ds.Transform.ScriptFile(),
		}
	}
	if ds.Readme != nil {
		prepped["readme"] = map[string]interface{}{
			"format": ds.Readme.Format,
			"script": ds.Readme.ScriptFile(),
		}
	}
	if ds.Structure != nil {
		prepped["structure"] = map[string]interface{}{
			"formatConfig": ds.Structure.FormatConfig,
			"schema":       value.NewResolvedLink("schema", ds.Structure.Schema),
		}
	}
	if ds.BodyFile() != nil {
		prepped["body"] = ds.BodyFile()
	}

	return qfs.NewMemfile("dataset", prepped), nil
}

// toStringMap checks for map[interface{}]interface{}, converts keys to strings
func toStringMap(msi map[interface{}]interface{}) (res map[string]interface{}) {
	res = map[string]interface{}{}
	for key, val := range msi {
		res[fmt.Sprintf("%s", key)] = val
	}
	return res
}

func imString(m map[interface{}]interface{}, key string) string {
	if v, ok := m[key]; ok {
		if str, ok := v.(string); ok {
			return str
		}
	}
	return ""
}
