package base

import (
	"encoding/json"
	"fmt"

	"github.com/ghodss/yaml"
	"github.com/qri-io/dataset"
)

// Encode serializes data to a given format
func Encode(format string, cfg dataset.FormatConfig, val interface{}) ([]byte, error) {
	switch format {
	case "json":
		if imap, ok := val.(map[interface{}]interface{}); ok {
			val = ensureMapsHaveStringKeys(imap)
		}

		// Pretty defaults to true for the dataset head, unless explicitly set in the config.
		pretty := true
		if cfg != nil {
			pvalue, ok := cfg.Map()["pretty"].(bool)
			if ok {
				pretty = pvalue
			}
		}
		if pretty {
			return json.MarshalIndent(val, "", " ")
		} else {
			return json.Marshal(val)
		}
	case "yaml", "":
		return yaml.Marshal(val)
	default:
		return nil, fmt.Errorf("unknown format: \"%s\"", format)
	}
}

// ensureMapsHaveStringKeys will recursively convert map's key to be strings. This will allow us
// to serialize back into JSON.
// TODO (b5) - this is copied from the fil package
func ensureMapsHaveStringKeys(imap map[interface{}]interface{}) map[string]interface{} {
	build := make(map[string]interface{})
	for k, v := range imap {
		switch x := v.(type) {
		case map[interface{}]interface{}:
			v = ensureMapsHaveStringKeys(x)
		case []interface{}:
			for i, elem := range x {
				if inner, ok := elem.(map[interface{}]interface{}); ok {
					x[i] = ensureMapsHaveStringKeys(inner)
				}
			}
		}
		build[fmt.Sprintf("%s", k)] = v
	}
	return build
}
