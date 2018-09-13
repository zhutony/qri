package actions

import (
	"fmt"

	"github.com/qri-io/qri/p2p"
	"github.com/qri-io/qri/repo"
)

// DatasetHead gets commit, structure, meta, viz & transform for a given reference, either
// from the local repo or by asking peers for it, modifying the input ref on success
func DatasetHead(node *p2p.QriNode, ds *repo.DatasetRef) error {
	err := repo.CanonicalizeDatasetRef(node.Repo, ds)
	if err != nil && err != repo.ErrNotFound {
		log.Debug(err.Error())
		return err
	}
	if err == repo.ErrNotFound {
		if node == nil {
			return fmt.Errorf("%s, and no p2p connection", err.Error())
		}
		return node.RequestDataset(ds)
	}

	return ReadDataset(node.Repo, ds)
}
