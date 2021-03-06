package lib

import (
	"bytes"
	"context"
	"fmt"

	"github.com/qri-io/qri/sql"
)

// SQLMethods encapsulates business logic for the qri search command
// TODO (b5): switch to using an Instance instead of separate fields
type SQLMethods struct {
	inst *Instance
}

// NewSQLMethods creates SQLMethods from a qri Instance
func NewSQLMethods(inst *Instance) *SQLMethods {
	return &SQLMethods{inst: inst}
}

// CoreRequestsName implements the requests interface
func (m SQLMethods) CoreRequestsName() string { return "sql" }

// SQLQueryParams defines paremeters for the exec Method
// ExecParams provides parameters to the execute command
type SQLQueryParams struct {
	Query        string
	OutputFormat string
	ResolverMode string
}

// Exec runs an SQL query
func (m *SQLMethods) Exec(p *SQLQueryParams, results *[]byte) error {
	if m.inst.rpc != nil {
		return checkRPCError(m.inst.rpc.Call("SQLMethods.Exec", p, results))
	}
	if p == nil {
		return fmt.Errorf("error: search params cannot be nil")
	}
	ctx := context.TODO()

	resolver, err := m.inst.resolverForMode(p.ResolverMode)
	if err != nil {
		return err
	}
	// create a loader sql will use to load & fetch datasets
	// pass in the configured peername, allowing the "me" alias in reference strings
	loadDataset := NewParseResolveLoadFunc(m.inst.cfg.Profile.Peername, resolver, m.inst)
	svc := sql.New(m.inst.repo, loadDataset)

	buf := &bytes.Buffer{}
	if err := svc.Exec(ctx, buf, p.OutputFormat, p.Query); err != nil {
		return err
	}

	*results = buf.Bytes()
	return nil
}
