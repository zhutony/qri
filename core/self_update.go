package core

import (
	"context"
	"fmt"

	namesys "gx/ipfs/QmViBzgruNUoLNBnXcx8YWbDNwV8MNGEGKkLo6JGetygdw/go-ipfs/namesys"
)

/*
* self-updating checks IPNS entry that represents the desired hash of the previous version of
* this program, which is the result of adding the complied binary of this program to IPFS.
* If the returned value of a lookup differs, we have a version mismatch, and need to perform
* an update.
*
* In the future we'll do this automatically, but for now we can at least warn users that they need
* to update their version
 */

// previousVersion is a hard-coded reference the gx "lastpubver" file of the previous release
const previousVersion = "/ipfs/QmcXZCLAgUdvXpt1fszjNGVGn6WnhsrJahkQXY3JJqxUWJ"

// prevIPNSName is the dnslink address to check for version agreement
const prevIPNSName = "/ipns/cli.previous.qri.io"

// ErrUpdateRequired means this version of qri is out of date
var ErrUpdateRequired = fmt.Errorf("update required")

// CheckVersion uses a name resolver to lookup prevIPNSName, checking if the hard-coded previousVersion
// and the returned lookup match. If they don't, CheckVersion returns ErrUpdateRequired
func CheckVersion(ctx context.Context, res namesys.Resolver) error {
	p, err := res.Resolve(ctx, prevIPNSName)
	if err != nil {
		return fmt.Errorf("error resolving name: %s", err.Error())
	}

	if p.String() != previousVersion {
		return ErrUpdateRequired
	}
	return nil
}
