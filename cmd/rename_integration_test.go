package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/qri-io/qri/dsref"
)

// Test rename works if dataset has no history
func TestRenameNoHistory(t *testing.T) {
	run := NewFSITestRunner(t, "qri_test_rename_no_history")
	defer run.Delete()

	workDir := run.CreateAndChdirToWorkDir("remove_no_history")

	// Init as a linked directory
	run.MustExec(t, "qri init --name remove_no_history --format csv")

	// Read .qri-ref file, it contains the reference this directory is linked to
	actual := run.MustReadFile(t, filepath.Join(workDir, ".qri-ref"))
	expect := "test_peer/remove_no_history"
	if diff := cmp.Diff(expect, actual); diff != "" {
		t.Errorf("qri list (-want +got):\n%s", diff)
	}

	// Go up one directory
	parentDir := filepath.Dir(workDir)
	os.Chdir(parentDir)

	// Rename
	run.MustExec(t, "qri rename me/remove_no_history me/remove_second_name")

	// Old dataset name can't be used
	err := run.ExecCommand("qri get me/remove_no_history")
	if err == nil {
		t.Error("expected error, did not get one")
	}
	expect = "reference not found"
	if err.Error() != expect {
		t.Errorf("error mismatch, expect: %s, got: %s", expect, err.Error())
	}

	// New dataset name can be used
	err = run.ExecCommand("qri get me/remove_second_name")
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	// Read .qri-ref file, it contains the new reference name
	actual = run.MustReadFile(t, filepath.Join(workDir, ".qri-ref"))
	expect = "test_peer/remove_second_name"
	if diff := cmp.Diff(expect, actual); diff != "" {
		t.Errorf("qri list (-want +got):\n%s", diff)
	}

	// Test that `qri list` will only show the new ref. Still linked to the old directory name.
	output := run.MustExec(t, "qri list")
	expect = `1   test_peer/remove_second_name
    linked: /tmp/remove_no_history
    0 B, 0 entries, 0 errors

`
	if diff := cmp.Diff(expect, output); diff != "" {
		t.Errorf("qri list (-want +got):\n%s", diff)
	}
}

// Test rename updates the qri-ref link
func TestRenameUpdatesLink(t *testing.T) {
	run := NewFSITestRunner(t, "qri_test_rename_update_link")
	defer run.Delete()

	workDir := run.CreateAndChdirToWorkDir("remove_update_link")

	// Init as a linked directory
	run.MustExec(t, "qri init --name remove_update_link --format csv")

	// Save a version
	run.MustExec(t, "qri save")

	// Read .qri-ref file, it contains the reference this directory is linked to
	actual := run.MustReadFile(t, filepath.Join(workDir, ".qri-ref"))
	expect := "test_peer/remove_update_link"
	if diff := cmp.Diff(expect, actual); diff != "" {
		t.Errorf("qri list (-want +got):\n%s", diff)
	}

	// Go up one directory
	parentDir := filepath.Dir(workDir)
	os.Chdir(parentDir)

	// Rename
	run.MustExec(t, "qri rename me/remove_update_link me/remove_second_name")

	// Test that `qri list` will only show the new ref. Still linked to the old directory name.
	output := run.MustExec(t, "qri list")
	expect = `1   test_peer/remove_second_name
    linked: /tmp/remove_update_link
    22 B, 2 entries, 0 errors

`
	if diff := cmp.Diff(expect, output); diff != "" {
		t.Errorf("qri list (-want +got):\n%s", diff)
	}

	// Read .qri-ref file, it contains the new dataset reference
	actual = run.MustReadFile(t, filepath.Join(workDir, ".qri-ref"))
	expect = "test_peer/remove_second_name"
	if diff := cmp.Diff(expect, actual); diff != "" {
		t.Errorf("read .qri-ref (-want +got):\n%s", diff)
	}
}

// Test that rename command only works with human-friendly references, those without paths
func TestRenameNeedsHumanName(t *testing.T) {
	run := NewTestRunner(t, "test_peer", "rename_human")
	defer run.Delete()

	// Create a dataset and get the resolved reference to it
	output := run.MustExecCombinedOutErr(t, "qri save --body=testdata/movies/body_ten.csv me/first_name")
	ref := dsref.MustParse(parseRefFromSave(output))

	if !strings.HasPrefix(ref.Path, "/ipfs/") {
		t.Errorf("expected saved ref to start with '/ipfs/', but got %q", ref.Path)
	}

	lhs := ref.Copy()

	// Given a resolved reference for the left-hand-side is an error
	err := run.ExecCommand(fmt.Sprintf("qri rename %s test_peer/second_name", lhs))
	if err == nil {
		t.Fatal("expected error, did not get one")
	}
	expectErr := `unexpected character '@', ref can only have username/name`
	if diff := cmp.Diff(expectErr, err.Error()); diff != "" {
		t.Errorf("unexpected (-want +got):\n%s", diff)
	}

	// Make left-hand-side into a human-friendly path, create right-hand-side with a path
	lhs.Path = ""
	rhs := ref.Copy()
	rhs.Name = "second_name"

	// Given a resolved reference for the right-hand-side is an error
	err = run.ExecCommand(fmt.Sprintf("qri rename %s %s", lhs, rhs))
	if err == nil {
		t.Fatal("expected error, did not get one")
	}
	expectErr = `unexpected character '@', ref can only have username/name`
	if diff := cmp.Diff(expectErr, err.Error()); diff != "" {
		t.Errorf("unexpected (-want +got):\n%s", diff)
	}

	// Make right-hand-side into a human-friendly path
	rhs.Path = ""

	// Now the rename command works without error
	err = run.ExecCommand(fmt.Sprintf("qri rename %s %s", lhs, rhs))
	if err != nil {
		t.Errorf("got error: %s", err)
	}
}
