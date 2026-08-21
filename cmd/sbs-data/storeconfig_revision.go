package main

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"sync"
)

// storeConfigRevision tracks which store configuration a running sbs-data is
// actually serving.
//
// The reload endpoint previously answered with the resulting store list and
// nothing that identifies the configuration itself. Two nodes could then report
// the same stores while running different files, and an operator rolling a
// change across a fleet had no way to tell which nodes had picked it up. The
// counter answers "how many times has this node reloaded" and the digest
// answers "which content is it on".
type storeConfigRevision struct {
	mu     sync.Mutex
	count  int
	digest string
}

// observe records a successful load of the file at path and returns the
// resulting revision and digest.
//
// A digest that cannot be computed is reported as empty rather than as a
// guess, since a wrong digest is worse than a missing one: it would make two
// different configurations look identical.
func (r *storeConfigRevision) observe(path string) (int, string) {
	digest := fileDigest(path)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.count++
	r.digest = digest
	return r.count, r.digest
}

// current returns the revision without changing it.
func (r *storeConfigRevision) current() (int, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.count, r.digest
}

func fileDigest(path string) string {
	raw, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])[:16]
}
