//go:build !enterprise

package main

import (
	"fmt"

	"github.com/nosway/namrbd/gateway/httpapi"
	phasesecurity "github.com/nosway/namrbd/sbs/cluster/security"
)

var newAdminEndpointPhasePKeyAccessLeaseClient = newAdminEndpointPhasePKeyAccessLeaseClientDefault

func newAdminEndpointPhasePKeyAccessLeaseClientDefault(repositoryConfig) (phasesecurity.KeyAccessAuthority, func(), error) {
	return nil, nil, fmt.Errorf("Phase P key access lease is Enterprise edition only")
}

func phasePAttachAdmissionEnabled(repositoryConfig) bool {
	return false
}

func newPhasePAttachAdmission(phasesecurity.KeyAccessLeaseIssuer, string, string, uint64) httpapi.AttachAdmissionFunc {
	return nil
}
