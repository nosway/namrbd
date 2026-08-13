//go:build !enterprise

package main

import (
	"flag"

	"github.com/nosway/namrbd/gateway/service"
	"github.com/nosway/namrbd/gateway/store"
)

type phasePRepositoryFlags struct{}

type phasePRepositoryConfig struct {
	SBSClusterReplicatedPayloadEncryption bool
	DataKeyID                             string
	KeyVersion                            uint64
}

func registerPhasePRepositoryFlags(*flag.FlagSet) phasePRepositoryFlags {
	return phasePRepositoryFlags{}
}

func (phasePRepositoryFlags) config() phasePRepositoryConfig {
	return phasePRepositoryConfig{}
}

func validatePhasePRepositoryConfig(repositoryConfig) error {
	return nil
}

func maybeWrapPhasePC6DataRepository(_ service.MetadataRepository, _ store.ObjectStore, _ repositoryConfig, dataRepo service.DataRepository, dataDesc string) (service.DataRepository, string, error) {
	return dataRepo, dataDesc, nil
}
