package replication

import (
	"testing"

	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

func TestWriteExecutionTransitionsToPayloadQuorumAndCommit(t *testing.T) {
	plan := &WritePlan{
		VolumeID: "00a1b2c3",
		Extents: []ExtentWritePlan{
			{
				Extent:       zeroExtent(1),
				PlacementRef: "pl-1",
				ReplicaSetID: "rs-1",
				Primary:      ReplicaTarget{ReplicaID: "rep-a"},
				WriteTargets: []ReplicaTarget{
					{ReplicaID: "rep-a"},
					{ReplicaID: "rep-b"},
					{ReplicaID: "rep-c"},
				},
				RequiredAcks: 2,
			},
			{
				Extent:       zeroExtent(2),
				PlacementRef: "pl-2",
				ReplicaSetID: "rs-2",
				Primary:      ReplicaTarget{ReplicaID: "rep-x"},
				WriteTargets: []ReplicaTarget{
					{ReplicaID: "rep-x"},
					{ReplicaID: "rep-y"},
					{ReplicaID: "rep-z"},
				},
				RequiredAcks: 2,
			},
		},
	}

	exec := NewWriteExecution(plan, "req-1", "att-1", 7, "idem-1", 3, 11)
	exec.MarkValidated()
	exec.MarkIntentPending()

	if err := exec.MarkReplicaAck(0, "rep-a"); err != nil {
		t.Fatalf("MarkReplicaAck(0, primary): %v", err)
	}
	if err := exec.MarkReplicaAck(0, "rep-b"); err != nil {
		t.Fatalf("MarkReplicaAck(0, secondary): %v", err)
	}
	if exec.State == WriteStatePayloadQuorumDone {
		t.Fatal("payload quorum should not be done until all extents reach quorum")
	}
	if err := exec.MarkReplicaAck(1, "rep-x"); err != nil {
		t.Fatalf("MarkReplicaAck(1, primary): %v", err)
	}
	if err := exec.MarkReplicaAck(1, "rep-y"); err != nil {
		t.Fatalf("MarkReplicaAck(1, secondary): %v", err)
	}
	if exec.State != WriteStatePayloadQuorumDone {
		t.Fatalf("state=%q want=%q", exec.State, WriteStatePayloadQuorumDone)
	}
	if !exec.CanCommitMetadata() {
		t.Fatal("CanCommitMetadata=false want=true")
	}
	if err := exec.MarkMetadataCommitted(); err != nil {
		t.Fatalf("MarkMetadataCommitted: %v", err)
	}
	if err := exec.MarkAcked(); err != nil {
		t.Fatalf("MarkAcked: %v", err)
	}
	if exec.State != WriteStateAcked {
		t.Fatalf("final state=%q want=%q", exec.State, WriteStateAcked)
	}
}

func TestWriteExecutionRequiresPrimaryAckForQuorum(t *testing.T) {
	plan := &WritePlan{
		VolumeID: "00a1b2c3",
		Extents: []ExtentWritePlan{
			{
				Extent:       zeroExtent(1),
				PlacementRef: "pl-1",
				ReplicaSetID: "rs-1",
				Primary:      ReplicaTarget{ReplicaID: "rep-a"},
				WriteTargets: []ReplicaTarget{
					{ReplicaID: "rep-a"},
					{ReplicaID: "rep-b"},
					{ReplicaID: "rep-c"},
				},
				RequiredAcks: 2,
			},
		},
	}
	exec := NewWriteExecution(plan, "req-1", "att-1", 7, "idem-1", 3, 11)
	exec.MarkValidated()
	exec.MarkIntentPending()

	if err := exec.MarkReplicaAck(0, "rep-b"); err != nil {
		t.Fatalf("secondary ack1: %v", err)
	}
	if err := exec.MarkReplicaAck(0, "rep-c"); err != nil {
		t.Fatalf("secondary ack2: %v", err)
	}
	if exec.Extents[0].QuorumReached {
		t.Fatal("quorum reached without primary ack")
	}
	if exec.CanCommitMetadata() {
		t.Fatal("CanCommitMetadata=true without primary ack")
	}
}

func TestWriteExecutionAllowsNonPrimaryQuorumWhenPolicyAllows(t *testing.T) {
	plan := &WritePlan{
		VolumeID: "00a1b2c3",
		Extents: []ExtentWritePlan{
			{
				Extent:       zeroExtent(1),
				PlacementRef: "pl-1",
				ReplicaSetID: "rs-1",
				Primary:      ReplicaTarget{ReplicaID: "rep-a"},
				WriteTargets: []ReplicaTarget{
					{ReplicaID: "rep-a"},
					{ReplicaID: "rep-b"},
					{ReplicaID: "rep-c"},
				},
				RequiredAcks: 2,
			},
		},
	}
	exec := NewWriteExecution(plan, "req-1", "att-1", 7, "idem-1", 3, 11)
	exec.MarkValidated()
	exec.MarkIntentPending()

	if err := exec.MarkReplicaAckWithPolicy(0, "rep-b", false); err != nil {
		t.Fatalf("secondary ack1: %v", err)
	}
	if err := exec.MarkReplicaAckWithPolicy(0, "rep-c", false); err != nil {
		t.Fatalf("secondary ack2: %v", err)
	}
	if !exec.Extents[0].QuorumReached {
		t.Fatal("quorum not reached when non-primary quorum is explicitly allowed")
	}
	if exec.Extents[0].PrimaryAcked {
		t.Fatal("primary_acked=true without primary replica ack")
	}
	if !exec.CanCommitMetadata() {
		t.Fatal("CanCommitMetadata=false for explicitly allowed non-primary quorum")
	}
}

func TestWriteExecutionFailureBlocksCommit(t *testing.T) {
	plan := &WritePlan{
		VolumeID: "00a1b2c3",
		Extents: []ExtentWritePlan{
			{
				Extent:       zeroExtent(1),
				PlacementRef: "pl-1",
				ReplicaSetID: "rs-1",
				Primary:      ReplicaTarget{ReplicaID: "rep-a"},
				WriteTargets: []ReplicaTarget{{ReplicaID: "rep-a"}},
				RequiredAcks: 1,
			},
		},
	}
	exec := NewWriteExecution(plan, "req-1", "att-1", 7, "idem-1", 3, 11)
	exec.MarkValidated()
	exec.MarkIntentPending()
	if err := exec.MarkExtentFailed(0, errBoom); err != nil {
		t.Fatalf("MarkExtentFailed: %v", err)
	}
	if exec.State != WriteStateFailed {
		t.Fatalf("state=%q want=%q", exec.State, WriteStateFailed)
	}
	if exec.CanCommitMetadata() {
		t.Fatal("CanCommitMetadata=true for failed execution")
	}
	if err := exec.MarkMetadataCommitted(); err == nil {
		t.Fatal("MarkMetadataCommitted expected error, got nil")
	}
}

var errBoom = &testErr{"boom"}

type testErr struct{ msg string }

func (e *testErr) Error() string { return e.msg }

func zeroExtent(id uint64) metadata.ExtentMappingRecord {
	return metadata.ExtentMappingRecord{VolumeID: "00a1b2c3", ExtentID: id}
}
