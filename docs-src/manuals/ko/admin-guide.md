Operations Manual

Advanced feature 안내: Enterprise 섹션은 개발·검증 중인 설계 참고이며 공개
v1.0 관리 또는 지원 범위가 아닙니다. [기능 상태](../../feature-status.md)를
확인하십시오.

# NAMRBD 관리자 가이드

이 문서는 공개 플랫폼의 ownership, observability, 운영 guardrail을
설명합니다. Enterprise backup/DR, QoS, security/KMS, governance, advanced
iSCSI, mobility, dedupe 내용은 개발 중인 설계 참고입니다. 설치 절차는
[Installation Guide](installation-guide.md), 사용자 흐름은
[User Manual](user-manual.md)을 참고하십시오.

`legacy metadata CLI` active source는 `아카이브로 이관된 코어 레포지토리` 아래 historical archive로 남아 있다. 운영자는 volume lifecycle, storage authority, diagnostics에 `namrbdctl`, `sbsctl`, `namrbd-debug`, admin APIs를 사용한다.

## 1. Component Ownership

| Component | Owns | Does not own |
|----|----|----|
| `namrbd-gateway` | 호스트 I/O 포워딩, 게이트웨이 자가 레지스트리, 볼륨 마운트/경로계획 컨트롤 플레인 메타 관리, `etcd` 연동 상태 관리, 데이터패스 처리량 통계 지표 | SBS placement authority, EC membership, repair/rebalance/drain decisions |
| `sbs-service` | SBS authoritative metadata, cluster membership, topology, placement, volume/snapshot/restore, EC profile, maintenance transitions, enterprise Backup/DR target/policy/run/artifact/hold/status state, enterprise security policy/key/audit/crypto-erase state, and performance policy authority | 호스트 블록 디바이스 라이프사이클 관리, 커널 자원 큐잉 통제 |
| `sbs-data` | 노드 로컬 스토어 배열, 복제본 물리 청크, EC 분산 샤드, 로컬 디바이스 건전성 상태 | Global placement authority |
| Kernel modules | 블록 디바이스 가용 생명주기 관리, 입출력 요청 큐잉, 다중 경로 장애 극복(failover), DISCARD/WRITE_ZEROES 저지연 전송 처리 | Metadata authority and placement decisions |
| `namrbd-csi-driver` | 쿠버네티스 CSI 볼륨 생성/삭제/마운트 준비(Stage/Publish)/스냅샷/복원 및 동적 확장 규격 제어 | Snapshot/clone/read-view/GC semantics |
| `namrbd-iscsi-gateway` | iSCSI 타겟 기동 세션 수립, SCSI/iSCSI 표준 명령 분석 및 매핑, 프로토콜 상태 노출, 최종 검증 보고서 산출물 | SBS metadata, placement, encryption key authority, discard/reclaim authority, active/standby HA lease authority |
| `namrbdctl` | 호스트/디바이스 마운트, 언마운트, 세부 상태 모니터링, 볼륨 정합성 제어용 1차 CLI | Storage-side maintenance |
| `sbsctl` | SBS cluster/node/topology/volume/snapshot/restore/maintenance/guardrail operations, enterprise `backup`, `performance`, and `security` operations | Kernel device lifecycle |
| `sbsctl iscsi` | Community iSCSI smoke, fixture control state, status/session summaries, validation evidence. HA/failover output은 live HA support가 아니다. | Live cluster-wide SBS storage metadata mutation unless backed by service-owned APIs and evidence |
| `namrbd-debug` | low-level inspect/validate/break-glass diagnostics | Routine user workflow |

운영 규칙: 게이트웨이는 반드시 정식 제어 인터페이스(`sbs-service`)를 통해서만 SBS 권한 메타데이터를 수입합니다. 게이트웨이 코어 엔진이 직접 로우레벨 SBS TiKV 메타데이터를 해독하는 비정형 구조로 설계되어서는 안 됩니다.

## 2. Standard Topology

Enterprise Service production-like topology:

- gateway/control-plane 메타데이터 관리를 위한 HA `etcd` 클러스터 구성.
- 공속 메타데이터 권한 관리를 위한 고가용성 TiKV/PD 이중화 기판 구성.
- 클러스터 통합 제어를 위한 다중 `sbs-service` 노드 관리 서비스 배치.
- 물리 영역(zone)/토폴로지 라벨링이 부여된 다중 `sbs-data` 노드 및 로컬 디스크 스토어 구성.
- 고유한 `--gateway-id` 식별자를 공유하며 기동하는 하나 이상의 독립 게이트웨이 노드.
- 최신 버전의 `namrbd_blk.ko`, `namrbd_ctrl.ko` 및 `namrbdctl` 도구가 구비된 볼륨 마운트 호스트.
- 선택 적용을 위한 컨트롤러 및 노드 DaemonSet 사양의 쿠버네티스 CSI 드라이버 배포.
- 표준 iSCSI 기동장치 지원을 위해 기동되는 선택형 `namrbd-iscsi-gateway` 서비스.

Keep environment boundaries explicit:

- 동일 게이트웨이 서비스 그룹: 반드시 동일한 `--etcd-endpoints` 및 `--etcd-root`를 공유해야 합니다.
- 동일 SBS 스토리지 클러스터: 반드시 동일한 TiKV/PD 엔드포인트 세트와 keyspace를 공유해야 합니다.
- dev/stage/prod/validation 환경 격리: 반드시 상이한 roots/keyspaces 설정을 분리 관리하십시오.

### 2.1 Membership Change Workflow

Gateway, iSCSI, SBS membership 변경은 plan, preflight, apply, synchronize, verify, rollback, audit 순서의 operator envelope로 다룹니다. Gateway membership/liveness는 gateway/control-plane state가 권한이고, SBS node join/topology/store tuning/drain/remove/force-remove는 `sbs-service` AdminService가 권한입니다. `sbs-data`는 node-local health/capacity evidence를 제공하지만 cluster membership authority는 아닙니다.

먼저 read-only status를 확인하십시오. Membership 또는 capacity view를 해석하기 전에 `source_authority`, `collector_freshness_seconds`, `warning_count`, `first_error`, `last_error`, `rbac_checked`, `redaction_applied`, `unsupported_claim_visible` 필드를 함께 봅니다. Membership mutation workflow는 기존 CLI/API, RBAC rule, audit record, rollback behavior, human approval gate가 준비된 경우에만 사용합니다.

## 3. Observability

Community 공개 운영 자산은 `deploy/observability/`에 있습니다. 이 디렉터리는 SBS service/data Prometheus endpoint와 맞는 scrape 예시, starter alert rule, Grafana overview dashboard, metric catalog를 제공합니다.

``` bash
make observability-assets-check
```

`sbs-service`와 `sbs-data`는 `/healthz`, `/readyz`, Prometheus 형식 `/metrics`를 제공합니다. `namrbd-gateway`는 현재 `/api/v1/debug/gateway/metrics`와 `/api/v1/debug/sbs-cluster/metrics` JSON debug metrics를 제공합니다.

### 3.1 Host And Kernel

Use:

``` bash
namrbdctl status --device 0
lsblk /dev/namrbd0
dmesg | tail -n 100
```

Kernel watchpoints:

- attachment id and generation.
- 데이터 경로계획(path-plan) 개정 버전 및 실제 물리 경로 상태 지표.
- 볼륨 복원 혹은 동적 용량 확장 후 최종 갱신된 블록 디스크 크기.
- request op identity: READ, WRITE, FLUSH, DISCARD, WRITE_ZEROES.
- 자원 한계 압박 백프레셔(backpressure) 카운터 지표 및 내부 재큐잉(requeue) 사유.

Current kernel contract inherited through Enterprise Service:

- `REQ_OP_DISCARD` maps to discard semantics.
- `REQ_OP_WRITE_ZEROES` maps to zero semantics.
- Flush 동작은 전송 중인 데이터와 리소스 압박으로 재큐잉된 모든 데이터패스 백로그의 실제 저장이 끝날 때까지 대기한 후 최종 성공을 호스트에 보고합니다.
- 일시적인 자원 부족(Resource-busy) 상태는 물리 경로 장애로 해석하지 않습니다.

### 3.2 Gateway

Use gateway control-plane and metrics endpoints for:

- volume info.
- attach/path-plan status.
- dataplane health.
- `io_identity` Logical Zero 쓰기 카운터와 Discard 카운터의 독립된 통계 구분.
- 물리적 입출력 배분 상태 및 경로 장애 복구 현황.

공간 회수(Discard) 관측은 반드시 다음 지표를 분리 관리해야 합니다:

- `operation=discard` vs `operation=zero`.
- `policy=true_reclaim` vs `policy=zero_fallback`.
- alignment and reclaim geometry.
- fallback reason.
- 실제 디스카드(discard) 반환 바이트 및 논리 제로(logical zero) 기록 바이트.

### 3.3 `sbs-service`

Use:

``` bash
sbsctl cluster status --output json
sbsctl topology zone list
sbsctl node status --node-id data-01 --output json
sbsctl volume status --volume-id <volume_id> --output json
sbsctl volume transitions --volume-id <volume_id> --sbs-service-http-endpoint http://service-01.example.com:9081
```

`sbs-service` owns:

- leader/quorum state.
- volume placement and transitions.
- snapshot/restore authority.
- EC profile and topology placement.
- repair/rebuild/scrub/rebalance/drain state.
- Backup/DR, performance policy, security policy/key/audit, and crypto-erase state when enterprise features are enabled.

### 3.4 `sbs-data`

Use:

``` bash
curl -fsS http://data-01.example.com:9082/healthz
sbsctl store status --sbs-service-http-endpoint http://data-01.example.com:9082
```

Watch:

- store health.
- shard/store capacity.
- local payload failures.
- node-local latency.

### 3.5 엔터프라이즈 실시간 고가용 백업 및 DR 정책 <span class="edition-boundary-inline">Enterprise edition only</span>

엔터프라이즈 실시간 고가용 백업 및 DR 정책 is enterprise-only except for the community manual snapshot and manual restore-from-snapshot safety boundary. Operators should treat `sbs-service` as the authority for Backup/DR product state:

``` bash
sbsctl backup target list --output json
sbsctl backup policy list --output json
sbsctl backup run list --policy-id <policy_id> --output json
sbsctl backup artifact list --policy-id <policy_id> --output json
sbsctl backup status --policy-id <policy_id> --output json
```

Status watchpoints:

- `evidence_mode=product_state` 정식 가용 볼륨 제품 API 영속 상태 제어.
- `artifact_available=true` only after artifact integrity recheck plus userspace and kernel readback evidence.
- `restore_drill_result=kernel_readback_passed` 성공적으로 수립된 최종 가용 복구 시점의 산출물 등록.
- `delete_protection_status=guarded` 백업 보존 기한이 만료된 산출물의 정식 소거 전 보존 잠금 홀드 검사.
- `community_leakage_status=blocked` 커뮤니티 및 엔터프라이즈 기능 격리 경계를 영속적으로 보장하기 위한 경계 자동 검사.

가상 검증을 위한 Fixture 완료 검사 경로는 `--fixture` 제어 명령 및 Backup/DR fixture validation target의 가상 JSON 범위로 엄격히 제한되며, 이를 영속적인 제품 상태값으로 오인하지 않습니다.

### 3.6 KMS 연동 암호화 및 보안 통제 회로 <span class="edition-boundary-inline">Enterprise edition only</span>

KMS 연동 암호화 및 보안 통제 회로 is enterprise-only. `sbs-service` owns key provider metadata, security policies, data-key records, key access leases, audit records, rotation plans, and crypto erase state. Gateways and kernels consume admission decisions and short-lived key access results; they do not own KMS policy or persist plaintext keys.

``` bash
sbsctl security provider list --output json
sbsctl security policy list --output json
sbsctl security key list --output json
sbsctl security audit list --output json
sbsctl security crypto-erase list --output json
```

Security watchpoints:

- `plaintext_key_emitted=false` and redacted provider credential references.
- 비활성화되거나 유실 또는 소멸된 암호화 키는 볼륨 읽기, 쓰기, 마운트(attach), 볼륨 복구 및 백업 조회 시 완벽한 입출력 차단(fail-closed) 회로를 구동시킵니다.
- key rotation preserves old-object read compatibility until re-encrypt or explicit crypto erase completes.
- crypto erase must respect retention holds, protected artifacts, active attachments, and pending operations.
- 현재 security validation은 통합 mock 가상 공급자 기준이다. live external KMS network/provider destroy evidence는 해당 기능을 켤 때 별도로 검증한다.

### 3.7 기본 iSCSI Target Access <span class="edition-boundary-inline">Community basic; Enterprise edition only scale/HA</span>

기본 iSCSI target access는 선택형 표준 블록 프로토콜 frontend입니다. Community edition은 `namrbd-iscsi-gateway`, `sbsctl iscsi`, 최대 3개 distinct iSCSI-exported volumes 대상 기본 LUN export를 포함합니다. 3개 초과 export, unlimited export scale, iSCSI HA, MPIO/ALUA, 고급 보안/감사, 대규모 관측/스케일 기능은 Enterprise-only입니다.

현재 필수 compatibility claim은 Linux open-iscsi입니다. Windows native initiator는 post-closure memory-backend success evidence와 SBS-backed connection/log-cleanup evidence를 갖지만, full SBS-backed Windows read/write/readback/flush/cleanup support는 claim하지 않습니다. macOS는 licensed initiator evidence가 생길 때까지 제외됩니다.

``` bash
sbsctl iscsi status gateway --json
sbsctl iscsi status target --target-iqn <target_iqn> --json
```

iSCSI watchpoints:

- `target_iqn`, `portal_address`, `lun_id`, `volume_id`, `backend_mode`, and `backend_adapter`.
- `active_iscsi_gateway_id`, `export_lease_id`, `export_epoch`, `attachment_id`, and `generation` for writer/fencing evidence.
- 필수 요구 호환성 검증을 위한 `initiator_os=linux`, `initiator_vendor=open-iscsi`, `readback_matched=true`, 및 `error_count=0` 연동 게이트 통과.
- `macos_support_claimed=false`; 현재 iSCSI evidence만으로 macOS나 broad non-Linux support를 광고하지 않는다.
- CHAP and initiator allowlist runtime hooks fail closed with the current gotgt stack; raw CHAP secrets must never appear in JSON or logs.

### 3.8 Read-Only Operations API

`sbs-service`는 도구, report, GUI view, observe-first MCP descriptor가 사용할 수 있는 Community-safe read-only operations surface를 제공합니다. Shared SBS observability schema는 `namrbd.sbs.observability.v1`이며, NAMRBD가 이 schema를 소유합니다. NAMROS나 다른 consumer는 SBS health, capacity, reclaim, membership semantics를 별도로 재정의하지 말고 이 view를 소비해야 합니다.

``` bash
curl -fsS http://service-01.example.com:9081/api/v1/sbs/cluster
curl -fsS http://service-01.example.com:9081/api/v1/sbs/nodes
curl -fsS http://service-01.example.com:9081/api/v1/sbs/capacity
curl -fsS http://service-01.example.com:9081/api/v1/sbs/reclaim
curl -fsS http://service-01.example.com:9081/api/v1/membership/status
curl -fsS http://service-01.example.com:9081/api/v1/operations/summary
curl -fsS http://service-01.example.com:9081/api/v1/operations/warnings
curl -fsS http://service-01.example.com:9081/api/v1/query/views
curl -fsS http://service-01.example.com:9081/api/v1/mcp/tools
curl -fsS http://service-01.example.com:9081/api/v1/gui/summary
curl -fsS http://service-01.example.com:9081/api/v1/workflow/hardening
```

이 URL들은 view이며 mutation authority가 아닙니다. Capacity는 logical bytes, physical used/free bytes, reclaimable bytes, protected bytes, unknown bytes를 분리합니다. Reclaim view는 protected-reference check와 `sbs-data` before/after free-byte evidence가 있기 전까지 completion을 claim하지 않습니다. MCP와 GUI descriptor는 read-only이며, mutating tool/control은 별도 review 전까지 비활성입니다.

Read-only operations console은 동일한 `sbs-service` administration endpoint의 `/console/`에서 제공됩니다. Console은 동일한 operations view를 사용하고, `/api/v1/sbs/cluster`를 primary snapshot으로 삼아 status, topology, capacity, maintenance backlog, warning, membership source authority, reclaim evidence를 보여줍니다. 이 console은 새로운 source of truth가 아니며, stale/partial/failed collection state를 숨기지 않아야 합니다. 시각화 asset은 외부 CDN 없이 packaging된 형태로 동작해야 합니다.

Operator evidence bundle에는 product/build identity, source authority와 freshness, 관련 query snapshot, 최근 operation history, warning/error, redaction status, runbook suggestion, unavailable-evidence reason을 보존해야 합니다. Secret, token, raw payload content, private deployment path는 포함하지 않습니다. 향후 console 또는 MCP에서 상태 변경 기능을 제공하려면 reviewed API path, plan/preflight output, human approval, apply/synchronize/verify behavior, rollback guidance, audit record, RBAC/redaction check, emergency read-only lock이 먼저 준비되어야 합니다.

## 4. 실전 엔터프라이즈 인프라 운영 관리 <span class="edition-boundary-inline">Contains Enterprise edition only sections</span>

### 4.1 Volume Lifecycle

Create:

``` bash
sbsctl volume create \
  --volume-id <volume_id> \
  --size 1G \
  --block-size 4K \
  --replication-factor 3 \
  --policy-name spread-3az \
  --topology-mode strict
```

Status:

``` bash
sbsctl volume status --volume-id <volume_id> --output json
```

Attach:

``` bash
namrbdctl attach --device 0 --host <host_id> --volume <volume_id> --gateway http://gw01:9899
namrbdctl status --device 0
```

Detach:

``` bash
namrbdctl detach --device 0 --host <host_id> --volume <volume_id>
```

### 4.2 Snapshot, Clone, Restore

Enterprise Service은 현재 snapshot/clone/read-view semantics를 상속합니다:

- Snapshot identity is an immutable read-view.
- 클론 볼륨에서 발생하는 변동분(delta) 기록은 근간이 되는 원본 스냅샷을 훼손시키지 않습니다.
- 스냅샷 기반 복원은 일반 마운트 가용 볼륨을 즉각 정립해 냅니다.
- Snapshot/clone-aware GC must preserve live, snapshot, clone, restored, and pending-operation roots.

Operator restore command:

``` bash
sbsctl volume restore-from-snapshot \
  --snapshot-id <snapshot_id> \
  --volume-id <new_volume_id> \
  --size <size>
```

쿠버네티스 복원 프로세스는 동일 스토리지 협약을 준수하며 `VolumeContentSource.snapshot` 명세와 연동하는 CSI `CreateVolume` 호출로 자동 처리됩니다.

### 4.3 Expansion

볼륨 용량 확장은 증설 전용(grow-only) 상태로만 수행 가능합니다. 복원된 파일시스템 볼륨의 경우, 마운트된 애플리케이션 및 워크로드가 실제 증가된 디스크 용량을 감지하도록 하려면, 호스트 내부의 노드 파티션 용량 재로딩 및 파일시스템 리사이징(grow) 수동 실행이 요구될 수 있습니다.

볼륨 및 매핑 디바이스 상태 교차 확인:

``` bash
sbsctl volume status --volume-id <volume_id> --output json
namrbdctl status --device 0
lsblk /dev/namrbd0
```

### 4.4 EC Operations <span class="edition-boundary-inline">Enterprise edition only</span>

EC is an enterprise capability. Operator rules:

- EC 프로필과 볼륨 기하 배치는 볼륨 생성 시점 이후 불변(immutable) 상태로 잠깁니다.
- Topology mode and failure domain must be chosen before provisioning.
- 데이터 저하 읽기, 온라인 자체 스크러빙, 데이터 복구 및 샤드 재구축, 균등 리밸런싱, 스토리지 노드 배출(drain)은 `sbs-service`의 코어 관리 책임 하에 수행됩니다.
- Online EC profile conversion is not a current operation; treat it as future controlled migration/repack work.

Use:

``` bash
sbsctl cluster status --output json
sbsctl repair list --output json
sbsctl rebalance list --output json
```

### 4.5 Enterprise Backup/DR Control Plane <span class="edition-boundary-inline">Enterprise edition only</span>

백업 타겟 인프라 및 스케줄 정책 수립:

``` bash
sbsctl backup target create \
  --target-id target-a \
  --type local_filesystem \
  --root /var/lib/namrbd-backup/target-a \
  --capacity-status ok

sbsctl backup policy create \
  --policy-id policy-a \
  --source-volume-id <volume_id> \
  --target-id target-a \
  --schedule every:24h \
  --retention-count 2 \
  --retention-age-days 7
```

Record a manual run and mark an artifact available only after restore drill evidence:

``` bash
sbsctl backup run start \
  --policy-id policy-a \
  --run-id run-a \
  --source-snapshot-id <snapshot_id> \
  --snapshot-root-id <snapshot_root_id>

sbsctl backup artifact availability \
  --artifact-id artifact-a \
  --run-id run-a \
  --target-id target-a \
  --source-volume-id <volume_id> \
  --source-snapshot-id <snapshot_id> \
  --snapshot-root-id <snapshot_root_id> \
  --restore-size 8K \
  --restore-drill-id restore-drill-readback-pass \
  --restore-drill-result kernel_readback_passed_artifact_transition_pending \
  --artifact-integrity-rechecked \
  --userspace-readback-matched \
  --kernel-readback-matched
```

실제 파일 영구 소거 전에, 보존용 홀드(Hold) 락을 수립하고 모의 검사(dry-run) 세부 현황을 조회하십시오:

``` bash
sbsctl backup hold create \
  --hold-id hold-a \
  --target-kind artifact \
  --target-id artifact-a

sbsctl backup purge plan \
  --artifact-id artifact-a \
  --output json
```

Backup/DR 제어 레이어는 destructive purge executor, background copy scheduler, 또는 automated remote DR을 추가하지 않습니다. Enterprise security/compliance controls는 이미 유효한 Backup/DR state를 감싸지만 integrity와 restore-drill rules 없이 artifact를 available로 만들지는 않습니다. `sbs-service`가 변경된 배포에서는 validation 환경에서 API 결과를 해석하기 전에 반드시 `sbs-service`를 restart/redeploy해야 합니다.

### 4.6 Enterprise Security Control Plane <span class="edition-boundary-inline">Enterprise edition only</span>

Use `sbsctl security` for enterprise provider, policy, key, lease, audit, and crypto erase operations. Community builds must hide this surface.

``` bash
sbsctl security provider create \
  --provider-id provider-a \
  --provider-type local_fixture \
  --endpoint-ref fixture:local \
  --output json

sbsctl security policy create \
  --policy-id policy-a \
  --key-provider-id provider-a \
  --output json

sbsctl security policy bind \
  --policy-id policy-a \
  --volume-id <volume_id> \
  --output json

sbsctl security audit verify --output json
```

### 4.7 Enterprise Governance/WORM Boundary <span class="edition-boundary-inline">Enterprise edition only</span>

Governance/WORM 보안 통제 사양은 사전 한계 합의에 맞춰 scoped support로 정리되었습니다. 적용되는 통제 범위는 블록 디바이스 기반의 파생 보안 객체 통제, 디버깅 목적의 WORM 가상 Fixture 기동, 게이트웨이 영역에서의 기인증 블록 쓰기 필터링으로 제한됩니다. 수시 덮어쓰기 입출력을 임의 수용하는 일반 마운트용 볼륨은 WORM 상태로 정의되지 않습니다.

Do not treat scoped Governance/WORM support as regulatory certification or object-store API compatibility. Public governance API/CLI, kernel/iSCSI/NVMe protected-state support, ransomware recovery support, and remote DR support remain unsupported until their owning evidence gates close.

데이터가 파괴되는 치명적인 보안 통제 명령(암호화 키 영구 소멸 등)을 실행하기 전에, 반드시 아래 감사 계획 및 실행 불가(blocking) 사유 검출을 선행하십시오:

``` bash
sbsctl security key destroy-plan --data-key-id <data_key_id> --output json
sbsctl security crypto-erase plan --target-type volume --target-id <volume_id> --output json
```

### 4.7 iSCSI Target Operations <span class="edition-boundary-inline">Community basic; Enterprise edition only scale/HA</span>

SBS-backed iSCSI LUN은 volume과 backend endpoint가 준비된 뒤에만 시작합니다. 현재 기본 iSCSI product wording은 보수적입니다: one target with one LUN이 initial model이고, Persistent Reservation product semantics는 reject되며, MPIO/ALUA는 advertise하지 않습니다.

``` bash
namrbd-iscsi-gateway \
  --backend=sbs \
  --portal <gateway_ip>:3260 \
  --serve \
  --sbs-endpoint <sbs_volume_service_host>:9444 \
  --volume-id <volume_id> \
  --export-id <export_id> \
  --target-iqn <target_iqn> \
  --active-iscsi-gateway-id <iscsi_gateway_id> \
  --export-lease-id <lease_id> \
  --export-epoch <epoch> \
  --attachment-id <attachment_id> \
  --generation <generation> \
  --allow-gotgt-wildcard-listen \
  --summary-json ./namrbd-output/gateway-summary.json \
  --operation-jsonl ./namrbd-output/gateway-operations.jsonl \
  --json
```

`--allow-gotgt-wildcard-listen`는 현재 gotgt listener 제한을 반영한 옵션입니다. isolated fixture 또는 통제된 validation/deployment network에서만 사용하고, initiator source-IP ACL로 해석하지 않습니다.

운영 적용 전 체크리스트:

- 각 exported LUN을 명시적인 portal address, target IQN, export id, active iSCSI gateway id, lease id, attachment id, generation에 고정합니다.
- TCP/3260은 host firewall, network policy 또는 동등한 접근 제어 계층으로 보호합니다. wildcard listener flag는 initiator 인증 기능이 아닙니다.
- Target gateway는 service manager로 실행하고, operation JSONL은 local log policy에 맞춰 rotate하며, summary JSON은 support/debug evidence로 보존합니다.
- Binary, portal, SBS endpoint, target, export, attachment, command mapping 변경 후에는 initiator evidence를 해석하기 전에 target gateway를 재시작합니다.
- Community 배포는 3개의 distinct iSCSI-exported-volume cap을 지킵니다. 더 큰 export scale, HA, MPIO/ALUA, 고급 보안/감사, 대규모 관측성은 Enterprise로 계획합니다.

Linux initiator 유효성 입증 시에는 부분적인 세션 로그를 수동으로 해석하지 말고 유지보수되는 iSCSI smoke validation 결과로 판정하십시오. 인정 가능한 증거에는 gateway summary JSON, operation JSONL, initiator session detail, readback 성공 여부, `ok_count`, `error_count`, 그리고 해당 실행에서 gateway를 재시작했는지가 포함되어야 합니다.

`cmd/namrbd-iscsi-gateway`, `iscsi` package, gotgt fork patch, backend adapter semantics, iSCSI command mapping을 바꾼 경우에는 live iSCSI gateway process를 재시작한 뒤 initiator evidence를 판정합니다.

## 5. Discard/UNMAP Operations <span class="edition-boundary-inline">Enterprise edition only true reclaim</span>

Discard/Reclaim true discard rules remain the Enterprise Service UNMAP/discard baseline:

- 디바이스 정밀 정렬을 거친 복제본 볼륨 Discard 요청은 정식 `policy=true_reclaim` 수집 상태를 노출합니다.
- EC 풀스트라이프/페이지에 정렬된 온라인 Discard 요청은 정식 `policy=true_reclaim` 상태를 보고합니다.
- 정렬되지 않았거나 비정형으로 파일시스템 레이어에서 전달된 조각 discard 요청은 `policy=zero_fallback` 상태 지표 관측 기록으로만 기입되며, 즉각적인 실질 하드웨어 공간 반환으로 오인 보고되지 않습니다.
- 스냅샷 복구, 클론 즉각 생성, read-view 일관성 계약은 온라인 discard 기동 후에도 완벽하게 일관성이 보존되어야 합니다.
- 커널 네이티브 `discard` 및 `write zeroes` 경로는 명확히 별개의 운영 흐름으로 진단됩니다.

Volume delete 성공은 그 자체로 physical reclaim evidence가 아닙니다. Operator-facing summary로는 `/api/v1/sbs/reclaim`을 사용하고, pending retired payload chunks/bytes, failed batches, blocked reasons, protected-reference status, before/after free-byte evidence 필요 여부를 확인합니다.

Live validation은 replicated reclaim, EC full-stripe reclaim, kernel discard/write-zeroes 경로를 각각 분리해서 실행하고, policy 판정, reclaim 또는 zero-fill byte count, protected read-view 결과, `ok_count`, `error_count`, 테스트에 사용한 kernel module build/reload state를 기록해야 합니다.

Kubernetes discard exposure remains disabled by default. Only enable manifest discard mount options with explicit evidence:

``` bash
export NAMRBD_CSI_ENABLE_DISCARD=1
export NAMRBD_CSI_DISCARD_VALIDATION_PROFILE="<current validation evidence id>"
```

## 6. CSI/Kubernetes Operations

Current CSI surface:

- Identity, Controller, and Node services.
- Create/DeleteVolume.
- Create/Delete/ListSnapshot.
- Restore from snapshot.
- Controller expansion.
- 쿠버네티스 노드 볼륨 마운트 준비(Stage/Publish) 및 볼륨 동적 증설.
- 단일 호스트 전용 볼륨 마운트(RWOP) 경합 및 접근 차단 스모크 유효성 통과.

Operational check는 CSI controller/node readiness, dynamic provision/delete, snapshot/restore, expansion, RWOP conflict behavior, 그리고 discard exposure를 명시적으로 켠 경우 discard-wrapper evidence를 확인해야 합니다.

Discard exposure가 켜진 경우 Kubernetes wrapper는 node readiness, manifest render/apply/lint 여부, delegated CSI smoke 결과, `ok_count`, `error_count`, first error, last error, summary work directory를 기록해야 합니다.

## 7. Release Guardrails

Release packaging 전에는 현재 metadata-retirement guardrail과 edition-boundary check를 함께 확인하십시오:

``` bash
make test-community
```

Expected:

- `regression_count=0`.
- `아카이브로 이관된 코어 레포지토리` is historical-only.
- No active source, build target, smoke dependency, release manifest, or public/community export includes active `legacy metadata CLI`.
- Community builds include `namrbd-csi-driver`, Kubernetes CSI manifests, basic `namrbd-iscsi-gateway`, `sbsctl iscsi`, and LUN export, enforce the 3 distinct iSCSI-exported-volume cap, and hide enterprise-only iSCSI HA, MPIO/ALUA, advanced security/audit, and scale observability operations.
- Community builds hide enterprise `backup`, `performance`, and `security` command surfaces while keeping manual replicated snapshot/restore visible.

Release guardrail evidence는 packaging 대상 checkout에서 scan surface, hit count, historical-only match, `regression_count=0`를 기록해야 합니다.

Generated public/community export artifact validation remains a separate release artifact check unless it is explicitly run and recorded.

## 8. Closure And Validation

Closure는 검토 대상 source revision, binary, image, service restart, kernel module state에 대해 배포된 제품 경로가 fresh evidence를 가진다는 뜻입니다. Private validation path, 과거 hostname, cached artifact를 public support claim처럼 재사용하지 않습니다.

기본 iSCSI target access는 Linux open-iscsi를 필수 compatibility baseline으로 둡니다. Validation package에는 fixture startup, SBS-backed Linux initiator discovery/login, guarded LUN selection, write/readback, flush 또는 UNMAP observation, logout, cleanup, Community edition-boundary status, unsupported initiator exclusion이 포함되어야 합니다.

Full Discard/Reclaim closure는 required-mode Kubernetes smoke evidence, replicated reclaim evidence, Enterprise EC 사용 시 EC reclaim evidence, live kernel discard evidence를 요구합니다.

Enterprise Backup/DR, Performance, Security, iSCSI closure package는 서로 분리된 기록이어야 합니다. 각 package는 scheduler execution, destructive purge execution, live external KMS provider/destroy, active/standby iSCSI HA, full SBS-backed Windows support, macOS support, remote DR automation을 validated, skipped, future work 중 어디로 분류했는지 명시합니다.

최소 closure summary field:

- `result`, `ok_count`, `error_count`, `skipped_count`, first error, last error.
- git revision, binary checksum, image tag, kernel module version, service restart/reload status.
- 변경되었거나 의도적으로 변경하지 않은 gateway, `sbs-service`, `sbs-data`, CSI driver, iSCSI gateway, kernel process.
- Community/Enterprise build, replicated/EC backend, Kubernetes discard gate, iSCSI portal/IQN/LUN identity, backup/security policy id 같은 feature mode evidence.
- host block device, CSI pod, snapshot restore, backup artifact, iSCSI LUN에 대한 readback evidence.
- unsupported 또는 future compatibility path에 대한 명시적 exclusion.

`namrbd-gateway`, `sbs-service`, `sbs-data`, CSI, iSCSI, kernel code를 바꾸는 validation package는 smoke나 workload 결과를 해석하기 전에 대응되는 deploy, restart, reload step을 포함해야 합니다.

## 9. Troubleshooting Checklist

When a smoke or workload fails, capture:

- failed command/target.
- layer: script/env, CLI, gateway, kernel, `sbs-service`, `sbs-data`, metadata.
- 최초 유입된 경고 예외 및 최종 확인된 가용 오류 정보.
- summary `result`, `ok_count`, `error_count`, `skipped_count`.
- attachment id, generation, path plan revision, device size, and runtime path status for kernel/gateway failures.
- Kubernetes PVC/PV/pod/events and CSI logs for CSI failures.
- iSCSI target IQN, portal, LUN id, initiator IQN/vendor/version, SCSI status/sense, session logs, and gateway operation JSONL for iSCSI target failures.
- deploy/restart/reload state.

Common boundaries:

- Metadata CAS conflicts must be classified as stale writer/fencing vs ordinary retryable same-volume concurrency.
- 이중화 게이트웨이 구성 테스트는 입출력 부하의 고른 배분 및 데이터 전송 정합성을 입증해야 합니다.
- 스냅샷, 클론 생성, Read-view 기하 변동 발생 시에도, 원본 볼륨 정보, 스냅샷 내용물, 클론 볼륨 변동분 기록, 가용 보호 잠금 관계, 사용 만료된 폐기 페이로드 가비지 컬렉션(GC) 일관성은 한 치의 오차 없이 완벽하게 수호되어야 합니다.
- 폐기 대상 페이로드 및 자원 추적 로그 보고 시, 디버깅 목적의 비정형 휴먼 로그가 정형화된 JSON 데이터 스트림 흐름을 오염시켜서는 안 됩니다.

## 10. Security Notes

Enterprise security implements the 통합 mock 가상 공급자 security/compliance baseline: key policy, data-key metadata, key access leases, encrypted replicated/EC/backup payload evidence, disabled-key fail-closed behavior, rotation, audit, and crypto erase. Current operational security focus:

- keep admin endpoints protected by environment/network policy.
- 수동 복구 가이드 가동, 응급 복구(break-glass), 마스터키 교체, 스냅샷 백업 및 볼륨 복원, 암호화 완전 폐기(crypto erase) 조작의 정밀 감사 추적 로그 보존.
- 공개/커뮤니티용 내보내기 인터페이스 소스에서 아카이브 및 상용 흔적이 섞이지 않도록 완전 분리.
- 운영자가 직접 실행하는 표준 상용 제어 명령 경로인 `legacy metadata CLI`는 더 이상 노출되지 않습니다.
- never emit plaintext key material, provider credentials, CHAP secrets, or payload samples in JSON/logs/summaries.
- 실제 외부 클라우드 KMS 연동 가용성 및 정식 하드웨어 소멸 검증을 직접 실행하여 연동 필증을 획득하기 전까지는, 외부 KMS 실시간 통제 지원 사양을 함부로 주장해서는 안 됩니다.

[\<- Architecture Index](../architecture-manual/index.md) [User Manual -\>](user-manual.md)
