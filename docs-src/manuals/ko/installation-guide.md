Operations Manual

Edition boundary: Community edition 설치 경로와 Enterprise edition only 섹션이 함께 포함되어 있습니다.

# NAMRBD 설치 가이드 (Enterprise Service 기준)

이 문서는 NAMRBD Community 및 Enterprise 배포의 표준 설치, bring-up, 배포 검증 경로를 정리한다. 운영자는 이 문서를 설치 절차의 1차 기준으로 사용하고, 장애 대응과 day-2 운영은 [Admin Guide](admin-guide.md)를 함께 본다.

현재 설치 대상 구성 요소:

- `sbs-service`: SBS cluster authoritative metadata, placement, topology, repair/rebalance/drain, volume lifecycle authority.
- `sbs-data`: node-local payload store와 replica/EC shard I/O endpoint.
- `namrbd-gateway`: host I/O를 SBS target으로 전달하고 attach/path-plan control-plane metadata를 `etcd`에 기록한다.
- `namrbdctl`: host device create/attach/detach/status와 volume-facing primary CLI surface.
- `sbsctl`: SBS cluster, node, topology, volume, snapshot/restore, maintenance, guardrail, enterprise backup/security/performance operations CLI.
- `namrbd-debug`: 로우레벨 볼륨 점검, 정합성 검증 및 응급 디버깅 지원.
- `namrbd-csi-driver`: Kubernetes CSI Identity/Controller/Node service.
- `namrbd-iscsi-gateway`: 표준 Linux open-iscsi 기동장치를 위한 Community 기본 iSCSI 타겟 프론트엔드.
- `sbsctl iscsi`: SBS-cluster-backed status/list/get을 위한 Community iSCSI product control surface. HA/failover 제품 지원은 Enterprise-only 영역이다.

`legacy metadata CLI` active source는 `아카이브로 이관된 코어 레포지토리` 아래 historical archive로 남아 있다. 설치, 빌드, smoke, 운영 절차에서 primary command로 사용하지 않는다.

관련 공개 문서:

- [Admin Guide](admin-guide.md): day-2 운영, 관측, 장애 대응.
- [User Manual](user-manual.md): 사용자 quick start와 명령 요약.
- [Interface Specifications](../architecture-manual/chapters/appendix-interface-specifications.md): gateway, iSCSI, SBS, operator surface boundary.
- [etcd HA Guide](etcd-ha-cluster-install-operations-guide.md): HA etcd 운영 절차.
- [TiKV HA Guide](tikv-ha-cluster-install-operations-guide.md): HA TiKV/PD 운영 절차.

## 1. 사전 요구 사항

호스트 시스템 요구 사항:

- `namrbd_blk.ko` 빌드를 위한 커널 헤더/소스 트리가 제공되는 Linux 호스트.
- Go 1.26.4.
- `make`, `curl`, `jq`, `sudo`.
- `etcd` for gateway/control-plane metadata.
- 기본 다중 노드 런타임에서 `sbs-service` 공속 메타데이터 권한 수립용으로 구성된 `TiKV/PD` 연동 인프라.
- Kubernetes 1.29+ style cluster and snapshot CRDs when CSI/Kubernetes paths are installed.

권장 validation/operator 변수:

``` bash
export NAMRBD_ETCD_ENDPOINTS="data-01.example.com:2379,data-02.example.com:2379,data-03.example.com:2379"
export NAMRBD_ETCD_ROOT="/namrbd/prod"
export NAMRBD_TIKV_PD_ENDPOINTS="pd01:2379"
export NAMRBD_TIKV_API_VERSION="v1"
export NAMRBD_TIKV_KEYSPACE="namrbd-prod-001"
export NAMRBD_SBS_ADMIN_ENDPOINT="service-01.example.com:9443"
```

소유권 규칙:

- 게이트웨이는 게이트웨이/컨트롤 플레인 상태 관리를 위해 `--metadata-backend=etcd`를 사용합니다.
- `sbs-service`는 TiKV/PD를 통해 SBS 공속 메타데이터 권한을 소유합니다.
- 게이트웨이는 `--sbs-admin-endpoint`를 통해 SBS 권한 레이어와 통신하며, 기본 런타임에서 raw SBS TiKV 메타데이터를 직접 열지 않습니다.
- 로컬 Pebble SBS 메타데이터와 `--sbs-cluster-bootstrap-metadata`는 레거시/개발용 부트스트랩 경로 전용입니다.

## 2. 개발자 빌드와 테스트

공개 소스 체크아웃에서 작업하거나 코드 기여를 준비할 때는 이 섹션에서 시작합니다. 사전 빌드된 패키지를 설치하는 운영자는 [운영용 다중 노드 런타임](#3-primary-multi-node-runtime)부터 진행하면 됩니다.

공개 소스 트리의 주요 구성은 다음과 같습니다:

- `cmd/`: `namrbd-gateway`, `namrbdctl`, `sbs-service`, `sbs-data`, `sbsctl`, `namrbd-csi-driver`, `namrbd-iscsi-gateway`, `sbsctl iscsi` 명령 바이너리.
- `internal/` 및 `sbs/`: 게이트웨이, 메타데이터, 스토리지, SBS 권한, 복제, 배치, Community-safe 런타임 구현.
- `kernel/module/`: out-of-tree Linux 블록 디바이스 모듈.
- `deploy/observability/`: 공개 health, metrics, Grafana, alert, metric catalog 자산.
- `docs-src/` 및 `mkdocs.yml`: 공개 MkDocs 문서 source. 빌드 결과는
  <https://nosway.github.io/namrbd/> 에 게시됩니다.

리포지토리 루트에서 Community 명령 번들과 커널 모듈을 빌드합니다:

``` bash
make build-community
make test-community
make kernel-module
```

컨테이너 quickstart는 Compose 파일을 렌더링한 뒤 로컬 SBS smoke를 실행합니다. 이 경로는 `sbs-service` 1개와 `sbs-data` 1개를 시작하고, 작은 복제 볼륨을 만든 뒤 `sbsctl` write/read I/O를 검증합니다:

``` bash
make quickstart-compose-config
make quickstart-local-sbs-smoke
make quickstart-local-down
```

공개 운영 및 문서 자산은 다음 gate로 검증합니다:

``` bash
make observability-assets-check
make docs-source-check
```

MkDocs가 설치되어 있으면 수정 가능한 공개 문서를 빌드할 수 있습니다:

``` bash
make docs-build
```

개별 명령을 확인할 때는 공개 Makefile target을 사용합니다:

``` bash
make build-namrbd-csi-driver
make build-namrbd-iscsi-gateway
```

변경을 제안하기 전에는 변경된 경로를 직접 실행하는 가장 작은 검증 gate를 실행합니다. 소스 범위가 넓거나 edition boundary를 건드린 경우에는 다음 명령부터 시작합니다:

``` bash
mkdir -p .build-cache/go-build .build-cache/go-mod
GOCACHE=$PWD/.build-cache/go-build GOMODCACHE=$PWD/.build-cache/go-mod go test ./...
make test-community
```

Shell, manifest, summary 생성 로직을 바꾼 경우에는 문법 검사만으로 끝내지 말고, 실제 변경된 함수 경로와 결과 필드를 확인합니다.

## 3. Primary Multi-Node Runtime

기본 런타임은 게이트웨이 메타데이터, SBS 권한, 페이로드 I/O 레이어를 물리적으로 분리합니다:

| Layer | Component | Authority |
|----|----|----|
| 게이트웨이 컨트롤 플레인 | `etcd` + `namrbd-gateway` | 볼륨 바인딩(attach), 경로 계획(path-plan), 게이트웨이 레지스트리 |
| SBS 메타데이터 권한 레이어 | `sbs-service` + TiKV/PD | 클러스터 멤버십, 배치, 볼륨/스냅샷/복구, EC 프로필, 유지보수 |
| 페이로드 데이터 패스 | `sbs-data` | 로컬 스토어, 복제본 청크, EC 샤드 |
| 호스트/디바이스 | kernel modules + `namrbdctl` | 디바이스 생명주기 및 마운트 |
| 표준 프로토콜 프론트엔드 | `namrbd-iscsi-gateway` + `sbsctl iscsi` | iSCSI 타겟 세션, 포탈, LUN 엑스포트 상태, 검증 요약; 스토리지 최종 권한은 SBS 백엔드 기반 |

### 3.1 etcd

고가용성(HA) etcd 클러스터를 준비하고 모든 게이트웨이에 동일한 엔드포인트를 노출합니다:

``` bash
export NAMRBD_ETCD_ENDPOINTS="10.10.0.11:2379,10.10.0.12:2379,10.10.0.13:2379"
export NAMRBD_ETCD_ROOT="/namrbd/prod"
```

단일 환경의 모든 게이트웨이는 반드시 동일한 `--etcd-endpoints`와 `--etcd-root`를 공유해야 합니다. dev/stage/prod 루트를 격리하십시오.

### 3.2 TiKV/PD

SBS 공속 메타데이터 권한을 위한 TiKV/PD를 준비합니다:

``` bash
export NAMRBD_TIKV_PD_ENDPOINTS="10.20.0.10:2379"
export NAMRBD_TIKV_API_VERSION="v1"
export NAMRBD_TIKV_KEYSPACE="namrbd-prod-001"
```

단일 SBS 클러스터의 모든 `sbs-service` 인스턴스는 반드시 동일한 PD 엔드포인트 세트와 keyspace를 사용해야 합니다.

### 3.3 `sbs-data`

각 스토리지 노드에서 `sbs-data`를 시작합니다:

``` bash
./sbs-data \
  --node-id data-01 \
  --data-path /var/lib/namrbd/sbs-data \
  --grpc-listen 0.0.0.0:9460 \
  --http-listen 0.0.0.0:9082
```

헬스체크 및 스토어 상태 점검:

``` bash
curl -fsS http://data-01.example.com:9082/healthz
sbsctl store status --admin-http-endpoint http://data-01.example.com:9082
```

다중 스토어 노드의 경우 명시적인 스토어/샤드 정의를 전달하고 재시작 시에도 스토어 ID가 안정적으로 유지되도록 하십시오.

### 3.4 `sbs-service`

서비스 노드에서 `sbs-service`를 시작합니다. 다음은 `service-01` 노드 기동 예시입니다:

``` bash
./sbs-service \
  --cluster-id namrbd-prod \
  --sbs-cluster-id sbs-prod-9n \
  --node-id service-01 \
  --metadata-backend tikv \
  --tikv-pd-endpoints "$NAMRBD_TIKV_PD_ENDPOINTS" \
  --tikv-api-version "$NAMRBD_TIKV_API_VERSION" \
  --tikv-keyspace "$NAMRBD_TIKV_KEYSPACE" \
  --grpc-listen 0.0.0.0:9443 \
  --http-listen 0.0.0.0:9081
```

운영자 명령을 위한 관리 엔드포인트를 지정합니다:

``` bash
export SBS_ADMIN_ENDPOINTS="service-01.example.com:9443"
curl -fsS http://service-01.example.com:9081/healthz
```

클러스터 권한을 초기화하고 토폴로지를 구성합니다:

``` bash
sbsctl cluster init
sbsctl topology zone create --zone zone-a
sbsctl topology zone create --zone zone-b
sbsctl topology zone create --zone zone-c
sbsctl cluster status --output json
```

스토리지 노드들을 가입시킵니다:

``` bash
sbsctl node join --node-id data-01 --grpc-endpoint data-01.example.com:9460 --admin-http-endpoint http://data-01.example.com:9082 --zone zone-a
sbsctl node join --node-id data-02 --grpc-endpoint data-02.example.com:9460 --admin-http-endpoint http://data-02.example.com:9082 --zone zone-a
sbsctl node join --node-id data-03 --grpc-endpoint data-03.example.com:9460 --admin-http-endpoint http://data-03.example.com:9082 --zone zone-a
sbsctl node join --node-id data-04 --grpc-endpoint data-04.example.com:9460 --admin-http-endpoint http://data-04.example.com:9082 --zone zone-b
sbsctl node join --node-id data-05 --grpc-endpoint data-05.example.com:9460 --admin-http-endpoint http://data-05.example.com:9082 --zone zone-b
sbsctl node join --node-id data-06 --grpc-endpoint data-06.example.com:9460 --admin-http-endpoint http://data-06.example.com:9082 --zone zone-b
sbsctl node join --node-id data-07 --grpc-endpoint data-07.example.com:9460 --admin-http-endpoint http://data-07.example.com:9082 --zone zone-c
sbsctl node join --node-id data-08 --grpc-endpoint data-08.example.com:9460 --admin-http-endpoint http://data-08.example.com:9082 --zone zone-c
sbsctl node join --node-id data-09 --grpc-endpoint data-09.example.com:9460 --admin-http-endpoint http://data-09.example.com:9082 --zone zone-c
```

연동 상태 확인:

``` bash
sbsctl cluster status --output json
sbsctl node status --node-id data-01 --output json
```

### 3.5 Gateway

각 게이트웨이를 `etcd` 정보 및 `sbs-service` 관리 엔드포인트와 연동하여 기동합니다:

``` bash
./namrbd-gateway \
  --gateway-id gw-gw01 \
  --listen 0.0.0.0:9899 \
  --data-listen 0.0.0.0:9898 \
  --advertise-control-address 10.30.0.11 \
  --advertise-data-address 10.30.0.11 \
  --metadata-backend etcd \
  --etcd-endpoints "$NAMRBD_ETCD_ENDPOINTS" \
  --etcd-root "$NAMRBD_ETCD_ROOT" \
  --volume-cache-ttl 30s \
  --data-backend-mode sbs-cluster \
  --sbs-admin-endpoint "$NAMRBD_SBS_ADMIN_ENDPOINT"
```

추가 게이트웨이들은 동일한 `etcd` 루트 및 SBS 관리 권한을 공유하면서, 고유한 `--gateway-id` 및 광고용 주소(advertised address)를 사용합니다.

### 3.6 Host Device

마운트 대상 호스트에서 커널 모듈을 빌드하고 로드합니다:

``` bash
sudo insmod kernel/module/namrbd_blk.ko no_path_retry=fail
sudo insmod kernel/module/namrbd_ctrl.ko
```

`no_path_retry` 옵션은 가용성 정책에 따라 `fail`, `queue` 또는 초 단위 값이 될 수 있습니다. Enterprise Service은 기존 커널 discard/WRITE_ZEROES, 보안 기동 인증, 게이트웨이 v1 데이터플레인 프레이밍 기준 모델을 고스란히 상속받습니다.

디바이스를 생성하고 게이트웨이 엔드포인트를 매핑합니다:

``` bash
namrbdctl create-device
namrbdctl config-rest --device 0 --server "1,gw01,9899,false,/api/v1"
```

### 3.7 볼륨 생성 및 마운트(Attach)

복제본 기반 볼륨을 생성합니다:

``` bash
sbsctl volume create \
  --volume-id 00000065 \
  --size 1G \
  --block-size 4K \
  --replication-factor 3 \
  --policy-name spread-3az \
  --topology-mode strict

sbsctl volume status --volume-id 00000065 --output json
```

호스트에서 마운트(attach)를 수행합니다:

``` bash
namrbdctl attach \
  --device 0 \
  --host host-a \
  --volume 00000065 \
  --gateway http://gw01:9899

namrbdctl status --device 0
lsblk /dev/namrbd0
```

마운트 해제 및 청소 절차:

``` bash
namrbdctl detach --device 0 --host host-a --volume 00000065
namrbdctl destroy-device --device 0
sudo rmmod namrbd_ctrl
sudo rmmod namrbd_blk
```

### 3.8 기본 표준 iSCSI 기동장치 접속 <span class="edition-boundary-inline">Community basic; Enterprise edition only scale/HA</span>

NAMRBD는 리눅스 커널 모듈 경로 외에 선택적으로 적용할 수 있는 iSCSI 타겟 게이트웨이를 포함합니다. iSCSI 게이트웨이는 순수 프로토콜 프론트엔드로서 타겟 세션 및 SCSI/iSCSI 상태 맵을 책임지며, SBS 메타데이터, 데이터 배치, 오류 복구, 보안 통제, 공간 회수(discard) 또는 볼륨 생명주기 관리 권한을 절대 가질 수 없습니다.

Community edition은 `namrbd-iscsi-gateway`, `sbsctl iscsi`, 최대 3개 distinct iSCSI-exported volumes 대상 기본 LUN export를 포함합니다. 3개 초과 export, unlimited export scale, iSCSI HA, MPIO/ALUA, 고급 보안/감사, 대규모 관측/스케일 기능은 Enterprise-only입니다.

SBS 백엔드 기반 LUN을 기동하는 controlled validation 예시는 다음과 같습니다:

``` bash
export NAMRBD_ISCSI_PORTAL="10.30.0.21:3260"
export NAMRBD_ISCSI_TARGET_IQN="iqn.2026-06.io.namrbd:iscsi.00000065"

./namrbd-iscsi-gateway \
  --backend=sbs \
  --portal "$NAMRBD_ISCSI_PORTAL" \
  --serve \
  --sbs-endpoint data-01.example.com:9460 \
  --volume-id 00000065 \
  --export-id iscsi-00000065 \
  --target-iqn "$NAMRBD_ISCSI_TARGET_IQN" \
  --active-iscsi-gateway-id gw-iscsi-a \
  --export-lease-id lease-iscsi-00000065 \
  --export-epoch 1 \
  --attachment-id att-iscsi-00000065 \
  --generation 1 \
  --allow-gotgt-wildcard-listen \
  --summary-json ./namrbd-output/gateway-summary.json \
  --operation-jsonl ./namrbd-output/gateway-operations.jsonl \
  --json
```

`--allow-gotgt-wildcard-listen` 옵션은 현재 gotgt v0.2.2 리스너 특성상 필수적이며, 반드시 격리된 테스트 환경이나 기구축된 랩 네트워크 범위로 제한되어야 합니다. 이를 소스 IP 제어용 ACL로 신뢰하지 마십시오.

운영 환경 적용 전 체크리스트:

- TCP/3260에는 전용 portal 주소와 방화벽 정책을 적용하고, initiator 접근 제어를 wildcard listener flag에 의존하지 않습니다.
- `namrbd-iscsi-gateway`는 서비스 관리자로 실행하고, 바이너리, portal, target, export, SBS endpoint가 바뀌면 재시작합니다.
- 각 LUN마다 target IQN, export id, active gateway id, export lease id, attachment id, generation, summary JSON path, operation JSONL path를 기록합니다.
- Community 배포는 최대 3개의 distinct iSCSI-exported volumes 제한을 지킵니다. 더 큰 export scale, HA, MPIO/ALUA, 고급 보안/감사, 대규모 관측성은 Enterprise 배포로 계획합니다.
- 배포된 build마다 Linux open-iscsi initiator에서 discovery, login, guarded LUN selection, write/readback, flush observation, logout, cleanup을 검증합니다.

open-iscsi가 설치된 Linux 기동 측에서 다음을 수행합니다:

``` bash
sudo systemctl enable --now iscsid
sudo iscsiadm -m discovery -t sendtargets -p "$NAMRBD_ISCSI_PORTAL"
sudo iscsiadm -m node -T "$NAMRBD_ISCSI_TARGET_IQN" -p "$NAMRBD_ISCSI_PORTAL" --login
sudo iscsiadm -m session -P 3
ls -l /dev/disk/by-path/*iscsi*lun-0
```

현재 iSCSI compatibility claim은 Linux open-iscsi만을 정식 기준으로 둡니다. Windows 네이티브 initiator는 optional memory-backend 및 SBS-backed connection/log-cleanup evidence만 있으며, 전체 SBS-backed read/write/readback/flush/cleanup 지원은 향후 compatibility track입니다. macOS 지원은 검증 환경이 수립될 때까지 제외됩니다.

## 4. CSI/Kubernetes Install

Community edition은 CSI Identity, Controller, Node 서비스와 네임스페이스, RBAC, CSIDriver, 컨트롤러 Deployment, 노드 DaemonSet, 기본 복제 StorageClass, VolumeSnapshotClass 매니페스트 번들을 포함합니다. EC StorageClass와 고급 reclaim/scale 정책은 Enterprise-only입니다.

사전 요구 사항:

- Snapshot CRD 및 snapshot controller가 사전에 설치되어 있어야 합니다.
- `namrbd-csi-driver` 컨테이너 이미지가 빌드되어 쿠버네티스 클러스터에서 사용 가능해야 합니다.
- 컨트롤러가 구성된 게이트웨이 및 SBS 관리 엔드포인트와 통신이 가능해야 합니다.
- 각 노드에 최신 버전의 커널 모듈이 정상 적재(load)되어 있어야 합니다.

Manifest를 render/lint한 뒤 준비된 validation 환경에서 Kubernetes CSI e2e smoke를 실행합니다. Manifest rendering result, node readiness, object application result, smoke `ok_count`/`error_count`를 기록합니다.

Discard exposure를 활성화한 경우에는 정확히 배포된 image와 kernel module에 대해 required-mode Kubernetes smoke 결과를 새로 기록합니다. 기록에는 `ok_count`, `error_count`, first error, last error, node readiness, manifest가 render/apply/lint 중 어디까지 수행되었는지가 포함되어야 합니다.

### 4.1 Discard Exposure Gate <span class="edition-boundary-inline">Enterprise edition only reclaim validation</span>

쿠버네티스 디스카드 노출 기능은 기본적으로 비활성화되어 있습니다. 기본 매니페스트는 `mountOptions: []` 상태로 렌더링되며 노출 제어 상태를 어노테이션으로 남깁니다. 다음 두 조건이 모두 충족되지 않는 한 파일시스템 `discard` 마운트 옵션을 활성화하지 마십시오:

``` bash
export NAMRBD_CSI_ENABLE_DISCARD=1
export NAMRBD_CSI_DISCARD_VALIDATION_PROFILE="<current kernel discard or validation evidence id>"
```

Discard/Reclaim 엔진은 백엔드 및 커널 차원의 정밀 스모크 검증을 통해 실질 discard 정합성을 자체 인증하지만, 기본 쿠버네티스 매니페스트는 안전을 위해 보수적인 방침을 고수합니다.

## 5. 스냅샷, 복원, 온라인 용량 확장 및 EC <span class="edition-boundary-inline">Includes Enterprise edition only EC</span>

Enterprise Service은 현재 스냅샷, EC, CSI, discard/reclaim 제품 의미 사양을 온전히 물려받습니다:

- `sbsctl volume restore-from-snapshot`는 관리자를 위해 제공되는 정식 복구 명령입니다.
- CSI 차원의 `VolumeContentSource.snapshot` 기반 `CreateVolume` 호출은 백엔드의 `CreateVolumeFromSnapshot` 래퍼 명령으로 정상 바인딩됩니다.
- 복원된 볼륨은 일반 마운트 가능한 볼륨 형태로 정립되며, 용량 확장은 증설(grow-only) 형태로만 정상 지원합니다.
- Erasure Coding(EC)은 엔터프라이즈 전용 기술입니다. EC 프로필 및 기하 배치는 볼륨 생성 후 변경 불가능하며, 프로필 이관은 향후 패키징 이관(migration/repack) 프로세스 범위에서 정식 논의됩니다.
- 다중 호스트 동시 쓰기(RWX)는 현재 블록 코어 기능 범위가 아닙니다.

스냅샷, 클론, 볼륨 복원, EC, 샤드 재구축, 자체 스크러빙 및 가비지 컬렉션(GC) 관련 정밀 동작 규칙은 아키텍처 매뉴얼을 참고해 주십시오.

## 6. 설치 검증

설치 경로에 맞는 가장 작은 validation gate를 실행합니다. 최소한 syntax/edition-boundary check, CSI sanity, Kubernetes manifest validation, replicated 및 EC discard/reclaim evidence, kernel discard evidence, 기능을 켠 경우 performance/security validation evidence, iSCSI gateway를 설치한 경우 basic iSCSI fixture evidence를 기록합니다.

Public 또는 release-facing validation에서는 private environment fact가 아니라 제품 검증 사실을 기록합니다:

- 정확한 git revision, build artifact, image tag, kernel module version, 재시작한 process.
- `ok_count`, `error_count`, first error, last error, skipped gate count.
- Kubernetes 설치 시 CSI smoke와 manifest application state.
- 관련 edition/backend가 켜진 경우에만 replicated 및 EC discard/reclaim evidence.
- iSCSI gateway 설치 시 Linux open-iscsi discovery, login, guarded LUN selection, readback, flush, logout, cleanup.
- macOS initiator validation, 전체 SBS-backed Windows I/O처럼 unsupported 또는 future compatibility track인 항목은 support claim이 아닌 exclusion으로 기록.

기생성된 커뮤니티 배포판 산출물의 무결성 검사는 명시적으로 기획 실행하여 검출 로그를 기입하지 않는 한 독자적인 릴리즈 산출물 검증 단계로 위임되어 있습니다.

## 7. Legacy/Dev Bootstrap

다음 인터페이스들은 Enterprise Service의 주요 표준 런타임으로 간주되지 않습니다:

- 레거시 메타데이터 CLI 명령어 동작 경로
- Redis 페이로드 백엔드 스토리지
- 게이트웨이가 직접 호출하여 동작하는 `--sbs-cluster-metadata-path`, `--sbs-cluster-metadata-backend`, `--tikv-pd-endpoints`, `--sbs-cluster-bootstrap-metadata` 등의 로우레벨 SBS 직접 메타 플래그
- 다중 노드 프로덕션용 로컬 Pebble SBS 메타데이터 설정

이 항목들은 과거 버전 호환성 테스트, 격리 개발 영역, 혹은 아카이브 보존용 상호 참조 목적으로는 가치 있으나, 최신의 실제 운영 가이드에서는 이를 표준 운영자 접근 경로로 소개해서는 안 됩니다.

[\<- Architecture Index](../architecture-manual/index.md) [Admin Guide -\>](admin-guide.md)
