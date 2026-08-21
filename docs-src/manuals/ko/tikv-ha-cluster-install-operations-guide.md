Operations Guide

# TiKV 고가용성(HA) 클러스터 설치 및 운영 가이드

이 문서는 **복수 서버에 PD와 TiKV를 분산 배치**하여 단일 장애를 흡수할 수 있는 TiKV 클러스터를 구축·운영하는 방법을 정리한다.

범위:

- HA 토폴로지와 quorum 요건
- TiUP `cluster`를 이용한 프로덕션형 설치
- 네트워크·방화벽·디스크 준비
- NAMRBD(`sbs-service`, gateway) 연동
- 일상 점검, 장애 대응, 유지보수

범위 밖 또는 별도 공개 문서 참고:

- 단일 노드 developer playground는 이 공개 HA 운영 가이드의 지원 범위가 아니다.
- SBS metadata quorum 장애 모델·repair 정책은 [Metadata Authority](../architecture-manual/chapters/04-metadata-authority.md)와 [Topology, Placement, And Expansion](../architecture-manual/chapters/14-topology-placement-and-expansion.md)을 기준으로 해석한다.
- 전체 NAMRBD bring-up은 [Installation Guide](installation-guide.md)를 따른다.

## 1. NAMRBD에서 TiKV가 하는 일

TiKV는 NAMRBD에서 **두 가지 역할**로 쓰일 수 있다. 클러스터를 나누는 것을 권장한다.

| 역할 | 사용 컴포넌트 | API | authority |
|----|----|----|----|
| SBS cluster metadata | `sbs-service`, (legacy/dev) gateway raw metadata | TxnKV | storage layout, placement, repair state |
| Legacy RawKV payload store | `namrbd-gateway` (`store-backend=tikv`) | RawKV | object blob |

공통 규칙:

- 클라이언트는 **PD client endpoint** 목록으로 topology를 발견한다.
- `--etcd-endpoints`와 `--tikv-pd-endpoints`는 **다른 주소**다. 혼동하면 attach는 되고 storage metadata만 실패하는 등 진단이 어려워진다. etcd HA 설치는 [etcd HA Cluster Install Operations Guide](etcd-ha-cluster-install-operations-guide.md)를 참고한다.
- 같은 SBS cluster에 속한 모든 소비자는 **동일한 PD endpoint 집합**과 **동일한 keyspace**를 사용한다.

## 2. 권장 HA 토폴로지

### 2.1 최소 운영 기준

분산 SBS 검증 환경과 동일하게, metadata quorum 용도는 아래를 권장한다.

- **PD 3대** (홀수, Raft quorum)
- **TiKV 3대 이상** (Region 복제 RF=3 기본)
- PD와 TiKV 프로세스를 **서로 다른 failure domain**(호스트, 가능하면 rack/zone)에 배치

```
                    +------------------+
                    |  NAMRBD clients  |
                    | sbs-service, gw  |
                    +--------+---------+
                             |
              PD client URLs (comma-separated)
                             |
        +--------------------+--------------------+
        |                    |                    |
   +----+----+          +----+----+          +----+----+
   |  PD-1   |<-------->|  PD-2   |<-------->|  PD-3   |
   +----+----+          +----+----+          +----+----+
        |                    |                    |
        +--------------------+--------------------+
                             |
                    schedule / heartbeat
                             |
        +--------------------+--------------------+
        |                    |                    |
   +----+----+          +----+----+          +----+----+
   | TiKV-1  |          | TiKV-2  |          | TiKV-3  |
   | (data)  |          | (data)  |          | (data)  |
   +---------+          +---------+          +---------+
```

### 2.2 비권장 구성

- PD 1 + TiKV 1 (개발 재현용만)
- 세 PD/TiKV를 **한 물리 호스트**에만 두는 구성
- playground 결과만으로 production HA sign-off

### 2.3 확장 시

- TiKV store만 추가해 용량·처리량을 늘릴 수 있다.
- PD는 홀수 유지(3, 5, …). 5 PD는 대규모·다지역에 가깝다.
- NAMRBD SBS data node 수(`data-01..data-09` 등)와 TiKV node 수는 **독립**이다. metadata용 TiKV는 보통 3~5대의 전용 quorum host에 둔다.

## 3. 사전 준비

### 3.1 호스트

| 항목   | 권장                                                    |
|--------|---------------------------------------------------------|
| OS     | Linux (RHEL/Rocky/Ubuntu LTS), 동일 major 버전 통일     |
| CPU    | x86_64 또는 arm64, 클러스터 전체 동일 아키텍처          |
| RAM    | TiKV 노드당 32 GiB 이상(워킹셋·block cache에 따라 증가) |
| 디스크 | NVMe/SSD, 데이터 전용 마운트(`noatime`), XFS 또는 ext4  |
| 시간   | NTP/chrony 동기화(수 ms~수십 ms skew)                   |
| SSH    | 배포용 orchestrator → 모든 노드 passwordless SSH        |

### 3.2 네트워크 포트

TiUP 기본 포트(커스텀 시 topology YAML에서 변경):

| 포트        | 컴포넌트 | 용도                               |
|-------------|----------|------------------------------------|
| `2379/tcp`  | PD       | client API (`--tikv-pd-endpoints`) |
| `2380/tcp`  | PD       | peer Raft                          |
| `20160/tcp` | TiKV     | KV 서비스                          |
| `20180/tcp` | TiKV     | status/metrics                     |

방화벽 규칙:

- 모든 PD/TiKV 노드 ↔ 모든 PD/TiKV 노드: 위 포트 **양방향**
- NAMRBD `sbs-service` / gateway가 있는 **모든 호스트** → **모든 PD client URL(`2379`)**: 허용
- 외부 인터넷에서 TiKV 포트를 열 필요는 없다(내부망만)

validation 환경에서 비표준 포트(예: `44751`)를 쓰는 경우, topology와 `NAMRBD_TIKV_PD_ENDPOINTS`를 **같은 값**으로 맞춘다.

### 3.3 디렉터리

각 TiKV 노드 예시:

``` bash
sudo mkdir -p /data/tikv
sudo mkdir -p /data/pd
sudo chown -R "$(whoami)" /data/tikv /data/pd   # TiUP이 사용할 계정에 맞게 조정
```

PD는 상대적으로 작은 디스크로 충분하나, **별도 디스크**에 두는 편이 안전하다.

## 4. TiUP으로 HA 클러스터 설치

배포는 **한 대의 orchestrator**에서 TiUP으로 수행한다. TiKV 프로세스는 대상 서버에 원격 기동된다.

### 4.1 TiUP 설치 (orchestrator)

``` bash
curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh
source ~/.bashrc   # 또는 ~/.profile
tiup --version
tiup install pd tikv
```

### 4.2 topology 파일

`namrbd-tikv-ha.yaml` 예시 — 호스트명·IP·zone는 환경에 맞게 수정한다.

``` yaml
# Example: 3 PD + 3 TiKV, no TiDB (metadata / RawKV dedicated cluster)
global:
  user: "tikv"
  ssh_port: 22
  deploy_dir: "/opt/tidb-deploy"
  data_dir: "/data/tidb-data"

pd_servers:
  - host: pd1.example.internal
    client_port: 2379
    peer_port: 2380
    name: pd1
  - host: pd2.example.internal
    client_port: 2379
    peer_port: 2380
    name: pd2
  - host: pd3.example.internal
    client_port: 2379
    peer_port: 2380
    name: pd3

tikv_servers:
  - host: tikv1.example.internal
    port: 20160
    status_port: 20180
    config:
      server.labels:
        zone: "zone-a"
        host: "tikv1"
  - host: tikv2.example.internal
    port: 20160
    status_port: 20180
    config:
      server.labels:
        zone: "zone-b"
        host: "tikv2"
  - host: tikv3.example.internal
    port: 20160
    status_port: 20180
    config:
      server.labels:
        zone: "zone-c"
        host: "tikv3"
```

TiDB SQL을 쓰지 않는 **PD+TiKV 전용** 클러스터이면 `tidb_servers` 섹션은 생략한다.

### 4.3 배포 및 기동

``` bash
export CLUSTER_NAME=namrbd-tikv-ha

tiup cluster deploy "$CLUSTER_NAME" v8.5.0 namrbd-tikv-ha.yaml \
  --user tikv \
  -y

tiup cluster start "$CLUSTER_NAME"
tiup cluster display "$CLUSTER_NAME"
```

`display` 출력에서 각 PD의 **Client URLs**를 기록한다. NAMRBD 설정은 아래 형식이다.

``` bash
export NAMRBD_TIKV_PD_ENDPOINTS="pd1.example.internal:2379,pd2.example.internal:2379,pd3.example.internal:2379"
```

### 4.4 설치 직후 검증

``` bash
# PD health (각 PD client URL에 대해)
curl -fsS "http://pd1.example.internal:2379/health"

# TiKV store 상태 (tiup 내장)
tiup cluster check "$CLUSTER_NAME"

# 임의 노드에서 PD reachability (NAMRBD 소비자와 동일 경로)
for ep in pd1.example.internal:2379 pd2.example.internal:2379 pd3.example.internal:2379; do
  host="${ep%:*}"
  port="${ep#*:}"
  timeout 3 bash -c "</dev/tcp/${host}/${port}" && echo "ok ${ep}"
done
```

PD leader 확인:

``` bash
tiup ctl:v8.5.0 pd -u http://pd1.example.internal:2379 member
```

## 5. NAMRBD 연동

### 5.1 환경 변수 (권장)

``` bash
export NAMRBD_TIKV_PD_ENDPOINTS="pd1.example.internal:2379,pd2.example.internal:2379,pd3.example.internal:2379"
export NAMRBD_TIKV_API_VERSION="v1"
export NAMRBD_TIKV_KEYSPACE="namrbd-sbs-prod-001"
```

운영 규칙:

- dev/stage/prod는 **keyspace를 분리**한다.
- endpoint 목록에 **살아 있는 PD를 2개 이상** 넣어 client failover를 허용한다.
- metadata backend는 `sbs-service`에 `--metadata-backend=tikv`와 함께 지정한다. 상세는 [Installation Guide](installation-guide.md) §5.2-5.4를 참고한다.

### 5.2 `sbs-service` 예시

``` bash
./sbs-service \
  --cluster-id namrbd-prod \
  --sbs-cluster-id sbs-prod-9n \
  --node-id service-01 \
  --metadata-backend tikv \
  --tikv-pd-endpoints "$NAMRBD_TIKV_PD_ENDPOINTS" \
  --tikv-api-version "$NAMRBD_TIKV_API_VERSION" \
  --tikv-keyspace "$NAMRBD_TIKV_KEYSPACE" \
  --sbs-service-listen 0.0.0.0:9443 \
  --sbs-service-http-listen 0.0.0.0:9081
```

모든 `sbs-service` 인스턴스와 동일 cluster의 gateway(legacy raw metadata 경로)는 **같은 PD/keyspace**를 사용한다.

### 5.3 API version 선택

| 용도 | 권장 `--tikv-api-version` |
|----|----|
| dedicated metadata / RawKV cluster | `v1` |
| TiDB와 동일 클러스터 공존, multi-tenant keyspace | `v2` (TLS·keyspace 정책 추가 검토) |

`sbs/cluster/metadata`는 **v1ttl을 지원하지 않는다**. metadata backend에는 `v1` 또는 `v2`만 사용한다.

### 5.4 bootstrap 순서

1.  TiKV/PD cluster health 확인
2.  (선택) keyspace·root prefix 접근 smoke
3.  `etcd` control-plane 기동·health
4.  `sbs-data` → `sbs-service` 기동
5.  gateway attach/read/write smoke

## 6. 일상 운영

### 6.1 점검 체크리스트

| 항목 | 명령/방법 | 정상 기준 |
|----|----|----|
| PD quorum | `tiup cluster display`, `curl .../health` | 과반 PD up, leader 존재 |
| TiKV store | `tiup ctl pd store` | 모든 store `Up` |
| Region leader skew | Grafana / `pd-ctl region` | 지속적 hotspot 없음 |
| 디스크 | `df`, TiKV metrics | 사용률 \< 운영 임계(예: 80%) |
| NAMRBD 연결 | `sbs-service` healthz, metadata op | timeout 급증 없음 |
| clock skew | `chronyc tracking` | 수십 ms 이내 |

### 6.2 PD endpoint 관리

- DNS 또는 고정 VIP 뒤에 PD를 두는 경우, **NAMRBD 설정과 실제 PD client URL**이 일치하는지 변경 후 반드시 확인한다.
- PD 멤버 교체 후에는 `NAMRBD_TIKV_PD_ENDPOINTS`에서 **제거된 주소를 빼고** 새 주소를 추가한 뒤 `sbs-service`/gateway를 순차 재기동한다.

### 6.3 keyspace·데이터 분리

- validation: `namrbd-sbs-validation`
- prod: `namrbd-sbs-prod-001`

같은 TiKV 클러스터를 공유하더라도 keyspace로 논리 분리한다. **prod keyspace를 validation smoke에 쓰지 않는다.**

### 6.4 계획 유지보수 (rolling)

권장 순서:

1.  클러스터 부하·repair backlog 확인
2.  TiUP으로 **한 노드씩** `tiup cluster restart -R tikv` 또는 PD/TiKV role별 rolling
3.  각 단계 후 `display`, store up, NAMRBD metadata latency 확인
4.  foreground I/O tail latency가 허용 범위인지 확인

PD와 TiKV를 **동시에** 여러 대 내리지 않는다. PD quorum이 깨지면 metadata mutation이 불안정해진다.

``` bash
tiup cluster restart "$CLUSTER_NAME" -R tikv --limit tikv1.example.internal:20160
```

### 6.5 용량 확장

TiKV store 추가:

1.  topology YAML에 `tikv_servers` 항목 추가
2.  `tiup cluster scale-out` (TiUP 버전별 서브커맨드는 공식 문서 참고)
3.  PD가 replica를 rebalance할 때까지 대기
4.  NAMRBD 측 변경 없음(PD endpoint 집합만 동일하면 됨)

## 7. 장애 시나리오와 대응

### 7.1 단일 TiKV 노드 다운

기대:

- RF=3이면 quorum 유지, Region leader 재선출
- metadata read/write는 계속 가능하나 tail latency 증가 가능

대응:

- store 복구 또는 교체
- NAMRBD: gateway/sbs timeout·metadata error rate 관측
- repair/rebalance가 foreground I/O를 압박하지 않는지 확인

운영 원칙은 이 문서의 [점검 체크리스트](#6-1-점검-체크리스트)와 [계획 유지보수 절차](#6-4-계획-유지보수-rolling)를 따른다.

### 7.2 PD 1대 다운 (3 PD 중 1)

기대:

- quorum 유지, leader 재선출, 짧은 latency spike

대응:

- PD 프로세스·디스크·네트워크 확인
- `etcd` attach 문제와 **분리**해 진단 (control-plane vs storage metadata)

### 7.3 PD quorum 상실 (과반 PD down)

운영 원칙:

- **새 metadata mutation 중단**이 안전하다
- NAMRBD: attach가 control-plane에서 가능해 보여도 storage open 실패가 늘 수 있음
- quorum 복구 전 aggressive repair/rebalance 금지

### 7.4 디스크 full / compaction 압박

증상: write latency 증가, scan 지연, `sbs-service` metadata timeout

대응:

- TiKV store 용량 확장 또는 오래된 validation keyspace 정리
- compaction/background throttle 조정(TiKV `config`)
- NAMRBD maintenance 우선순위: foreground I/O \> repair \> rebalance

### 7.5 네트워크 분할

minority partition의 TiKV/PD는 authoritative write에 쓰이지 않아야 한다. NAMRBD 소비자는 **local cache만으로 authoritative write를 이어가면 안 된다**.

## 8. 백업·복구·업그레이드

### 8.1 백업

- 프로덕션: [TiKV BR](https://docs.pingcap.com/tidb/stable/backup-and-restore-overview) 또는 PingCAP 권장 백업 도구로 **정기 snapshot**
- NAMRBD metadata keyspace는 SBS cluster 상태 전체를 포함하므로, **etcd control-plane 백업과 함께** 복구 절차를 문서화한다

### 8.2 복구

- BR restore 후 PD endpoint가 동일한지 확인
- `NAMRBD_TIKV_KEYSPACE`·metadata root가 restore 대상과 일치하는지 확인
- 복구 후 `sbs-service` health + volume attach smoke

### 8.3 버전 업그레이드

``` bash
tiup cluster upgrade "$CLUSTER_NAME" v8.5.1
```

- TiKV/PD 릴리스 노트의 breaking change 확인
- staging cluster에서 먼저 rolling upgrade
- upgrade 중 NAMRBD smoke: metadata read/write, attach fencing

## 9. NAMRBD 검증 절차

로컬 또는 validation 환경에서 TiKV 연결을 검증할 때는 현재 유지보수되는 검증 절차가 다음 시나리오를 모두 기록하는지 확인합니다:

| 목적 | 검증 내용 |
|----|----|
| Legacy RawKV object store | legacy RawKV payload persistence가 필요한 경우에만 historical compatibility evidence로 확인 |
| Distributed metadata + quorum fault | TiKV metadata path에서 quorum fault, restart, metadata read/write, summary JSON을 확인 |
| two-gateway attach fencing | 두 gateway의 attachment, generation, single-writer fencing 결과를 확인 |
| quorum degrade/loss | PD/TiKV quorum degradation과 quorum loss 시 fail-closed behavior를 확인 |

실행 환경 예:

``` bash
export TIKV_PD_ENDPOINTS="$NAMRBD_TIKV_PD_ENDPOINTS"
export TIKV_API_VERSION=v1
export TIKV_KEYSPACE=namrbd-sbs-validation-smoke
```

HA sign-off는 **다중 호스트 3 PD + 3 TiKV**에서 위 suite를 돌린 결과로 판단한다. single-host playground만으로는 부족하다.

## 10. 보안·TLS (요약)

TLS를 쓰는 경우 gateway의 TiKV client 설정에 CA/cert/key를 지정한다. API 버전과 TLS를 함께 사용할 때는 PD/TiKV inter-node TLS와 클라이언트에서 PD로 연결하는 TLS를 구분하고, 모든 NAMRBD 컴포넌트가 동일한 인증서 체인을 신뢰하도록 맞춘다.

- PD/TiKV inter-node TLS와 **클라이언트→PD TLS**를 구분해 topology에 반영
- 인증서 rotation 시 NAMRBD 재기동 순서: TiKV cluster 안정 → `sbs-service` → gateway

## 11. 빠른 참조

### PD endpoint만 다시 확인할 때

``` bash
tiup cluster display namrbd-tikv-ha
# 또는
ps -ef | grep '[p]d-server' | tr '\0' '\n' | grep client-urls
```

### NAMRBD와 etcd 혼동 방지

| 설정 | 예시 | 용도 |
|----|----|----|
| `NAMRBD_ETCD_ENDPOINTS` | `10.10.0.11:2379,...` | attach, generation, gateway membership |
| `NAMRBD_TIKV_PD_ENDPOINTS` | `10.20.0.11:2379,...` | SBS storage metadata, RawKV object store |

포트 번호가 같아도 **프로세스가 다르면** endpoint 집합도 다르다.

## 12. 관련 문서

- [etcd HA Guide](etcd-ha-cluster-install-operations-guide.md) — gateway control-plane etcd HA
- [Metadata Authority](../architecture-manual/chapters/04-metadata-authority.md) — metadata quorum 운영·장애 모델
- [Topology, Placement, And Expansion](../architecture-manual/chapters/14-topology-placement-and-expansion.md) — placement와 expansion boundary
- [Installation Guide](installation-guide.md) — etcd + TiKV + sbs-service + gateway 전체 설치
- PingCAP TiUP cluster: https://docs.pingcap.com/tidb/stable/tiup-cluster/
- TiKV architecture: https://docs.pingcap.com/tidb/stable/tikv-overview/

[\<- Architecture Index](../architecture-manual/index.md) [etcd HA Guide -\>](etcd-ha-cluster-install-operations-guide.md)
