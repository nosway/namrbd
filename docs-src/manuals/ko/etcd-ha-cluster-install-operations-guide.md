Operations Guide

# etcd 고가용성(HA) 클러스터 설치 및 운영 가이드

이 문서는 **복수 서버에 etcd 멤버를 분산 배치**하여 단일 장애를 흡수할 수 있는 etcd 클러스터를 구축·운영하는 방법을 정리한다.

범위:

- HA 토폴로지와 Raft quorum 요건
- etcd v3 멀티 멤버 bootstrap·systemd 운영
- 네트워크·방화벽·디스크 준비
- NAMRBD gateway / `namrbdctl` 연동
- 일상 점검, 백업, 장애 대응, 멤버 교체

범위 밖 또는 별도 공개 문서 참고:

- 단일 노드 개발자 playground는 이 공개 운영 가이드의 지원 범위가 아니다.
- control-plane vs storage metadata 역할 분리는 [Metadata Authority](../architecture-manual/chapters/04-metadata-authority.md)와 이 문서의 역할 설명을 따른다.
- TiKV/PD HA: [TiKV HA Guide](tikv-ha-cluster-install-operations-guide.md)
- etcd 키 레이아웃·CLI는 이 문서의 [control-plane 키 prefix 요약](#control-plane-키-prefix-요약)과 gateway 운영 절차를 따른다.

## 1. NAMRBD에서 etcd가 하는 일

etcd는 NAMRBD **gateway control-plane authority**다. `sbs-service`는 etcd를 쓰지 않는다.

| 항목                                          | authority              |
|-----------------------------------------------|------------------------|
| volume spec·control-plane status              | `etcd`                 |
| attachment ownership, generation              | `etcd`                 |
| gateway membership / liveness (lease)         | `etcd`                 |
| gateway discovery endpoint                    | `etcd`                 |
| extent/chunk/SBS placement (sbs-cluster mode) | `TiKV` (`sbs-service`) |

공통 규칙:

- 모든 gateway와 `namrbdctl`은 **동일한 `--etcd-endpoints`**와 **`--etcd-root`**를 사용한다.
- dev/stage/prod는 **`--etcd-root`로 논리 분리**한다 (같은 물리 클러스터를 공유해도 prefix는 분리).
- `--etcd-endpoints`와 `--tikv-pd-endpoints`는 **다른 프로세스**를 가리킨다. 기본 포트가 둘 다 `2379`이므로 **호스트·포트를 혼동하지 않도록** 문서화한다.

## 2. 권장 HA 토폴로지

### 2.1 최소 운영 기준

- **etcd 3멤버** (홀수, Raft quorum = 2/3)
- 멤버를 **서로 다른 호스트**(가능하면 rack/zone)에 배치
- NAMRBD gateway가 있는 호스트와 **동일 머신에 etcd를 co-locate하지 않는 것**을 권장(리소스·장애 격리). 개발 검증에서는 단일 호스트 3프로세스도 가능하나 HA sign-off용은 아님.

```
              +---------------------------+
              | namrbd-gateway, namrbdctl |
              +-------------+-------------+
                            |
            client URLs (comma-separated, :2379)
                            |
     +----------+-----------+-----------+----------+
     |          |                       |          |
+----+----+ +----+----+             +----+----+ +----+----+
| etcd-1  | | etcd-2  |<-- Raft --> | etcd-3  | | (spare) |
| :2379   | | :2379   |    :2380    | :2379   | |  N/A    |
+---------+ +---------+             +---------+ +---------+
```

### 2.2 비권장 구성

- etcd 1대만 (개발·단위 테스트용)
- 3멤버를 한 OS에만 두고 production HA로 간주
- quorum 상실 상태에서 `etcdctl`로 키를 임의 수정

### 2.3 5멤버 확장

대규모·높은 write 부하 control-plane에서는 5멤버를 고려할 수 있다. quorum은 3/5다. NAMRBD 클라이언트 설정은 **모든 살아 있는 멤버의 client URL**을 endpoint 목록에 넣으면 된다.

## 3. 사전 준비

### 3.1 호스트

| 항목 | 권장 |
|----|----|
| OS | Linux LTS (RHEL/Rocky/Ubuntu) |
| RAM | 멤버당 8 GiB 이상 (control-plane metadata 규모에 따라 증가) |
| 디스크 | SSD, 전용 마운트, `noatime` |
| 시간 | NTP/chrony (Raft election에 영향) |
| hostname/DNS | `advertise-client-urls`에 쓸 이름이 모든 소비자에서 해석 가능 |

### 3.2 네트워크 포트

| 포트       | 용도                                              |
|------------|---------------------------------------------------|
| `2379/tcp` | client API (`--etcd-endpoints`, `etcdctl`)        |
| `2380/tcp` | peer Raft (멤버 간, 반드시 멤버 전체 ↔ 전체 허용) |

방화벽:

- 모든 etcd 멤버 ↔ 모든 etcd 멤버: `2379`, `2380` **양방향**
- 모든 gateway / 운영자 jump host → 모든 etcd 멤버 `2379`

**TiKV PD와 포트 충돌:** 둘 다 기본 `2379`를 쓴다. 같은 호스트에 etcd와 PD를 둘 때는 **한쪽 포트를 변경**하고 NAMRBD 환경 변수에 실제 포트를 반영한다. 권장은 **전용 etcd 호스트 3대 + 전용 TiKV/PD 호스트**로 분리하는 것이다.

### 3.3 디렉터리

각 멤버:

``` bash
sudo mkdir -p /var/lib/etcd
sudo chown etcd:etcd /var/lib/etcd   # 서비스 계정에 맞게 조정
```

## 4. etcd 바이너리 설치

아래는 orchestrator에서 공식 릴리스를 받아 각 노드에 배포하는 예시다. 배포판 패키지(`etcd` RPM/DEB)를 쓰는 경우에도 **멤버 플래그와 URL 규칙은 동일**하다.

### 4.1 버전 설치 (각 etcd 노드)

``` bash
ETCD_VER=v3.5.16
ARCH=amd64   # arm64 환경이면 arm64

curl -fsSL "https://github.com/etcd-io/etcd/releases/download/${ETCD_VER}/etcd-${ETCD_VER}-linux-${ARCH}.tar.gz" \
  | sudo tar xz -C /usr/local/bin --strip-components=1 \
    "etcd-${ETCD_VER}-linux-${ARCH}/etcd" \
    "etcd-${ETCD_VER}-linux-${ARCH}/etcdctl"
etcd --version
```

### 4.2 클러스터 변수 (배포 전 확정)

3노드 예시 — IP/호스트명은 환경에 맞게 수정한다.

``` bash
# 고정 식별자 (멤버 추가·교체 시에도 name은 유지하는 편이 좋다)
export ETCD_NAME_1=etcd-1
export ETCD_NAME_2=etcd-2
export ETCD_NAME_3=etcd-3

export ETCD_IP_1=10.10.0.11
export ETCD_IP_2=10.10.0.12
export ETCD_IP_3=10.10.0.13

export ETCD_INITIAL_CLUSTER="${ETCD_NAME_1}=http://${ETCD_IP_1}:2380,${ETCD_NAME_2}=http://${ETCD_IP_2}:2380,${ETCD_NAME_3}=http://${ETCD_IP_3}:2380"
```

## 5. 3멤버 클러스터 bootstrap

**중요:** `--initial-cluster`와 `--initial-cluster-state new`는 **최초 1회 bootstrap**에만 사용한다. 이미 데이터가 있는 클러스터에 다시 `new`를 쓰면 데이터가 깨질 수 있다.

### 5.1 멤버 1 (`etcd-1`)

``` bash
etcd \
  --name "${ETCD_NAME_1}" \
  --data-dir /var/lib/etcd \
  --listen-client-urls "http://0.0.0.0:2379" \
  --advertise-client-urls "http://${ETCD_IP_1}:2379" \
  --listen-peer-urls "http://0.0.0.0:2380" \
  --initial-advertise-peer-urls "http://${ETCD_IP_1}:2380" \
  --initial-cluster "${ETCD_INITIAL_CLUSTER}" \
  --initial-cluster-state new \
  --initial-cluster-token namrbd-etcd-ha \
  --heartbeat-interval 250 \
  --election-timeout 1250 \
  --quota-backend-bytes 8589934592 \
  --auto-compaction-mode revision \
  --auto-compaction-retention 24h
```

### 5.2 멤버 2·3

`--name`, `--advertise-client-urls`, `--initial-advertise-peer-urls`만 해당 멤버 값으로 바꾼다. **`--initial-cluster` 문자열은 세 노드 모두 동일**해야 한다.

### 5.3 systemd 예시 (`etcd-1`)

`/etc/systemd/system/etcd.service`:

``` ini
[Unit]
Description=etcd member for NAMRBD control-plane
After=network-online.target
Wants=network-online.target

[Service]
Type=notify
User=etcd
Environment=ETCD_NAME=etcd-1
Environment=ETCD_DATA_DIR=/var/lib/etcd
Environment=ETCD_LISTEN_CLIENT_URLS=http://0.0.0.0:2379
Environment=ETCD_ADVERTISE_CLIENT_URLS=http://10.10.0.11:2379
Environment=ETCD_LISTEN_PEER_URLS=http://0.0.0.0:2380
Environment=ETCD_INITIAL_ADVERTISE_PEER_URLS=http://10.10.0.11:2380
Environment=ETCD_INITIAL_CLUSTER=etcd-1=http://10.10.0.11:2380,etcd-2=http://10.10.0.12:2380,etcd-3=http://10.10.0.13:2380
Environment=ETCD_INITIAL_CLUSTER_STATE=new
Environment=ETCD_INITIAL_CLUSTER_TOKEN=namrbd-etcd-ha
ExecStart=/usr/local/bin/etcd
Restart=on-failure
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

기동:

``` bash
sudo systemctl daemon-reload
sudo systemctl enable --now etcd
```

멤버 2·3도 각각 `ETCD_NAME`·advertise URL만 맞춘 유닛을 배포한 뒤, **짧은 간격으로 순차 기동**한다.

### 5.4 bootstrap 직후 검증

``` bash
export ETCDCTL_API=3
export ENDPOINTS="http://10.10.0.11:2379,http://10.10.0.12:2379,http://10.10.0.13:2379"

etcdctl --endpoints="$ENDPOINTS" endpoint health
etcdctl --endpoints="$ENDPOINTS" endpoint status --write-out=table
etcdctl --endpoints="$ENDPOINTS" member list
```

기대:

- 세 endpoint 모두 `healthy`
- `member list`에 3멤버, leader 1명
- gateway가 올라갈 호스트에서도 동일 `endpoint health` 성공

## 6. NAMRBD 연동

### 6.1 환경 변수

``` bash
export NAMRBD_ETCD_ENDPOINTS="10.10.0.11:2379,10.10.0.12:2379,10.10.0.13:2379"
export NAMRBD_ETCD_ROOT="/namrbd/prod"
```

NAMRBD 클라이언트는 `host:port` 형식을 사용한다 (`http://` 접두사 없음). `etcdctl`은 URL에 `http://`를 붙인다.

### 6.2 gateway

``` bash
./namrbd-gateway \
  --gateway-id gw-gw01 \
  --listen 0.0.0.0:9899 \
  --metadata-backend etcd \
  --etcd-endpoints "$NAMRBD_ETCD_ENDPOINTS" \
  --etcd-root "$NAMRBD_ETCD_ROOT" \
  --gateway-lease-ttl 30s \
  --data-backend-mode sbs-cluster \
  --sbs-admin-endpoint "$NAMRBD_SBS_ADMIN_ENDPOINT"
```

운영 규칙:

- `--gateway-id`는 인스턴스마다 **유일**
- `--gateway-lease-ttl`은 etcd lease TTL이다. 네트워크 지연이 크면 너무 짧게 두지 않는다.
- `--volume-cache-ttl`을 쓰면 etcd 갱신이 캐시에 지연 반영될 수 있다.

### 6.3 `namrbdctl`

``` bash
namrbdctl volume-list \
  --etcd-endpoints "$NAMRBD_ETCD_ENDPOINTS" \
  --etcd-root "$NAMRBD_ETCD_ROOT"
```

`namrbdctl`·gateway·smoke 스크립트는 **같은 root**를 써야 한다.

### 6.4 bootstrap 순서 (전체 스택)

1.  etcd HA cluster health
2.  (선택) `etcdctl`로 test key put/get
3.  TiKV/PD HA ([TiKV HA Cluster Install Operations Guide](tikv-ha-cluster-install-operations-guide.md))
4.  `sbs-data` → `sbs-service`
5.  gateway attach/read/write smoke

## 7. 일상 운영

### 7.1 점검 체크리스트

| 항목            | 명령                                | 정상             |
|-----------------|-------------------------------------|------------------|
| endpoint health | `etcdctl endpoint health`           | 전 멤버 healthy  |
| leader          | `etcdctl endpoint status`           | leader 1명       |
| 디스크          | `df`, etcd metrics                  | quota 여유       |
| NAMRBD          | gateway attach 실패율, lease 만료   | 급증 없음        |
| DB size         | `etcdctl endpoint status`의 DB SIZE | 급격한 증가 추적 |

### 7.2 prefix·환경 분리

| 환경    | `--etcd-root` 예시 |
|---------|--------------------|
| validation | `/namrbd/validation` |
| staging | `/namrbd/stage`    |
| prod    | `/namrbd/prod`     |

validation smoke에서 prefix 삭제:

``` bash
etcdctl --endpoints="$ENDPOINTS" del "${NAMRBD_ETCD_ROOT}" --prefix
```

**prod에서는 실행하지 않는다.**

### 7.3 계획 유지보수 (rolling)

한 멤버씩:

``` bash
sudo systemctl stop etcd   # 대상 멤버만
# OS 패치·디스크 점검
sudo systemctl start etcd
etcdctl --endpoints="$ENDPOINTS" endpoint health
```

동시에 **2대 이상**을 내리면 quorum 상실(3멤버 기준)로 control-plane write가 실패한다.

### 7.4 압축·defrag

revision compaction은 `--auto-compaction-*`으로 자동화할 수 있다. 수동 defrag는 운영 창에서 멤버별로:

``` bash
etcdctl --endpoints="http://10.10.0.11:2379" defrag
```

defrag 중에도 클러스터는 동작하지만 I/O spike가 날 수 있으므로 gateway 부하가 낮은 시간대에 수행한다.

## 8. 백업·복구

### 8.1 snapshot 백업

``` bash
etcdctl --endpoints="$ENDPOINTS" snapshot save "etcd-snapshot-$(date +%Y%m%d-%H%M%S).db"
etcdctl snapshot status "etcd-snapshot-....db" --write-out=table
```

권장:

- 정기 snapshot + 오프사이트 보관
- TiKV metadata 백업 절차([TiKV HA Cluster Install Operations Guide](tikv-ha-cluster-install-operations-guide.md) §8)와 **함께** 복구 runbook 작성 (control-plane만 복구하면 generation/attach와 storage metadata가 어긋날 수 있음)

### 8.2 snapshot 복구 (요약)

공식 절차는 [etcd disaster recovery](https://etcd.io/docs/v3.5/op-guide/recovery/)를 따른다. 요지:

1.  클러스터 전체 중지
2.  snapshot에서 **한 멤버** data-dir 복원
3.  `--initial-cluster-state existing` 또는 recovery 문서의 `new` 토큰 절차로 재기동
4.  나머지 멤버를 `etcdctl member add` + data sync로 재합류

복구 후:

``` bash
export NAMRBD_ETCD_ROOT="<복구 대상과 동일한 root>"
namrbdctl validate-all --etcd-endpoints "$NAMRBD_ETCD_ENDPOINTS" --etcd-root "$NAMRBD_ETCD_ROOT"
```

## 9. 장애 시나리오와 대응

### 9.1 단일 etcd 멤버 다운 (3중 1)

기대:

- quorum 유지, read/write 계속
- 짧은 leader election 가능

대응:

- 멤버 재기동 또는 교체
- gateway attach·generation 오류율 관측

### 9.2 quorum 상실 (3중 2 이상 down)

기대:

- control-plane **쓰기 불가** (attach, generation bump, lease 갱신 실패)
- 이미 붙은 세션의 동작은 제품 경로에 따라 제한될 수 있음

운영 원칙:

- 임의 `etcdctl put/del`로 상태를 “고치려” 하지 않는다
- quorum 복구를 최우선
- TiKV metadata 문제와 **분리**해 진단 (`etcd` vs `TiKV`)

### 9.2.1 NAMRBD 증상 매핑

| 증상                       | 우선 확인                                |
|----------------------------|------------------------------------------|
| attach 실패                | `etcdctl endpoint health`, attachment 키 |
| two-gateway fencing 이상   | generation·attachment 일관성             |
| discovery 비어 있음        | gateway lease 키 `{root}/gateways/...`   |
| I/O는 되는데 attach만 실패 | etcd vs `sbs-service`/TiKV 분리          |

### 9.3 느린 디스크·quota 초과

증상: timeout, lease 만료 증가, `etcdserver: mvcc: database space exceeded`

대응:

- 디스크 확장, defrag, retention/compaction 정책 조정
- `--quota-backend-bytes` 상향(재기동 필요)

### 9.4 멤버 교체 (요약)

1.  `etcdctl member remove <id>` (죽은 멤버가 완전히 제거된 경우에만)
2.  새 호스트에 etcd 설치
3.  `etcdctl member add <name> --peer-urls=http://NEW:2380`
4.  출력된 `ETCD_INITIAL_CLUSTER` 환경으로 신규 멤버 기동 (`existing` 상태)
5.  `etcdctl move-leader` (선택) 후 cluster health 확인

멤버 추가·제거의 상세는 etcd 공식 [runtime reconfiguration](https://etcd.io/docs/v3.5/op-guide/runtime-configuration/)을 따른다.

## 10. 보안·TLS (요약)

프로덕션 내부망이라도 TLS를 쓰는 경우:

- `--listen-client-urls https://...`, peer URL도 TLS
- NAMRBD gateway / `namrbdctl`의 etcd 클라이언트가 TLS를 지원하는지 배포 빌드·플래그를 확인한다 (기본 예시는 plaintext `http`)

인증서 rotation 시: etcd 멤버 rolling → gateway 재기동 → smoke.

## 11. NAMRBD 검증 절차

| 목적 | 검증 내용 |
|----|----|
| Distributed SBS cluster (etcd + pebble/tikv) | 다중 호스트 etcd, SBS metadata, payload quorum, restart/degraded path를 포함한 cluster smoke |
| two-gateway attach fencing | 두 gateway가 같은 volume에 접근할 때 attachment, generation, single-writer fencing이 일관되게 유지되는지 확인 |
| Legacy RawKV tikv + etcd | legacy RawKV payload persistence가 필요한 경우에만 historical compatibility evidence로 확인 |

실행 시에는 `NAMRBD_ETCD_ENDPOINTS`와 `NAMRBD_ETCD_ROOT`를 명시하고, 현재 유지보수되는 cluster validation 절차가 summary JSON에 `ok_count`, `error_count`, first error, last error, attach fencing result를 기록하는지 확인합니다.

HA sign-off는 **서로 다른 호스트 3멤버 etcd**에서 two-gateway·attach fencing 시나리오를 통과한 결과로 판단한다.

## 12. 빠른 참조

### etcd vs TiKV PD

| 설정                       | 예시                  | 프로세스 |
|----------------------------|-----------------------|----------|
| `NAMRBD_ETCD_ENDPOINTS`    | `10.10.0.11:2379,...` | etcd     |
| `NAMRBD_TIKV_PD_ENDPOINTS` | `10.20.0.21:2379,...` | TiKV PD  |

같은 포트 번호라도 **호스트·프로세스가 다르면** 별도 클러스터다.

### control-plane 키 prefix (요약)

`{etcd-root}/volumes/...`, `{etcd-root}/gateways/...` — 상세 키 이름은 배포 환경의 gateway 설정값과 이 문서의 gateway control-plane 절차를 기준으로 확인한다.

## 13. 관련 문서

- [Installation Guide](installation-guide.md) — 전체 스택 설치
- [TiKV HA Guide](tikv-ha-cluster-install-operations-guide.md) — TiKV/PD HA
- [Metadata Authority](../architecture-manual/chapters/04-metadata-authority.md) — etcd/TiKV 역할·장애 모델
- [Admin Guide](admin-guide.md) — 운영 점검과 장애 대응
- etcd 운영: https://etcd.io/docs/v3.5/op-guide/clustering/
- etcd monitoring: https://etcd.io/docs/v3.5/op-guide/monitoring/

[\<- Architecture Index](../architecture-manual/index.md) [TiKV HA Guide -\>](tikv-ha-cluster-install-operations-guide.md)
