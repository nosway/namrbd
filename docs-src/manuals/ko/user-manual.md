사용자 매뉴얼

Edition boundary: Community edition 사용자 흐름과 Enterprise edition only 기능 섹션이 함께 포함되어 있습니다.

# NAMRBD 사용자 매뉴얼

이 매뉴얼은 NAMRBD 운영 흐름, Community 블록 접속, 기본 iSCSI 타겟 접속, 그리고 백업/원격 DR 정책, 성능 쓰로틀링(QoS), Vault KMS 페이로드 암호화, iSCSI HA/scale 기능의 Enterprise-only 경계를 안내합니다. 설치와 bring-up은 [Installation Guide](installation-guide.md), day-2 운영과 장애 대응은 [Admin Guide](admin-guide.md)를 우선 참고합니다.

현재 명령 구성:

- `namrbdctl`: host device create/attach/detach/status.
- `sbsctl`: volume, snapshot, restore, topology, maintenance, guardrail, enterprise backup/performance/security.
- `namrbd-debug`: 로우레벨 자원 점검 및 상세 디버깅.
- `namrbd-csi-driver`: Kubernetes CSI integration.
- `namrbd-iscsi-gateway`: 표준 Linux open-iscsi 기동장치를 위한 Community 기본 iSCSI target gateway.
- `sbsctl iscsi`: SBS-cluster-backed status/list/get을 위한 Community iSCSI product control surface. HA/failover 제품 지원은 Enterprise-only 영역이다.

`legacy metadata CLI`는 historical archive under `아카이브로 이관된 코어 레포지토리` 상태다. 현재 사용자/운영 workflow의 primary command가 아니다.

## 1. NAMRBD 제공 기능

NAMRBD는 다음과 같은 강점을 내장한 네트워크 기반 Linux 블록 디바이스를 기속 제공합니다:

- 다중 복제 및 엔터프라이즈 EC 백엔드 지원 볼륨
- 스냅샷, 클론, 즉각 복원 및 안전한 read-view 격리
- 증설 전용(grow-only) 볼륨 온라인 확장
- CSI/Kubernetes 동적 프로비저닝, VolumeSnapshot 복원, 블록 및 파일시스템 PVC 마운트, RWOP 경합 격리 검증
- Discard/Reclaim 엔진에 의한 복제본 및 EC 정렬 영역 대상 실질 디스카드(discard) 지원
- Zeroing 동작과 Discard 동작의 확실한 모니터링 구분
- 엔터프라이즈 백업 및 원격 재해 복구(DR) 컨트롤 플레인에 의한 백업 타겟, 정책, 수행 일지, 가용성 복원 검증, 보존 홀드 및 소거 계획 관리
- 엔터프라이즈 동적 성능 계층(QoS)과 운영 티어에 의한 성능 정책, 처리량 버짓 리스, 복원 웜업, 차분 인덱싱, 트랜잭션 저널 보호
- Vault KMS 마스터키 연동 페이로드 암호화에 의한 키 관리 정책, 복제본 암호화 검증, 기동 승인 및 폐기 키 접근 차단(fail-closed) 회로, 자동 로테이션, 감사 로그 수집
- 표준 Linux open-iscsi 기동장치 지원용 Community 기본 iSCSI 타겟 엑스포트 기능

현재 지원 경계:

- 쿠버네티스 디스카드 노출은 안전을 위해 기본 비활성화되어 있으며, 볼륨 마운트 옵션을 사용하기 전 명시적인 정합성 검증 필증(gate evidence) 수입을 요구합니다.
- 다중 호스트 쓰기 마운트(RWX)는 현재 블록 코어의 정식 기능 범위가 아닙니다.
- EC 프로필 전환은 온라인 상의 단순 메타데이터 플립(flipping)으로 해결되지 않으며, 향후 작업 영역에서 이관 및 재패킹(migration/repack) 프로세스로 고도화되어 제어됩니다.
- 현재 보안 baseline은 통합 mock 가상 공급자 보안 증적을 바탕으로 수립되었으며, Governance/WORM 지원은 블록 네이티브 파생 객체 보호와 유저스페이스 게이트웨이 기접수 쓰기 봉인 차단을 독립 범위로 다룹니다. 실시간 외부 KMS 네트워크 소거 및 디듀플리케이션은 향후 별도 과제로 연동됩니다.
- 기본 iSCSI 지원은 Linux open-iscsi를 필수 호환성 기준으로 주장합니다. Windows 환경은 가상 메모리 드라이브 및 세션 정리에 대한 부분 증적만을 내포하며, macOS는 전면 제외되어 있습니다.
- 커뮤니티 에디션은 수동 복제본 스냅샷, 수동 스냅샷 복원, 스냅샷 격리 보호, 복원 대상 용량 검증, 기초적인 삭제 및 종속성 보호, 그리고 최대 3개 distinct iSCSI-exported volumes 대상 기본 iSCSI gateway/CLI/LUN export로 제한됩니다. Enterprise 백업/DR 자동화, 동적 성능 계층, KMS 연동 암호화 및 보안 통제, Governance/WORM 보안 통제, 3개 초과 iSCSI export, unlimited export scale, iSCSI HA, MPIO/ALUA, 고급 보안/감사, 대규모 관측성, 원격 DR 자동화는 Enterprise-only이거나 별도 검증이 필요한 향후 영역입니다.

## 2. 빠른 시작

이 섹션은 시스템 관리자가 아래 컴포넌트를 정상 설치 및 가동했음을 전제로 합니다:

- `etcd`
- `sbs-service`
- `sbs-data`
- `namrbd-gateway`
- 호스트 커널 모듈

### 2.1 볼륨 생성

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

기대 checkpoint:

- `sbsctl volume status`가 요청한 volume id, size, block size, replication factor, created 또는 available 같은 사용 가능한 lifecycle state를 반환합니다.
- placement 또는 topology field가 요청한 policy와 일치하거나, host attachment 전에 명확한 validation error를 반환합니다.

### 2.2 호스트 로컬 블록 디바이스 매핑

``` bash
sudo insmod kernel/module/namrbd_blk.ko no_path_retry=fail
sudo insmod kernel/module/namrbd_ctrl.ko

namrbdctl create-device
namrbdctl config-rest --device 0 --server "1,gw01,9899,false,/api/v1"
```

### 2.3 연결

``` bash
namrbdctl attach \
  --device 0 \
  --host host-a \
  --volume 00000065 \
  --gateway http://gw01:9899

namrbdctl status --device 0
lsblk /dev/namrbd0
```

기대 checkpoint:

- `namrbdctl status`가 attached volume id, attachment id, generation, 하나 이상의 사용 가능한 gateway path를 보여줍니다.
- `lsblk`에서 filesystem 생성 전에 `/dev/namrbd0`와 기대 size가 확인됩니다.

### 2.4 디바이스 사용

파일시스템 예시:

``` bash
sudo mkfs.ext4 /dev/namrbd0
sudo mkdir -p /mnt/namrbd-demo
sudo mount /dev/namrbd0 /mnt/namrbd-demo
echo "hello namrbd" | sudo tee /mnt/namrbd-demo/hello.txt
sync
cat /mnt/namrbd-demo/hello.txt
```

기대 checkpoint:

- `cat /mnt/namrbd-demo/hello.txt`가 `hello namrbd`를 출력합니다.
- gateway, SBS, kernel log에 failed write, failed flush, stale attachment, path-plan generation mismatch가 없어야 합니다.

정리:

``` bash
sudo umount /mnt/namrbd-demo
namrbdctl detach --device 0 --host host-a --volume 00000065
namrbdctl destroy-device --device 0
sudo rmmod namrbd_ctrl
sudo rmmod namrbd_blk
```

## 3. Snapshot And Restore

스냅샷 및 볼륨 복원은 백엔드 스토리지 전용 동작이며, CSI 및 CLI 호출 경로는 최종적으로 동일한 백엔드 의미 구조로 바인딩됩니다.

중요 핵심 사양:

- 스냅샷 리드뷰(read-view)는 스케줄 생성 완료 시점 이후 완벽하게 불변(immutable) 상태로 격리됩니다.
- 원본 볼륨에 덮어쓰기(overwrite)가 발생해도 스냅샷 내용에는 어떤 영향도 주지 않습니다.
- 클론 볼륨에서 발생하는 변동분(delta) 기록은 근간이 되는 원본 스냅샷을 훼손시키지 않습니다.
- 스냅샷 기반 복원은 일반 마운트 가용 볼륨을 즉각 정립해 냅니다.
- 복원 대상 볼륨의 크기는 반드시 복원 원본 스냅샷의 원래 크기 이상으로 할당되어야 합니다.

운영자 수동 복구 명령어:

``` bash
sbsctl volume restore-from-snapshot \
  --snapshot-id <snapshot_id> \
  --volume-id <new_volume_id> \
  --size <size>
```

Kubernetes 사용자는 `VolumeSnapshot` 기반의 PVC를 생성함으로써 복원을 자동 처리하며, 내부 CSI 드라이버는 이를 `CreateVolumeFromSnapshot` 명령으로 바인딩합니다.

## 4. 엔터프라이즈 백업 및 원격 재해 복구(DR) <span class="edition-boundary-inline">Enterprise edition only</span>

엔터프라이즈 백업/DR 제어 레이어는 관련 상태를 `sbs-service`에 정식 기록합니다. 독자적인 백그라운드 스케줄러 기동, 파괴 소거, 혹은 무인 원격 DR 이관 기능은 제외됩니다. 엔터프라이즈 보안 통제를 통해 백업 산출물을 보호할 수 있으나, 최종 가용성은 여전히 정합성 점검 및 가용성 복원 점검(restore-drill) 증적 수집에 종속됩니다.

백업 타겟 인프라 및 스케줄 정책 수립:

``` bash
sbsctl backup target create \
  --target-id target-a \
  --type local_filesystem \
  --root /var/lib/namrbd-backup/target-a \
  --capacity-status ok

sbsctl backup policy create \
  --policy-id policy-a \
  --source-volume-id 00000065 \
  --target-id target-a \
  --schedule every:24h \
  --retention-count 2 \
  --retention-age-days 7
```

Record a run and mark the artifact available after restore drill evidence:

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
  --source-volume-id 00000065 \
  --source-snapshot-id <snapshot_id> \
  --snapshot-root-id <snapshot_root_id> \
  --restore-size 8K \
  --restore-drill-id restore-drill-readback-pass \
  --restore-drill-result kernel_readback_passed_artifact_transition_pending \
  --artifact-integrity-rechecked \
  --userspace-readback-matched \
  --kernel-readback-matched
```

백업 기동 상태 추적 및 파일 소거 안전성 검사:

``` bash
sbsctl backup status --policy-id policy-a --output json

sbsctl backup hold create \
  --hold-id hold-a \
  --target-kind artifact \
  --target-id artifact-a

sbsctl backup purge plan \
  --artifact-id artifact-a \
  --output json
```

`artifact_available=true` 표시는 백업 산출물이 데이터 정합성 재검사, 유저스페이스 게이트웨이 무결성 읽기, 그리고 커널 모듈을 통한 실제 블록 입출력 검증을 모두 성공적으로 패스했음을 의미합니다. 파일 소거 정책은 현재 단계에서 모의 테스트(dry-run)로만 기동됩니다.

## 5. Vault KMS 연동 페이로드 전 범위 완전 암호화 <span class="edition-boundary-inline">Enterprise edition only</span>

Enterprise security and compliance controls wrap already-correct storage paths. Security policy decides whether encrypted data can be accessed and how key, attach, backup, restore, audit, rotation, and crypto erase operations are recorded.

Common inspect commands:

``` bash
sbsctl security provider list --output json
sbsctl security policy list --output json
sbsctl security key list --output json
sbsctl security audit list --output json
sbsctl security crypto-erase list --output json
```

Typical policy setup in an enterprise environment:

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
  --volume-id 00000065 \
  --output json
```

중요 핵심 사양:

- 비활성화되거나 유실 또는 소멸된 암호화 키는 즉각적인 완벽 입출력 차단(fail-closed) 회로를 작동시키며, 일반 평문(plaintext) 기반의 우회 읽기/쓰기를 완벽히 불허합니다.
- Plaintext data keys, raw provider credentials, and payload samples must not be emitted in JSON, logs, or summaries.
- Rotation preserves old-object readability until re-encrypt or crypto erase intentionally changes access.
- Crypto erase removes access through key authority only after holds, protected artifacts, active attachments, and pending operations allow it.
- Live external KMS network/provider destroy evidence is not part of the closed 통합 mock 가상 공급자 baseline.

## 6. 기본 iSCSI Standard Access <span class="edition-boundary-inline">Community basic; Enterprise edition only scale/HA</span>

NAMRBD는 표준 블록 기동장치를 위한 선택형 iSCSI target gateway를 포함합니다. 이 gateway는 protocol frontend이며, volume lifecycle, committed metadata, read-view identity, discard/reclaim, security decision은 NAMRBD/SBS authority에 남습니다.

Community edition은 기본 gateway, `sbsctl iscsi`, 최대 3개 distinct iSCSI-exported volumes 대상 LUN export를 포함합니다. 3개 초과 export, unlimited export scale, iSCSI HA, MPIO/ALUA, 고급 보안/감사, 대규모 관측/스케일 기능은 Enterprise-only입니다.

스토리지 관리자는 `namrbd-iscsi-gateway`를 활용해 준비된 블록 볼륨을 LUN으로 노출합니다:

``` bash
namrbd-iscsi-gateway \
  --backend=sbs \
  --portal <gateway_ip>:3260 \
  --serve \
  --sbs-endpoint <sbs_volume_service_host>:9444 \
  --volume-id 00000065 \
  --export-id iscsi-00000065 \
  --target-iqn iqn.2026-06.io.namrbd:iscsi.00000065 \
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

`--allow-gotgt-wildcard-listen` is required by the current gotgt listener behavior and should be used only in a controlled validation or deployment network.

A Linux user with open-iscsi can discover and log in after the operator confirms the target IQN and portal:

``` bash
sudo systemctl enable --now iscsid
sudo iscsiadm -m discovery -t sendtargets -p <gateway_ip>:3260
sudo iscsiadm -m node -T iqn.2026-06.io.namrbd:iscsi.00000065 -p <gateway_ip>:3260 --login
sudo iscsiadm -m session -P 3
ls -l /dev/disk/by-path/*iscsi*lun-0
```

현재 iSCSI compatibility closure는 Linux open-iscsi만을 claim합니다. Windows native initiator는 optional memory-backend success와 SBS-backed connection/log-cleanup evidence를 갖지만, full SBS-backed read/write/readback/flush/cleanup support는 아직 claim하지 않습니다. macOS support도 claim하지 않습니다.

Cleanup on the initiator:

``` bash
sudo iscsiadm -m node -T iqn.2026-06.io.namrbd:iscsi.00000065 -p <gateway_ip>:3260 --logout
sudo iscsiadm -m node -T iqn.2026-06.io.namrbd:iscsi.00000065 -p <gateway_ip>:3260 --op delete
```

## 7. Kubernetes User Flow

The current Kubernetes baseline includes:

- replicated and enterprise EC StorageClasses.
- VolumeSnapshotClass.
- 블록 디스크 및 파일시스템 마운트 기반의 PVC
- snapshot restore.
- 더 큰 크기로 복원하는 타겟 지정 및 원본 PVC 온라인 확장
- RWOP conflict smoke.

Typical user objects:

``` yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: namrbd-demo
spec:
  accessModes:
    - ReadWriteOnce
  storageClassName: namrbd-replicated
  resources:
    requests:
      storage: 1Gi
```

Snapshot restore follows the standard Kubernetes `VolumeSnapshot` and `dataSource` PVC flow. 설치 시 manifest rendering은 [Installation Guide](installation-guide.md), 운영 점검은 [Admin Guide](admin-guide.md)를 참고합니다.

Discard note:

- Default NAMRBD CSI manifests render no filesystem discard mount option.
- Administrators must set `NAMRBD_CSI_ENABLE_DISCARD=1` and provide `NAMRBD_CSI_DISCARD_VALIDATION_PROFILE` before enabling discard exposure.

## 8. Discard And WRITE_ZEROES <span class="edition-boundary-inline">Enterprise edition only true reclaim</span>

Discard/Reclaim distinguishes:

- `discard`: storage reclaim intent.
- `write zeroes`: logical zero write intent.

실질적인 디스크 공간 반환(True reclaim)은 요청 동작이 백엔드 공간 회수 지오메트리 단위에 정렬되었을 때만 수행됩니다:

- 복제본 공간 회수 지오메트리에 정렬된 영역
- EC 풀스트라이프/페이지 단위에 정렬된 영역

블록 기하 구조에 맞지 않는 비정형 혹은 일부 파일시스템 영역의 discard 요청은 `zero_fallback` 관측 수준으로만 수립되며, 실제 디스크 공간 즉각 반환으로 주장되지 않습니다.

Validated Discard/Reclaim gates는 replicated reclaim, EC reclaim, live kernel discard behavior를 각각 다룹니다. true reclaim 지원을 안내하기 전에는 배포된 kernel module에 대해 fresh `blkdiscard` 또는 filesystem `fstrim` evidence를 기록합니다.

## 9. 검증 명령

검증 명령은 설치한 제품 경로를 증명하기 위한 것입니다. Public 또는 release-facing validation record에는 build revision, binary/image tag, 재시작한 service, kernel module version, 변경 동작을 실행한 가장 작은 validation gate가 포함되어야 합니다.

필수 기록 필드:

| Field | 목적 |
|----|----|
| `ok_count` / `error_count` | Smoke 또는 fixture가 성공적으로 완료되었는지 보여줍니다. |
| first error / last error | Private log 없이도 실패를 재현하고 분석할 수 있게 합니다. |
| deployment state | gateway, SBS service, SBS data, CSI, iSCSI, kernel component가 rebuild 또는 restart 되었는지 기록합니다. |
| mode evidence | 테스트에 사용한 backend, edition, StorageClass, reclaim policy, iSCSI portal, initiator를 식별합니다. |
| readback evidence | snapshot restore, backup artifact, iSCSI LUN, block-device write가 의도한 경로로 다시 읽혔는지 증명합니다. |
| unsupported scope | skip, future, Enterprise-only 경로를 support claim으로 오해하지 않도록 분리합니다. |

기능별 권장 gate:

- Community block path: volume create/status, attach/status, filesystem write/readback, detach, cleanup.
- CSI path: manifest render/lint, Kubernetes apply state, PVC bind, pod write/readback, snapshot restore, node readiness.
- iSCSI path: Linux open-iscsi discovery, login, guarded LUN selection, write/readback, flush observation, logout, target cleanup.
- Discard/reclaim path: replicated reclaim, Enterprise EC 사용 시 EC reclaim, kernel `blkdiscard` 또는 filesystem `fstrim`, alignment evidence.
- Enterprise Backup/DR 및 security path: restore-drill readback, artifact availability, hold/purge guardrail, key admission, fail-closed read, rotation, audit, crypto erase evidence.

Generated public/community export artifact validation remains a separate release artifact check unless explicitly run and recorded.

## 10. Troubleshooting

### 10.1 Kernel Module Build

Symptoms:

- missing kernel headers.
- `/lib/modules/$(uname -r)/build` does not exist.

Checks:

``` bash
uname -r
ls /lib/modules/$(uname -r)/build
make -C kernel/module
```

### 10.2 Attach Or Device Status

Checks:

``` bash
namrbdctl status --device 0
lsblk /dev/namrbd0
dmesg | tail -n 100
```

Capture attachment id, generation, path plan revision, device size, and runtime path status when reporting an issue.

### 10.3 Gateway Or SBS Authority

Checks:

``` bash
curl -fsS http://gw01:9899/healthz
sbsctl cluster status --output json
sbsctl volume status --volume-id <volume_id> --output json
```

게이트웨이는 기정의된 `--sbs-service-endpoint`를 통해서만 `sbs-service`에 접근해야 합니다. 게이트웨이가 raw SBS TiKV 메타 플래그를 직접 제어하는 비정형 경로는 역사적 개발 환경 전용이며, 상용 런타임에서 권장되지 않습니다.

### 10.4 Kubernetes

Checks:

``` bash
kubectl get nodes
kubectl -n namrbd-system get pods
kubectl get pvc,pv,volumesnapshot -A
```

Collect PVC/PV handles, pod events, CSI controller logs, CSI node logs, and the Discard/Reclaim summary path.

### 10.5 iSCSI

Checks:

``` bash
sbsctl iscsi status gateway --json
sbsctl iscsi status target --target-iqn <target_iqn> --json
sudo iscsiadm -m session -P 3
ls -l /dev/disk/by-path/*iscsi*lun-0
```

Collect target IQN, portal, LUN id, initiator IQN/vendor/version, SCSI status/sense, gateway summary JSON, operation JSONL, and whether `iscsi_gateway_restarted=true` for the run.

### 10.6 Smoke Failure

Record:

- failed command.
- summary `result`, `ok_count`, `error_count`, `skipped_count`.
- `first_error` and `last_error`.
- stdout/stderr log paths.
- deploy/reload/restart state.

## 11. FAQ

### 레거시 메타데이터 제어 CLI를 여전히 사용하나요?

No. It remains archived for historical reference and guardrail scans. Use `namrbdctl`, `sbsctl`, and `namrbd-debug`.

### Does current Kubernetes install enable discard by default?

No. Backend and kernel discard are validated, but CSI manifests keep discard disabled by default. Enable it only with explicit gate evidence.

### 가동 중인 볼륨의 EC 프로필을 온라인으로 변경할 수 있나요? <span class="edition-boundary-inline">Enterprise edition only EC</span>

No. EC profile/geometry is effectively create-time immutable in the current baseline. Treat profile changes as future controlled migration/repack work.

### Is RWX supported?

Not as a current block-core feature. RWX should be evaluated later as a separate filesystem/share-manager layer.

### What is the encryption/KMS boundary? <span class="edition-boundary-inline">Enterprise edition only</span>

The enterprise 통합 mock 가상 공급자 security baseline closes deployed 통합 mock 가상 공급자 follow-up gates. Do not claim live external KMS network/provider destroy, dedupe, or broader kernel readback beyond the recorded gates unless fresh evidence is attached.

### Governance/WORM의 통제 한계 영역은 어디인가요? <span class="edition-boundary-inline">Enterprise edition only</span>

Governance/WORM은 제한 범위 하에 정식 지원됩니다. 적용되는 통제 범위는 블록 네이티브 기반 파생 오브젝트 생성 통제와 게이트웨이 기접수 쓰기 전면 거부(write rejection) 필터링에 제한됩니다. 별도의 금융 규격 정식 인증, S3/Azure 호환 API 제어, 일반 쓰기 가능한 실시간 마운트 볼륨 수준의 통제, 공개 거버넌스 API 연동 등록, iSCSI/NVMe 자체 잠금 통제, 랜섬웨어 무인 복구, 혹은 크로스 리전 DR 자동화 기능은 내포하지 않습니다.

### iSCSI 연동 기술은 Linux 외의 다른 운영체제 접속을 보장하나요?

No. 현재 iSCSI support claim은 Linux open-iscsi evidence를 요구합니다. Windows native initiator는 optional memory-backend success와 SBS-backed connection/log-cleanup evidence를 갖지만, full SBS-backed Windows I/O와 macOS support는 future evidence track입니다.

## 12. Legacy Notes

The following content is historical or development-only:

- Redis payload backend examples.
- legacy pre-release smoke scripts.
- 게이트웨이가 직접 SBS 로우 메타데이터를 조작하기 위한 부트스트랩 플래그
- 과거 아키텍처 기반의 레거시 메타데이터 CLI 명령어 경로

Do not present these as the standard Enterprise Service user path. Historical references may remain in archived docs and guardrail inventories.

## 13. Offline Copy

오프라인 사본이 필요하면 이 HTML 페이지에서 브라우저의 print/PDF export 기능을 사용합니다. Public Community documentation은 HTML로 배포됩니다.

## 14. Related Public Documents

| Document | Purpose |
|----|----|
| [Installation Guide](installation-guide.md) | Community 및 Enterprise install and bring-up |
| [Admin Guide](admin-guide.md) | Operations, observability, troubleshooting |
| [Interface Specifications](../architecture-manual/chapters/appendix-interface-specifications.md) | Gateway, iSCSI, SBS, operator surface boundary |
| [Edition Boundaries](../architecture-manual/chapters/17-edition-and-release-boundaries.md) | Community 및 Enterprise product scope, including iSCSI limits |
| [Kubernetes/CSI Case](../architecture-manual/chapters/16-kubernetes-csi-integration-case.md) | CSI provisioning, snapshot restore, Kubernetes usage boundaries |
| [Zero, Discard, And Reclaim](../architecture-manual/chapters/12-zero-discard-and-reclaim.md) | Discard, write-zeroes, reclaim behavior |
| [Read Views, Snapshots, And Clones](../architecture-manual/chapters/10-read-views-snapshots-and-clones.md) | Snapshot/clone read-view behavior |

[\<- Architecture Index](../architecture-manual/index.md) [Installation Guide -\>](installation-guide.md)
