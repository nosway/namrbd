분산 블록 스토리지 문서

# NAMRBD 분산 블록 스토리지 포털

<div class="summary" markdown="1">

NAMRBD는 Network Attached Multipath Resilient Block Device의 약어로, 네이티브 Linux 블록 디바이스 경로, Kubernetes CSI 연동, 선택형 표준 iSCSI 타겟 게이트웨이를 제공하는 오픈 소스 분산 블록 스토리지 플랫폼입니다.

공개 소스는 복제 스토리지 플랫폼과 호스트, 게이트웨이, SBS, CSI, iSCSI, 운영 표면을 제공합니다. 소스 공개 여부와 v1.0 지원 검증 여부는 서로 다르므로 구성 선택 전에 [기능 상태](../../feature-status.md)를 확인하십시오. 고급 Enterprise 기능은 개발·검증 중이며 일반 제공 약속이 아닙니다.

</div>

## 1. 제품 포지셔닝

NAMRBD는 분산 스토리지 기판인 SBS 기술을 공유하는 초저지연 분산 블록 스토리지 제품입니다. S3 오브젝트 전용 규격을 제공하는 NAMROS 제품과는 달리, 리눅스 원격 호스트 기동 장치(`/dev/namrbdX`) 혹은 쿠버네티스 CSI 프로비저너를 통해 컨테이너 영속 스토리지로 직접 연결되는 정밀 블록 입출력 장치를 타겟으로 삼습니다.

## 2. 배포 및 평가 구성

| 구성 | 목적 | 핵심 종속성 | 현재 상태 |
|----|----|----|----|
| 로컬 단일 노드 quickstart | 개발자 평가 및 smoke 검증 | `namrbd-gateway`, `sbs-service`, `sbs-data`, 로컬 메타데이터 | 공개 개발 워크플로 |
| 복제 userspace gateway | 복제 블록 볼륨 서비스 | SBS 클러스터, 메타데이터 authority, `namrbd-gateway` | v1.0에서 검증된 볼륨 경로 |
| Kubernetes CSI 클러스터 | 동적 영속 볼륨 프로비저닝 | SBS 클러스터, `sbsctl`, `namrbd-csi-driver` | 공개 integration preview, v1.0 지원 미검증 |
| 기본 iSCSI target access | 단일 target path를 통한 Linux open-iscsi LUN export | `namrbd-iscsi-gateway`, `sbsctl iscsi`, TCP/3260 | 공개 integration preview, 최대 3개 distinct exported volumes |
| Linux kernel block path | 네이티브 `/dev/namrbdX` 연결 | 일치하는 kernel header, kernel module, gateway | 소스 공개, kernel I/O는 v1.0 지원 범위 밖 |

## 3. 개발 중인 Advanced Features

NAMRBD는 Enterprise edition을 위해 erasure-coded storage, 자동화된
backup/recovery, 보안·KMS·governance, 성능·QoS, 고급 iSCSI HA/scale,
remote replication/DR, data mobility/repack, deduplication을 개발하고
검증하고 있습니다. 이는 개발 방향에 대한 설명이며 일반 제공, 호환성,
성능 또는 지원 약속이 아닙니다. 간략한 기능 설명과 현재 제한은
[기능 상태](../../feature-status.md)를 참고하십시오.

## 4. 역할별 시작점

NAMRBD를 다루는 전문가의 직무와 목표에 최적화된 문서를 선택하십시오.

<div class="cards" markdown="1">

<div id="developer-path" class="section card" markdown="1">

### GitHub 개발자

소스, 빌드, 기여 경로

공개 소스 트리에서 명령 바이너리를 빌드하고 공개 검증 gate를 실행한 뒤, 아키텍처 매뉴얼로 저장소 계약을 이해하고 코드 변경을 시작합니다.

<a href="installation-guide.md#2-developer-build-and-test" class="btn">개발자 빌드 경로 열기 →</a>

</div>

<div class="section card" markdown="1">

### Kubernetes Operator

쿠버네티스 컨테이너 운영자 경로

NAMRBD CSI 드라이버 구동, StorageClass 파라미터 튜닝, 컨테이너 볼륨 정합성 공간 회수(Discard/WRITE_ZEROES) 및 영속 볼륨 볼륨스냅샷 복원 YAML 설정을 파악합니다.

<a href="user-manual.md" class="btn">열기 →</a>

</div>

<div class="section card" markdown="1">

### Storage Infra Administrator

인프라 시스템 관리자 경로

리눅스 아웃오브트리 kernel module 빌드, etcd HA 클러스터 관리, 기본 iSCSI target access 구성, 현재 검증 경계, SBS 볼륨 운영 절차를 확인합니다.

<a href="admin-guide.md" class="btn">열기 →</a>

</div>

</div>
