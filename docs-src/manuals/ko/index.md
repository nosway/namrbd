분산 블록 스토리지 문서

Edition boundary: Community edition 시작점과 Enterprise edition only 기능 요약이 함께 포함되어 있습니다.

# NAMRBD 분산 블록 스토리지 포털

<div class="summary" markdown="1">

NAMRBD는 Network Attached Multipath Resilient Block Device의 약어로, 네이티브 Linux 블록 디바이스 경로, Kubernetes CSI 연동, 선택형 표준 iSCSI 타겟 게이트웨이를 제공하는 분산 블록 스토리지입니다.

커뮤니티 에디션은 기본 단일 경로 블록 장치 연결, 수동 스냅샷 복구, `namrbd-iscsi-gateway`, `sbsctl iscsi`, 최대 3개 distinct iSCSI-exported volumes 대상 기본 LUN export를 포함합니다. 엔터프라이즈 에디션은 원격 DR 오케스트레이션과 복제 자동화, 동적 성능 제한(QoS), Vault 연동 페이로드 암호화, iSCSI HA/MPIO/ALUA, 고급 보안/감사, 대규모 관측/스케일 기능을 담당합니다.

</div>

## 1. 제품 포지셔닝

NAMRBD는 분산 스토리지 기판인 SBS 기술을 공유하는 초저지연 분산 블록 스토리지 제품입니다. S3 오브젝트 전용 규격을 제공하는 NAMROS 제품과는 달리, 리눅스 원격 호스트 기동 장치(`/dev/namrbdX`) 혹은 쿠버네티스 CSI 프로비저너를 통해 컨테이너 영속 스토리지로 직접 연결되는 정밀 블록 입출력 장치를 타겟으로 삼습니다.

## 2. 지원 구성 형태

| 구성 | 목적 | 핵심 종속성 | 에디션 범위 |
|----|----|----|----|
| Local Single-Node Validation | 개발자 로컬 가상 검증 및 스모크 테스트 | 단일 `namrbd-gateway`, 로컬 Pebble 메타 스토어 | <span class="badge">Community</span> |
| Kubernetes CSI Cluster | 쿠버네티스 CSI 볼륨 다이나믹 프로비저닝 | TiKV 메타 데이터, sbsctl, `namrbd-csi-driver` | <span class="badge">Community</span> |
| Basic iSCSI Target Access | 단일 타겟 경로를 통한 표준 Linux open-iscsi LUN export | `namrbd-iscsi-gateway`, `sbsctl iscsi`, TCP/3260 | <span class="badge">Community</span> 최대 3개 distinct exported volumes |
| Enterprise iSCSI HA/Scale | 대규모 이중화 타겟 게이트웨이 연동 | etcd, `namrbd-iscsi-gateway`, host multipath tooling | <span class="badge enterprise">Enterprise</span> |
| Remote DR Automation | 크로스 리전 복제, failover 계획, 복구 오케스트레이션 | 원격 게이트웨이, 정책 자동화, SBS-EC 백엔드 | <span class="badge enterprise">Enterprise</span> |

## 3. Community와 Enterprise 차이

| 기능 범주 | Community | Enterprise |
|----|----|----|
| 블록 볼륨 생성 및 마운트 (`namrbdctl`) | 기본 탑재 (로컬 검증) | 기본 탑재 (최적 커널 로딩) |
| 수동 스냅샷 및 롤백 복구 | 지원 | 지원 |
| Kubernetes CSI 동적 볼륨 매핑 | 기본 플러그인 탑재 | 정식 스토리지 클래스 바인딩 (QoS 포함) |
| Remote DR 및 정책 자동화 | 지원 대상 제외 | <span class="badge enterprise">Enterprise</span> 원격 복제, failover workflow, 복구 정책 자동화 |
| KMS 연동 페이로드 실시간 전 범위 암호화 | 지원 대상 제외 | <span class="badge enterprise">Enterprise</span> Vault 연동 및 Fail-Closed 회로 |
| 기본 iSCSI Target Access | 포함: `namrbd-iscsi-gateway`, `sbsctl iscsi`, 기본 LUN export, 최대 3개 distinct exported volumes | 포함, 더 큰 export scale은 Enterprise 정책 적용 |
| iSCSI HA / MPIO / ALUA / Scale Operations | 지원 대상 제외 | <span class="badge enterprise">Enterprise</span> HA, MPIO/ALUA, 고급 보안/감사, 대규모 관측성 |

## 4. 역할별 시작점

NAMRBD를 다루는 전문가의 직무와 목표에 최적화된 문서를 선택하십시오.

<div class="cards" markdown="1">

<div id="developer-path" class="section card" markdown="1">

### GitHub 개발자

소스, 빌드, 기여 경로

Community 소스 트리에서 명령 바이너리를 빌드하고 edition-boundary 검사를 실행한 뒤, 아키텍처 매뉴얼로 저장소 계약을 이해하고 코드 변경을 시작합니다.

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

리눅스 아웃오브트리 커널 dkms 자동 드라이버 컴파일, etcd 3중화 클러스터 관리, 기본 iSCSI 타겟 접속 구성, Enterprise-only iSCSI HA 경계, sbsctl 볼륨 백그라운드 힐링 런북을 학습합니다.

<a href="admin-guide.md" class="btn">열기 →</a>

</div>

</div>
