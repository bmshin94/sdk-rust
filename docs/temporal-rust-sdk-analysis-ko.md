# Temporal Rust SDK 전수조사 & 활용 전략 정리 (한국어)

> 작성: 카리나 (Claude Code) · 요청: @bmshin94 · 정리일: 2026-09-28

## 0. 레포지토리 정보

| 항목 | 값 |
|---|---|
| 이 레포 (포크) | https://github.com/bmshin94/sdk-rust |
| 원본 (upstream) | https://github.com/temporalio/sdk-rust |
| 작업 브랜치 | `claude/determined-planck-q6zb85` |
| 라이선스 | MIT (`LICENSE.txt`) |
| 주요 크레이트 | `temporalio-sdk` 1.0.0 / `temporalio-sdk-core` 0.9.0 |
| 툴체인 | Rust edition 2024, rust-version 1.92.0 |
| 규모 | Rust 파일 319개 / 약 163,000줄 / protobuf 97개 |
| 참고 문서 | `README.md`, `ARCHITECTURE.md`, `AGENTS.md`, `CONTRIBUTING.md`, `arch_docs/` |
| 공식 사이트 | https://temporal.io · 블로그: https://temporal.io/blog/why-rust-powers-core-sdk |
| crates.io / docs.rs | https://crates.io/crates/temporalio-sdk · https://docs.rs/temporalio-sdk |

---

## 1. 이게 뭐하는 건가

**Temporal의 공식 Rust SDK + Core SDK** 레포지토리다.

Temporal은 **Durable Execution(내구성 있는 실행)** 플랫폼이다. 한 줄 요약:

> 서버가 죽어도, 프로세스가 재시작돼도, 며칠이 걸려도 내 코드가 중단된 지점부터 정확히 이어서 실행되게 해주는 엔진.

동작 원리는 **이벤트 소싱 + 리플레이**다. Temporal 서버가 워크플로의 모든 실행 이력(History)을 이벤트로 저장하고, 워커가 죽었다 살아나면 그 이력을 재생(replay)해 메모리 상태를 복원한다. 그래서 아래 코드가 3일 동안 살아있어도 되고, 그 사이 재배포를 해도 살아남는다.

```rust
ctx.timer(Duration::from_secs(3 * 24 * 3600)).await;      // 3일 대기
let r = ctx.execute_activity(MyActivities::send_email, data, opts)?.await?;
```

### 가장 중요한 사실

이 Rust 코어가 **TypeScript / Python / .NET / Ruby SDK 전부의 엔진**이다 (README 명시).
- Python SDK → PyO3로 래핑
- TypeScript SDK → neon으로 래핑
- .NET SDK → `sdk-core-c-bridge`의 C ABI 직접 사용

즉 이 레포는 "Rust로 Temporal 쓰는 법"과 "전 세계 Temporal SDK의 공용 엔진" 두 가지를 동시에 담고 있다.

---

## 2. 폴더/크레이트 전수조사 (`crates/` 10개)

| 크레이트 | 크기 | 역할 |
|---|---|---|
| `sdk-core` | 3.8M | **심장부.** gRPC 폴링, 상태머신, 워크플로 캐시, 재시도 로직, 텔레메트리, 리플레이, ephemeral server |
| `protos` | 3.9M | Temporal 서버 통신용 protobuf 정의 97개. `api_upstream`은 git subtree |
| `client` | 888K | 서버 접속 클라이언트. envconfig, 스케줄, mTLS, 프록시, 인터셉터, 재시도 |
| `sdk` | 712K | Rust 개발자용 고수준 API (`#[workflow]`, `#[activity]`), 워커, 리플레이어, 테스트 지원 |
| `workflow` | 544K | 워크플로 저작 API + WASM 컴포넌트 지원 (`wit/`) |
| `common` | 392K | 공통 타입, payload 직렬화, payload visitor 생성 |
| `sdk-core-c-bridge` | 328K | **C FFI 브릿지** — 타 언어 SDK 진입점 (`include/`에 헤더) |
| `common-wasm` | 280K | WASM 안전한 공통 타입/직렬화 |
| `macros` | 144K | proc-macro 구현체 (`#[workflow]`, `#[activity]`, `#[signal]` 등) |
| `changelog-release-notes` | 48K | 릴리즈 노트 생성 도구 |

기타: `arch_docs/`(설계문서: sticky queues, workflow task chunking, SDK 입문), `etc/`(docker, prometheus, otel 설정), `.github/workflows/`(per-pr, heavy, changelog, release), `.cargo/config.toml`(cargo alias 정의).

### 아키텍처 요약

```
┌──────────── Temporal 서버 (이력 저장소) ────────────┐
└───────────────────────▲──┬──────────────────────────┘
                 gRPC   │  │   (protos 크레이트)
                        │  ▼
┌─────────────── crates/sdk-core (엔진) ──────────────┐
│  태스크 폴링 · 상태머신 · 재시도 · 캐시 · 메트릭     │
└──┬──────────────────────────────────┬───────────────┘
   │ 직접                              │ C ABI (sdk-core-c-bridge)
   ▼                                  ▼
crates/sdk (Rust)              Python / TypeScript / .NET / Ruby SDK
```

---

## 3. 핵심 개념 (Workflow vs Activity)

| | **Workflow** | **Activity** |
|---|---|---|
| 역할 | 오케스트레이션(순서 지시) | 실제 부수효과(IO, API, DB) |
| 결정성 | **필수** (몇 번 재생해도 동일해야 함) | 불필요 |
| 금지 사항 | 직접 IO, 스레드, 난수, 시스템 시간, 전역 가변 상태 | 없음 |
| 대체 API | `ctx.workflow_time()`, `ctx.timer()`, `ctx.execute_activity()` | - |

### 코드 예시

```rust
#[workflow]
pub struct HelloWorldWorkflow;

#[workflow_methods]
impl HelloWorldWorkflow {
    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>, name: String) -> WorkflowResult<String> {
        let greeting = ctx.execute_activity(
            GreetingActivities::greet, name,
            ActivityOptions::start_to_close_timeout(Duration::from_secs(10)),
        )?.await?;
        Ok(greeting)
    }
}

#[activities]
impl GreetingActivities {
    #[activity]
    pub async fn greet(_ctx: ActivityContext, name: String) -> Result<String, ActivityError> {
        Ok(format!("Hello, {name}!"))
    }
}
```

메서드 어트리뷰트: `#[init]`(생성자, 선택) · `#[run]`(필수) · `#[signal]` · `#[query]`(동기 전용) · `#[update]`

### 이 SDK의 인상적인 설계 포인트

- **런타임 비결정성 탐지기 기본 활성화** — `tokio::time::sleep`, `tokio::spawn`, `tokio::net`, `tokio::sync` 채널 등 비SDK wake를 감지해 워크플로 태스크를 실패시킨다. `WorkerOptions::detect_nondeterministic_futures(false)`로 해제 가능.
- **결정적 동시성 프리미티브** — `workflows::select!`, `join!`, `join_all` (선언 순서대로 폴링).
- **WASM 워크플로** — `wasm-workflows` 피처 + wasmtime 44 + WIT 컴포넌트 모델.
- **`WorkflowReplayer`** — 저장된 히스토리로 코드 변경의 호환성을 CI에서 검증.
- **취소 토큰 계층** — 워크플로 루트 토큰 / 자식 토큰 / 분리된 정리용 토큰.
- **`ctx.patched("id")`** — 실행 중인 워크플로를 깨지 않고 로직 버저닝.

---

## 4. 설치 및 사용법

### 사전 준비물

```bash
# Rust 1.92.0+ (rust-toolchain.toml로 고정)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
# protoc 필수
brew install protobuf              # macOS
apt install -y protobuf-compiler   # Ubuntu
# Temporal CLI (로컬 서버)
brew install temporal
```

### 이 레포 빌드/검증 (AGENTS.md 기준 — 반드시 이 alias 사용)

```bash
cargo build                   # 전체 빌드
cargo test                    # 유닛 테스트
cargo lint                    # clippy (clippy 직접 호출 금지)
cargo test-lint               # 테스트 대상 clippy
cargo +nightly fmt            # 포맷 (nightly 필요)
cargo integ-test              # 통합테스트 (임시 서버 자동 기동)
cargo doc --workspace --all-features
```

### 예제 실행 (가장 추천)

```bash
temporal server start-dev      # 터미널1: 서버 + 웹UI(http://localhost:8233)

cd crates/sdk                  # 터미널2: 워커
cargo run --features examples --example hello-world-worker

cargo run --features examples --example hello-world-starter   # 터미널3
```

예제 18종: hello_world, activity_heartbeating, activity_interceptor, standalone_activities, timer_examples, message_passing, child_workflows, continue_as_new, saga, patching, local_activities, search_attributes, updatable_timer, polling, cancellation, encryption, schedules, wasm_workflows

### 내 프로젝트에서 라이브러리로 사용

```toml
[dependencies]
temporalio-sdk = "1.0"
temporalio-client = "1.0"
tokio = { version = "1", features = ["full"] }
```

### 피처 플래그

| 피처 | 용도 |
|---|---|
| `envconfig` | 환경변수 / `temporal.toml` 로 접속 설정 (기본 on) |
| `prometheus` / `otel` | 메트릭 내보내기 |
| `testing` | `ActivityEnvironment`, 로컬 dev 서버 라이프사이클 |
| `experimental` | 변경 가능한 최신 API |
| `dynamic-tls` | mTLS 클라이언트 인증서 자동 로테이션 |
| `wasm-workflows` | WASM 워크플로 (wasmtime) |

---

## 5. 자주 나오는 질문 정리

### Q. 플러그인? 스킬? MCP?

**전부 아니다.** 이것은 순수 **Rust 라이브러리(크레이트) 모음 = SDK**다.

| 구분 | 이 레포 |
|---|---|
| Claude 플러그인 | ❌ |
| Claude 스킬 | ❌ |
| MCP 서버 | ❌ |
| SDK / 라이브러리 | ✅ |

혼동 주의:
- 루트의 `CLAUDE.md`는 **Claude Code 설정 파일**(오빠가 PR #1로 추가한 카리나 페르소나)이며 레포 본체 기능과 무관하다.
- `crates/sdk/src/plugins.rs`, `crates/client/src/plugins.rs`의 "plugin"은 **Temporal 내부 확장 훅**이며 Claude 플러그인과 전혀 다른 개념이다.
- 다만 **이것으로 MCP 서버를 만드는 것은 가능하고, 매우 유망하다** (아래 수익화 2번).

### Q. API 토큰이 필요한가?

- **로컬 개발: 불필요.** `temporal server start-dev` → `http://localhost:7233`, namespace `default`, 인증 없음.
- **Temporal Cloud: 필요.** `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, `TEMPORAL_API_KEY` (또는 mTLS 인증서).
- **Anthropic API 키 / GitHub 토큰은 전혀 불필요.** 이 레포는 LLM과 무관한 인프라 SDK다.
- MIT 라이선스 오픈소스이므로 자체 호스팅 시 라이선스 비용 0원.

### Q. 왜 GitHub에서 유명한가?

1. **하나의 코어가 4개 언어 SDK를 구동** — Temporal 생태계의 Single Source of Truth.
2. **Temporal 자체가 백엔드 업계 스타** — Uber Cadence에서 파생, 대형 서비스들이 프로덕션 사용, "Durable Execution" 카테고리 창시.
3. **실전 Rust 아키텍처 교과서** — tokio, 상태머신, proc-macro, tonic/gRPC, C FFI, WASM, OpenTelemetry 총집합.
4. **화제의 블로그 포스트** — "Why Rust powers Temporal's new Core SDK".
5. **Rust SDK 1.0 + edition 2024 + WASM 워크플로** 라는 최신 화제성.

단, 유명한 것은 upstream `temporalio/sdk-rust`이고 포크(`bmshin94/sdk-rust`)는 별개다.

### Q. 로컬 에이전트 구축에 도움되는가? → 매우 그렇다

| LLM 에이전트의 고통 | Temporal의 해답 |
|---|---|
| API 429/500/타임아웃 | Activity 자동 재시도 (옵션 설정만) |
| 10단계 중 7단계에서 죽음 | 히스토리 리플레이로 7단계부터 재개 |
| 사람 승인 대기 필요 | `#[signal]` / `#[update]` 로 며칠이고 대기 |
| 토큰 비용 태운 결과 유실 | 모든 단계 결과가 서버에 영구 저장 |
| 무한 루프 / 폭주 | 타임아웃 + 취소 토큰 + `continue_as_new` |
| 디버깅 불가 | 웹 UI 타임라인 = 감사 로그 무료 제공 |
| 멀티 에이전트 협업 | 자식 워크플로 |
| 도구 실행 격리 | WASM 워크플로 샌드박스 |

에이전트 루프 설계 골격:

```rust
#[workflow]
pub struct AgentLoop { messages: Vec<Message>, awaiting_approval: bool }

#[workflow_methods]
impl AgentLoop {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>, goal: String) -> WorkflowResult<String> {
        for _ in 0..MAX_STEPS {
            let plan = ctx.execute_activity(LlmActivities::call_claude,
                ctx.state(|s| s.messages.clone()),
                ActivityOptions::start_to_close_timeout(Duration::from_secs(60)))?.await?;
            match plan.action {
                Action::Done(a) => return Ok(a),
                Action::Dangerous(cmd) => {                     // 사람 승인 대기
                    ctx.state_mut(|s| s.awaiting_approval = true);
                    ctx.wait_condition(|s| !s.awaiting_approval).await?;
                    ctx.execute_activity(ToolActivities::run, cmd, opts)?.await?;
                }
                Action::Tool(c) => {
                    let out = ctx.execute_activity(ToolActivities::run, c, opts)?.await?;
                    ctx.state_mut(|s| s.messages.push(out.into()));
                }
            }
        }
        ctx.continue_as_new(&summarize(ctx), Default::default())?;  // 컨텍스트 리셋 후 계속
    }

    #[signal] fn approve(&mut self, _ctx: &mut WorkflowContext<Self>) { self.awaiting_approval = false; }
    #[query]  fn progress(&self, _ctx: &WorkflowContextView) -> u32 { self.messages.len() as u32 }
}
```

로컬에서 전부 무료로 구동 가능하고, Rust라서 단일 바이너리 배포가 되며, 웹 UI가 그대로 관제탑이 된다.

### Q. React나 PHP로 만들 수 있는가?

**안 되는 것**
- React(브라우저)에서 워크플로 실행 ❌ — 워커는 gRPC 상시 연결/폴링이 필요하고, 자격증명을 프론트에 두면 안 된다.
- Rust 코어를 PHP에서 직접 호출 ❌ — 이 레포에 PHP 브릿지는 없다.

**정석 아키텍처**

```
[React 프론트] ──HTTP/REST──▶ [백엔드 API] ──gRPC──▶ [Temporal 서버]
  시작 버튼/진행률/승인          start_workflow            │
                                signal / query            ▼
                                                    [Rust 워커 프로세스]
```

- React: 시작 버튼, 진행 상황 표시, 승인 버튼 → 백엔드 API 호출만
- 백엔드(Node/PHP/Rust): `start_workflow` / `signal` / `query` 중계
- 워커: 별도 프로세스 (Rust 권장)

**PHP 3가지 길**: ① 커뮤니티 PHP SDK(Spiral Scout `temporal-php`, RoadRunner 기반)로 워크플로까지 PHP 작성 ② PHP는 클라이언트 전용 + 워커는 Rust/Python (가장 안전) ③ gRPC 직접 호출 (비권장)

**추천 스택**: React(Next.js) 프론트 + Next.js API Routes 또는 기존 PHP + 🦀 Rust 워커(이 레포) + 로컬 dev 서버 → 이후 Cloud/자체호스팅

---

## 6. 수익화 아이디어 8선

| # | 아이디어 | 수익 모델 | MVP | 추천도 |
|---|---|---|---|---|
| 1 | **Durable AI Agent 백엔드 SaaS** — 안 죽는 에이전트 실행 런타임 | 실행 건당 $0.01~ + 월 $49/$199/$999 | 4~6주 | ⭐⭐⭐⭐⭐ |
| 2 | **Temporal × MCP 브릿지** — 워크플로를 MCP 툴로 노출 (start/status/signal/cancel) | OSS 무료 + 매니지드 $29~299/월 + 컨설팅 | **2주** | ⭐⭐⭐⭐⭐ |
| 3 | **기술 콘텐츠/강의** — 한국어 자료 선점 | 강의 ₩99,000 × N, 전자책, 뉴스레터 | 즉시 | ⭐⭐⭐⭐ |
| 4 | **업종별 워크플로 템플릿 판매** — 이커머스/핀테크/AI/온보딩 | $199~$499 + 커스터마이징 업셀 | 2~3주 | ⭐⭐⭐⭐ |
| 5 | **컨설팅 / 마이그레이션 대행** — 국내 전문가 공급 부족 | 리뷰 ₩300~500만, PoC ₩1,000~2,000만, 리테이너 ₩200~500만/월 | 즉시 | ⭐⭐⭐⭐⭐ |
| 6 | **셀프호스팅 운영/옵저버빌리티 도구** — 비용분석, 리플레이 CI, SLA 알림 | 팀당 $99~499/월 + 설치형 | 4주 | ⭐⭐⭐ |
| 7 | **WASM 멀티테넌트 자동화 플랫폼** — 고객/AI 생성 코드를 샌드박스 실행 | 사용량 기반 과금 | 3개월+ | ⭐⭐⭐ |
| 8 | **워크플로 비주얼 설계기** — 드래그&드롭 → 코드 생성 | $19/월(개인), $99/월(팀) | 3~4주 | ⭐⭐⭐⭐ |

### 우선순위 기준

- 가장 빠른 현금화 → **5번 (컨설팅)**
- 가장 낮은 리스크 → **3번 (콘텐츠)**
- 가장 큰 업사이드 → **1번 + 2번 (SaaS)**
- 가장 빠른 MVP → **2번 (MCP 브릿지, 2주)**

### 실행 로드맵 제안

```
1주차    이 분석 내용을 한국어 기술 블로그로 발행            → 3번 착수
2~3주차  Temporal-MCP 브릿지 오픈소스 공개 (스타/브랜딩)     → 2번
4~8주차  그 위에 Durable AI Agent SaaS MVP                  → 1번
동시 진행 인바운드 문의 → 컨설팅 수주 (즉시 매출)           → 5번
6개월+   WASM 멀티테넌트 플랫폼으로 확장                    → 7번
```

핵심 시너지: 3번(콘텐츠)이 다른 모든 아이디어의 마케팅 채널 역할을 하고, 2번(OSS)이 신뢰도를 만들어 1번(SaaS)과 5번(컨설팅)의 유입을 만든다.

---

## 7. 참고 링크

- 이 포크: https://github.com/bmshin94/sdk-rust
- 원본 Rust SDK: https://github.com/temporalio/sdk-rust
- Temporal 공식: https://temporal.io · 문서: https://docs.temporal.io
- 왜 Rust인가 (블로그): https://temporal.io/blog/why-rust-powers-core-sdk
- crates.io: https://crates.io/crates/temporalio-sdk · https://crates.io/crates/temporalio-client
- docs.rs: https://docs.rs/temporalio-sdk · https://docs.rs/temporalio-client
- 타 언어 SDK: [TypeScript](https://github.com/temporalio/sdk-typescript) · [Python](https://github.com/temporalio/sdk-python) · [.NET](https://github.com/temporalio/sdk-dotnet) · [Ruby](https://github.com/temporalio/sdk-ruby)
- 커뮤니티 PHP SDK: https://github.com/temporalio/sdk-php

---

*이 문서는 레포지토리 전수조사(README, ARCHITECTURE, AGENTS, Cargo 매니페스트, 10개 크레이트, 18개 예제, CI 워크플로)를 기반으로 작성되었습니다.* ✨
