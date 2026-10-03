# SmartRetail Business-Case Repositioning Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reorganize the SmartRetail Data Platform documentation so recruiters and technical reviewers understand the business problem, invariants, architectural decisions, evidence, and trade-offs before the technology stack.

**Architecture:** This is a documentation-first repositioning. No production behavior changes are introduced. Existing validated implementation facts are preserved, while the README and supporting docs are reorganized around business problem → rules → requirements → architecture decisions → evidence.

**Tech Stack:** Markdown, Mermaid/text architecture diagrams, GitHub repository documentation; implementation evidence already present in Java 21, Spring Boot, Kafka, PostgreSQL, Spark, Airflow, Iceberg, React, Docker and CI.

**Spec:** `docs/superpowers/specs/2026-10-02-business-case-repositioning-design.md`

## Global Constraints

- Do not add new production technologies in this slice.
- Do not claim unimplemented scale characteristics.
- Preserve existing technical documentation and validated implementation details.
- The primary invariant remains: the same `Idempotency-Key` must not create two distinct logical order events.
- Do not claim global exactly-once semantics; Kafka processing is compatible with at-least-once delivery plus idempotent boundaries.
- Continuous ingestion/Bronze/Silver remain streaming flows; Airflow orchestrates finite post-Silver batch work.
- Claims must be backed by repository evidence wherever possible.
- Existing release/version history must remain intact.

## Review Focus

1. Duplicate-delivery wording: documentation must distinguish API idempotency from consumer idempotency and must not imply exactly-once globally.
2. Data-quality wording: the current Silver gate validates Silver invariants; it must not be described as a Bronze rejection counter unless evidence exists.
3. Airflow wording: Airflow must be presented as the orchestrator of finite post-Silver jobs, not as the streaming engine.
4. Scale wording: phrases such as “thousands/millions of events”, throughput, latency, availability or SLO numbers must not be asserted without load-test evidence.
5. Current-state wording: implemented v0.1–v0.5 capabilities must be separated from roadmap/future features such as ML, security expansion or future infrastructure.

---

### Task 1: Create the business-problem and business-rules documentation

**Files:**
- Create: `docs/BUSINESS_PROBLEM.md`
- Create: `docs/BUSINESS_RULES.md`

**Interfaces:**
- Consumes: approved design in `docs/superpowers/specs/2026-10-02-business-case-repositioning-design.md` and current implementation claims in `README.md`.
- Produces: canonical business context and rule identifiers (`BR-001` through `BR-006`) referenced by later documentation.

- [ ] **Step 1: Create `docs/BUSINESS_PROBLEM.md`**

Document the actors (web/mobile/PDV/integrations), duplicate-delivery failure mode, impact on projections/downstream processing/analytics, and the intended reliable-ingestion outcome. Explicitly state that this repository is a portfolio simulation and avoid invented production-volume numbers.

- [ ] **Step 2: Verify the business-problem document**

Check that the document answers: who has the problem, what can fail, why it matters, and what outcome the platform provides.

Expected: all four questions are answered before any technology list.

- [ ] **Step 3: Create `docs/BUSINESS_RULES.md`**

Define the six rules from the spec exactly by identifier and intent:

- `BR-001` Idempotent ingestion
- `BR-002` Reliable event publication
- `BR-003` Idempotent downstream consumption
- `BR-004` Curated analytical data
- `BR-005` Reproducible analytical products
- `BR-006` Operational traceability

For `BR-001`, include the observable repeated-key behavior without inventing a response shape not supported by the existing API.

- [ ] **Step 4: Verify terminology against current implementation**

Compare `BUSINESS_RULES.md` with the existing README sections for Idempotency, Transactional Outbox, Silver Data Quality, Analytics Export and E2E validation.

Expected: no rule contradicts the current implementation and no rule claims exactly-once globally.

- [ ] **Step 5: Commit**

```bash
git add docs/BUSINESS_PROBLEM.md docs/BUSINESS_RULES.md
git commit -m "docs: define SmartRetail business problem and rules"
```

---

### Task 2: Create functional and non-functional requirements

**Files:**
- Create: `docs/REQUIREMENTS.md`

**Interfaces:**
- Consumes: `docs/BUSINESS_PROBLEM.md`, `docs/BUSINESS_RULES.md`.
- Produces: stable requirement identifiers `FR-001`–`FR-010` and `NFR-001`–`NFR-006` for README, architecture and ADR references.

- [ ] **Step 1: Create `docs/REQUIREMENTS.md`**

Include the functional and non-functional requirements from the approved spec. Add a traceability column mapping each requirement to one or more business-rule IDs.

- [ ] **Step 2: Mark implemented vs architectural/future scope where necessary**

Requirements tied to already-implemented v0.1–v0.5 behavior should be marked `Implemented`. Anything not evidenced in the current repository must be marked `Planned` or omitted from this repositioning slice.

- [ ] **Step 3: Verify no unsupported service-level targets exist**

Search the new document for numeric throughput, latency, availability and SLO targets.

Expected: none unless directly supported by existing repository evidence.

- [ ] **Step 4: Commit**

```bash
git add docs/REQUIREMENTS.md
git commit -m "docs: add traceable SmartRetail requirements"
```

---

### Task 3: Add Architecture Decision Records for the implemented design

**Files:**
- Create: `docs/adr/ADR-001-kafka-event-backbone.md`
- Create: `docs/adr/ADR-002-transactional-outbox.md`
- Create: `docs/adr/ADR-003-medallion-and-airflow-boundary.md`
- Create: `docs/adr/ADR-004-analytics-serving-model.md`

**Interfaces:**
- Consumes: current `README.md`, `docs/ARCHITECTURE.md`, business rules and requirements.
- Produces: decision records linked by the README and architecture document.

- [ ] **Step 1: Write ADR-001 for Kafka**

Record context, decision, alternatives, consequences and trade-offs. Tie the decision to asynchronous decoupling, multiple independent consumers, replayability and scalable consumption. Avoid claiming Kafka guarantees business-level exactly-once processing.

- [ ] **Step 2: Write ADR-002 for Transactional Outbox**

Explain the database/Kafka dual-write failure mode, why the Outbox is used, at-least-once implications, idempotent consumer requirement and operational cost of an additional publisher/retry path.

- [ ] **Step 3: Write ADR-003 for Medallion + Airflow boundary**

Document why Bronze preserves raw/replayable data, Silver enforces trusted invariants, Gold provides consumption-oriented products, and why Airflow starts at finite post-Silver batch jobs rather than replacing Structured Streaming.

- [ ] **Step 4: Write ADR-004 for the analytics serving model**

Explain why the React dashboard consumes a Spring Boot Analytics API backed by PostgreSQL `analytics` instead of querying lake files or transactional ingestion tables directly.

- [ ] **Step 5: Cross-check every ADR against implementation status**

Expected: each decision describes an implemented v0.1–v0.5 behavior, not a roadmap-only capability.

- [ ] **Step 6: Commit**

```bash
git add docs/adr
git commit -m "docs: record SmartRetail architecture decisions"
```

---

### Task 4: Align the architecture document with the current implemented state

**Files:**
- Modify: `docs/ARCHITECTURE.md`

**Interfaces:**
- Consumes: ADRs, requirements, current v0.5 README evidence.
- Produces: technical architecture document that clearly separates `Current implementation` from `Target / roadmap`.

- [ ] **Step 1: Replace ambiguous target-only framing**

The current opening says the document is an “arquitetura alvo”. Update it so implemented v0.1–v0.5 capabilities are explicitly current, while ML/security/future infrastructure remain labeled roadmap.

- [ ] **Step 2: Update the ingestion flow**

Ensure the current path explicitly shows:

```text
Ingestion API -> PostgreSQL transaction -> Transactional Outbox -> Kafka
```

and references `BR-001`, `BR-002`, ADR-001 and ADR-002.

- [ ] **Step 3: Update the streaming/lakehouse/orchestration boundary**

Make explicit that Kafka/Bronze/Silver are continuous Spark Structured Streaming flows, while Airflow orchestrates Silver Data Quality → Gold → Iceberg refresh → validation → analytics publication.

- [ ] **Step 4: Correct future-state labels**

Keep MLlib, MLflow, advanced security/governance and other unimplemented items only under a clearly marked roadmap/future section.

- [ ] **Step 5: Verify Review Focus cases**

Expected: no global exactly-once claim; no Airflow-as-streaming-engine wording; no unverified SLO/scale numbers.

- [ ] **Step 6: Commit**

```bash
git add docs/ARCHITECTURE.md
git commit -m "docs: align architecture narrative with implemented platform"
```

---

### Task 5: Create a recruiter/interview case-study document

**Files:**
- Create: `docs/PORTFOLIO_CASE_STUDY.md`

**Interfaces:**
- Consumes: business problem, rules, requirements, ADRs and current E2E evidence.
- Produces: concise case narrative that can be reused in interviews, portfolio links and LinkedIn without requiring a reviewer to read source code first.

- [ ] **Step 1: Write a 60-second project explanation**

Use this sequence: problem → primary invariant → solution flow → why Kafka/Outbox/Airflow → evidence/result.

- [ ] **Step 2: Add a 3–5 minute technical explanation**

Cover the end-to-end data flow, duplicate-delivery handling, data-quality gate, serving model and main trade-offs.

- [ ] **Step 3: Add interviewer Q&A prompts**

Include at least: “Why Kafka?”, “Why Outbox?”, “Why not exactly-once?”, “Why Airflow after Silver?”, “How is duplication handled?”, “What would you change for real production scale?”. Answers must distinguish current evidence from future improvements.

- [ ] **Step 4: Add evidence references**

Link to existing README sections, API examples, architecture doc, CI workflow badges and the newly created ADRs instead of inventing performance numbers.

- [ ] **Step 5: Commit**

```bash
git add docs/PORTFOLIO_CASE_STUDY.md
git commit -m "docs: add SmartRetail portfolio case study"
```

---

### Task 6: Rebuild the README opening around the business case

**Files:**
- Modify: `README.md`

**Interfaces:**
- Consumes: all documentation created in Tasks 1–5.
- Produces: the primary recruiter-facing repository entry point.

- [ ] **Step 1: Preserve the current validated implementation content**

Before editing, retain the current release history, architecture diagram, v0.1–v0.5 details, E2E outputs, endpoints, local execution instructions and badges.

- [ ] **Step 2: Replace the technology-first opening**

The first substantive sections after the title should become:

1. `Business Problem`
2. `Business Impact / Failure Modes`
3. `Core Business Rules`
4. `Solution Overview`
5. `Architecture at a Glance`
6. `Engineering Decisions & Trade-offs`
7. `Evidence` / validated E2E behavior
8. detailed releases and technology stack

- [ ] **Step 3: Add business-first summary copy**

The first paragraph must explain reliable retail-event ingestion and duplicate-delivery protection before naming the stack.

- [ ] **Step 4: Link supporting documentation**

Add links to:

- `docs/BUSINESS_PROBLEM.md`
- `docs/BUSINESS_RULES.md`
- `docs/REQUIREMENTS.md`
- `docs/ARCHITECTURE.md`
- `docs/PORTFOLIO_CASE_STUDY.md`
- ADR-001 through ADR-004

- [ ] **Step 5: Reframe technology descriptions as decisions**

For Kafka, Outbox, Spark/Medallion, Airflow and the analytics serving model, state the problem each component solves and include its trade-off rather than presenting it only as stack inventory.

- [ ] **Step 6: Preserve current-state evidence**

Keep the currently validated v0.5 outputs such as Data Quality Gate, post-load validation, Analytics Export and Airflow success, but label sample data as validation evidence rather than production performance metrics.

- [ ] **Step 7: Run documentation review checks**

Search README for claims matching `exactly-once`, `milhões`, `thousands`, `SLA`, `99.`, `high throughput` and similar scale/performance wording.

Expected: no unsupported quantitative or exactly-once claims.

- [ ] **Step 8: Commit**

```bash
git add README.md
git commit -m "docs: reposition SmartRetail as business-driven engineering case"
```

---

### Task 7: Final documentation integrity review

**Files:**
- Verify: `README.md`
- Verify: `docs/BUSINESS_PROBLEM.md`
- Verify: `docs/BUSINESS_RULES.md`
- Verify: `docs/REQUIREMENTS.md`
- Verify: `docs/ARCHITECTURE.md`
- Verify: `docs/PORTFOLIO_CASE_STUDY.md`
- Verify: `docs/adr/*.md`

**Interfaces:**
- Consumes: Tasks 1–6.
- Produces: review-ready branch with consistent claims and navigable documentation.

- [ ] **Step 1: Verify traceability**

For every `BR-*`, confirm at least one requirement or architecture/ADR reference exists. For every major README architectural claim, confirm a supporting doc or existing implementation evidence exists.

- [ ] **Step 2: Verify internal links**

Open every newly added relative Markdown link from the README and supporting docs.

Expected: all paths resolve on the branch.

- [ ] **Step 3: Verify current vs future scope**

Check ML, advanced security, cloud deployment and unimplemented scale features.

Expected: each is either clearly labeled roadmap/future or absent from current-state claims.

- [ ] **Step 4: Verify the five Review Focus risks**

Expected: all five are explicitly safe after review.

- [ ] **Step 5: Compare branch against `main`**

```bash
git diff --stat main...HEAD
git diff main...HEAD -- README.md docs/
```

Expected: documentation-only changes for this slice; no source, infrastructure or dependency changes.

- [ ] **Step 6: Final commit if review corrections were required**

```bash
git add README.md docs/
git commit -m "docs: finalize SmartRetail business case documentation"
```

- [ ] **Step 7: Prepare pull request summary**

Summarize: business-first repositioning, new rules/requirements/ADRs, architecture-current-state correction, recruiter case study, and explicit non-goal of changing production behavior.
