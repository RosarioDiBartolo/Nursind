# Testing Playbook

## 1. Mental Model

Treat testing as an investment in confidence under constraints:
- Signal: catch real regressions
- Speed: give fast feedback
- Stability: avoid flakes
- Cost: keep maintenance reasonable

Use a layered portfolio: many fast tests near logic, fewer broad tests across system boundaries.

## 2. Risk-First Prioritization

Prioritize these first:
- Auth, permissions, money movement, billing
- Data integrity (writes, migrations, idempotency, concurrency)
- Core user journeys (login, checkout, create-edit-delete of primary entity)
- Business-critical rules (eligibility, pricing, state transitions, limits, expirations)

Then target bug-dense zones:
- Boundaries: null/empty, zero/negative, maxima, off-by-one, timezone and date rollover
- Contracts: payload schema drift, versioning, serialization edges
- Reliability paths: retries, timeouts, duplicate events, at-least-once delivery
- External dependencies: payment, storage, email, queues

Usually low-value targets:
- Framework internals you do not own
- Trivial pass-through accessors
- Fast-changing cosmetic details without product risk

## 3. Layer Selection Heuristic

Pick the cheapest layer that catches the defect reliably:
- Unit: pure logic, transformations, state machine transitions, invariant checks
- Integration/component: DB + serialization + HTTP boundaries + module wiring
- E2E: 3-10 critical smoke journeys in prod-like environments

If a bug is caused by interface mismatch, prefer integration over adding E2E.

## 4. Test Design Rules

- Write behavior-focused tests, not implementation choreography.
- Use Arrange/Act/Assert and keep one main concept per test.
- Mock only unstable/slow/external boundaries by default.
- Keep owned boundaries as real as practical.
- Assert stable outcomes:
  - status codes/types/categories
  - key fields and invariants
  - contract/schema conformance
- Avoid brittle exact-string assertions unless user-facing copy is the requirement.

## 5. Flake Prevention Checklist

- Inject or freeze time
- Seed or eliminate randomness
- Await true conditions, not sleeps
- Isolate shared state (test DB reset, unique IDs, hermetic setup)
- Avoid ambient network dependency unless explicitly testing network behavior
- Quarantine flaky tests immediately; do not normalize intermittent failures

## 6. What to Add Next

When backlog is unclear, add tests in this order:
1. Domain invariants and state machine rules
2. Boundary cases around those rules
3. Integration tests for high-risk endpoints with real persistence/serialization
4. API contract tests between services/clients
5. Minimal E2E smoke journeys

## 7. CI Portfolio Pattern

- Every PR: unit + most integration
- Merge/nightly: broader E2E suite (kept intentionally small)
- Fast failure policy for flaky tests: fix, quarantine, or delete

## 8. Stack Intake Template

Use this prompt to gather context before planning:
- Backend language/framework
- Frontend stack (if any)
- Database and queue/cache providers
- External integrations
- Deployment model (monolith/services/serverless)
- Highest-risk user journeys
- Current test stack and known pain points
- Target PR feedback budget (minutes)
