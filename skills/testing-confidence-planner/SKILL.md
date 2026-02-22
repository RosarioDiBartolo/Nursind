---
name: testing-confidence-planner
description: Build and refine practical software testing strategies that maximize confidence per unit cost. Use when defining what to test, selecting test layers (unit/integration/E2E), designing stable tests, reducing flakiness, prioritizing by risk, planning CI test stages, or translating broad testing advice into a concrete stack-specific plan.
---

# Testing Confidence Planner

## Core Workflow

1. Clarify system scope.
Collect stack, architecture style, critical user journeys, external dependencies, and release cadence.

2. Prioritize by risk.
Rank areas by impact x likelihood. Focus first on money flows, authorization, data integrity, and core business rules.

3. Choose the cheapest effective test layer.
Prefer the lowest layer that can reliably catch the failure mode:
- Unit for logic and invariants
- Integration/component for boundary contracts and persistence/serialization behavior
- E2E for a small set of critical journeys

4. Design tests around behavior.
Write tests in Arrange/Act/Assert form. Assert outcomes and invariants, not internals.

5. Enforce determinism.
Remove nondeterminism (time, random, async races, shared state, real network drift) and quarantine flaky tests quickly.

6. Produce an execution plan.
Output a practical plan with test inventory, tooling choices, folder structure, CI stages, and next 10-20 tests.

## Output Contract

When asked for a plan, return these sections in order:
1. System profile
2. Risk map
3. Layer strategy
4. Top test targets (ordered)
5. Determinism and anti-flake rules
6. CI strategy
7. Immediate next tests to add

## Guardrails

- Optimize for confidence, speed, and maintainability over raw coverage percentages.
- Treat coverage as a discovery map, not a goal.
- Keep E2E suites intentionally small and business-critical.
- Prefer real boundaries over mocks for owned interfaces when feasible.
- Avoid brittle assertions (internal calls, unstable strings) unless user-facing behavior requires them.

## References

- Read `references/testing-playbook.md` for detailed heuristics, checklists, and anti-patterns.
