# Forbidden Word Operator Explanation Template

Copy the template below when an operator needs to explain or register a forbidden term.

## Copy Template

```md
### Forbidden Term Request
- term:
- severity: HIGH | MED | LOW
- scope: all | naver | godomall | smartstore | 11st | esm
- reason:
- source:
- added_at: YYYY-MM-DD
- notes:
```

## Operator Guide

1. Fill `term` with the exact word or phrase that should be blocked.
2. Choose `severity` based on operational risk or policy sensitivity.
3. Choose `scope` for the affected market, or `all` when every market should block it.
4. Record a short `reason` that explains why the rule exists.
5. Record `source` so later reviewers can trace the decision.

## Example

```md
### Forbidden Term Request
- term: <placeholder-term-1>
- severity: HIGH
- scope: all
- reason: Marketplace policy or internal review flagged the term.
- source: operator_manual_seed
- added_at: 2026-04-22
- notes: Replace placeholder with a confirmed term when available.
```
