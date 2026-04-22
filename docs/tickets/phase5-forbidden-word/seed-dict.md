# Phase 5 Forbidden Term Seed Dictionary

Phase 5 starts with a schema-first dictionary. Operators can fill in real forbidden terms later.

## Columns

| Column | Required | Description |
| --- | --- | --- |
| `term` | yes | Actual forbidden term or a temporary placeholder until the operator confirms it. |
| `severity` | yes | Risk level: `HIGH`, `MED`, `LOW`. |
| `scope` | yes | Market scope: `all`, `naver`, `godomall`, `smartstore`, `11st`, `esm`. |
| `reason` | yes | Why the term should be blocked. |
| `source` | yes | Where the rule came from, such as operator memo or marketplace policy. |
| `added_at` | yes | Date the row was added in `YYYY-MM-DD` format. |

## Seed Rows

| term | severity | scope | reason | source | added_at |
| --- | --- | --- | --- | --- | --- |
| `<placeholder-term-1>` | HIGH | all | Replace with a confirmed operator-managed forbidden term. | operator_manual_seed | 2026-04-22 |
| `<placeholder-term-2>` | MED | smartstore | Use for a market-specific blocked phrase once confirmed. | operator_manual_seed | 2026-04-22 |

## Notes

- Keep one row per forbidden term.
- Use `all` only when the same term must be blocked across every market.
- If no real term is ready yet, keep the placeholder rows and replace them later.
