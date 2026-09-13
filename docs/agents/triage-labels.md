# Triage Labels

The skills speak in terms of five canonical triage roles. This file maps those roles to the actual label strings used in this repo's issue tracker.

| Label in mattpocock/skills | Label in our tracker | Meaning                                  |
| -------------------------- | -------------------- | ---------------------------------------- |
| `needs-triage`             | `needs-triage`       | Maintainer needs to evaluate this issue  |
| `needs-info`               | `needs-info`         | Waiting on reporter for more information |
| `ready-for-agent`          | `ready-for-agent`    | Fully specified, ready for an AFK agent  |
| `ready-for-human`          | `ready-for-human`    | Requires human implementation            |
| `wontfix`                  | `wontfix`            | Will not be actioned                     |

When a skill mentions a role (e.g. "apply the AFK-ready triage label"), use the corresponding label string from this table.

Edit the right-hand column to match whatever vocabulary you actually use.

## Creating the labels

`wontfix` ships as a GitHub stock label. The other four need creating once:

```sh
gh label create needs-triage    --description "Maintainer needs to evaluate this issue"  --color D93F0B
gh label create needs-info      --description "Waiting on reporter for more information" --color FBCA04
gh label create ready-for-agent --description "Fully specified, ready for an AFK agent"  --color 0E8A16
gh label create ready-for-human --description "Requires human implementation"            --color 1D76DB
```

If `gh label create` reports a label already exists, leave it as it is — do not `--force` over the existing colour or description.

## Priority labels

Orthogonal to the triage roles above: every issue also carries exactly one priority
label, independent of its category (`bug`/`enhancement`/`question`) and state
(`needs-triage`/`ready-for-agent`/etc.) roles.

| Label                | Meaning                                                           |
| -------------------- | ----------------------------------------------------------------- |
| `priority-very-high` | Address immediately — active breakage, security, or data loss     |
| `priority-high`      | A real bug with a plausible trigger, or comparable urgency        |
| `priority-medium`    | Real risk or proven-recurring pattern, not currently causing harm |
| `priority-low`       | Narrow-impact or already mitigated by an existing floor/handler   |
| `priority-very-low`  | Cosmetic, purely organizational, or "nothing is wrong today"      |

**Every issue must carry a priority label from the moment it is filed.** Assign one
when triaging (alongside the category and state roles), not as an afterthought.
Priorities are not fixed — re-triage and change them as circumstances change or the
codebase evolves.

```sh
gh label create priority-very-high --description "Address immediately"           --color 8B0000
gh label create priority-high      --description "Should be addressed soon"      --color E67E22
gh label create priority-medium    --description "Normal priority"               --color F1C40F
gh label create priority-low       --description "Address when convenient"       --color 82C91E
gh label create priority-very-low  --description "Minimal priority, nice-to-have" --color 2F9E44
```
