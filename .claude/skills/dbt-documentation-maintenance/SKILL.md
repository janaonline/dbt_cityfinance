---
name: dbt-documentation-maintenance
description: Decide whether docs/folder_structure.md, docs/architecture.md, CLAUDE.md, or a Skill needs updating after a completed task in this project (dbt_cityfinance). Use at the end of any task, before finishing, as a quick check — not to update docs proactively for every change.
---

# Documentation maintenance (dbt_cityfinance)

This project keeps documentation in three places, each with a specific job. After finishing a task, check whether it changed something these files claim to be true — update only if so.

| Location | Covers | Update when... |
|---|---|---|
| `docs/folder_structure.md` | Repo layout, which modules have staging/schema.yml, macro/seed inventory | A folder/file was added/removed/moved, or a module's staging/schema.yml presence changed |
| `docs/architecture.md` | Data flow, schema routing table, macros, SQL patterns, testing architecture | Schema/tag/materialization config changed, a macro was added/changed, a new recurring SQL pattern was introduced, test coverage changed, a new module was added |
| `CLAUDE.md` (root) | Project-wide instructions Claude needs for *most* tasks | A setup/command change, a project-wide convention change, or the module list itself changed |
| `.claude/skills/*/SKILL.md` | Reusable workflow/domain knowledge for a specific kind of task | The workflow it documents actually changed (e.g. a new macro to use, a new test pattern, a new module-creation step) |

## The rule (mirrors the root CLAUDE.md's maintenance section)

- **Folder structure changed** (new/removed file or directory, a module gained/lost `staging/`/`schema.yml`) → update `docs/folder_structure.md`.
- **Architecture changed** (schema routing, materialization, tags, macros, SQL conventions, testing strategy, dependencies) → update `docs/architecture.md`.
- **Project-wide instruction changed** (setup commands, global conventions) → update `CLAUDE.md`.
- **Reusable workflow/domain knowledge changed** → update the relevant Skill.
- **New module or model added** → review `docs/folder_structure.md`, `docs/architecture.md`'s module inventory/routing tables, and relevant Skills (at minimum [dbt-module-development](../dbt-module-development/SKILL.md)).
- **SQL/business logic changed** → review [sql-business-logic](../sql-business-logic/SKILL.md), any relevant tests, and `docs/architecture.md`'s pattern section.

## What does NOT need a docs update

- A routine single-model addition/edit that follows an existing module's established pattern (e.g. another `fold`-style model in `grants_condition` using the same shape as its siblings) — this doesn't change any documented fact.
- Bug fixes that don't change the documented behavior/convention (e.g. fixing a specific model's join condition, without changing the general fuzzy-join *pattern* itself).
- Anything already covered accurately by an existing doc/skill — **do not touch a file just to re-confirm it's still correct**; only edit when a fact actually changed.

## How to update

- Keep `CLAUDE.md` concise — if a change needs more than a few lines of explanation, put the detail in `docs/architecture.md` or `docs/folder_structure.md` and leave a short pointer in `CLAUDE.md`.
- When a docs/architecture.md table (module inventory, schema routing) needs a new row, verify the actual `dbt_project.yml`/filesystem state before writing it — don't extrapolate from the pattern of existing rows.
- When editing a Skill, check the other Skills for anything that now contradicts it (e.g. if the schema-routing mechanism changed, [dbt-model-development](../dbt-model-development/SKILL.md), [dbt-module-development](../dbt-module-development/SKILL.md), and [dbt-debugging](../dbt-debugging/SKILL.md) all reference it and would need the same update).
