# Instructions for agents

A data processing framework built on Spark and Delta Lake. It reads files or Delta tables, applies transformation, conformation, data quality and dedup, and writes to Delta tables.

## Before any change

1. Read [docs/spec.md](docs/spec.md). It is the normative specification: implemented behavior must follow its rules.
2. Use [docs/architecture.md](docs/architecture.md) to understand the design (diagrams and examples). If they diverge, `spec.md` wins.

## Working rules

- **Cite rule IDs** (`CKP-06`, `SCD-03`, `CFG-14`...) in tests, in comments that explain a decision and in commit messages.
- **The acceptance scenarios (`AC-*`, section 9 of the spec) are tests.** Every rule implementation must come with tests for the scenarios that cover it.
- **Do not invent behavior.** If something is not in the spec, or is listed in "Not specified in v1", stop and raise the question instead of picking a rule.
- **Follow [CONTRIBUTING.md](CONTRIBUTING.md)** for branches, commits, pull requests and versioning.
- **Behavior changes update the spec.** A rule change updates `spec.md` in the same commit, and `architecture.md` when it affects diagrams or examples. Mermaid diagrams must keep rendering.

## Principles that cannot be broken

- **Simple on the common path** (`PRI-01`): common loads are configured with text parameters only. Complexity goes only into sub-parameters or extension points.
- **The target is the state** (`PRI-02`): no control table or log process of our own. The checkpoint is the maximum of a column on the target.
- **Environment-dependent code stays behind `FileSystemAdapter`** (`PRI-03`). The rest of the framework does not access the file system directly.
- **Metadata names and default values live in `enums.py`** (`ENUM-*`), in plain classes with attributes that can be overridden in code. No hard-coded column names scattered through the code.
- **Validity comes from the source or the extraction** (`PRI-04`), never from the time the framework processes the data.

## Vocabulary

- Use **checkpoint**, not "watermark".
- Validity (`modified_date`), checkpoint (`checkpoint_column`) and version order (`order_column`) are three different concepts (spec, section 1).

## Language and documentation

- Everything in the repository is in English: code, comments, documentation and commit messages.
- Documentation describes the framework in a platform-independent way: do not mention specific platforms or the history of discussions and decisions.
