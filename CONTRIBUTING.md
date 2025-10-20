# Contributing to Projects

This guide explains how to get set up, propose changes, and meet the quality, security, and licensing standards.

---

## 1) Development setup

### Python

1. Use **Python ≥ 3.11**.
2. Create an environment and install deps:

   ```bash
   python -m venv .venv && source .venv/bin/activate
   pip install -U pip
   pip install -r requirements.txt
   pip install -r requirements-dev.txt
   ```
3. (Optional) With Conda:

   ```bash
   conda env create -f environment.yml
   conda activate your-env
   ```

---

## 2) Workflow & branching

* Default branch: **`main`**.
* Create feature branches: `feature/<short-desc>`, `fix/<short-desc>`, `docs/<short-desc>`.
* Keep PRs focused and small; link the Issue they address.

### Commits

* Follow **Conventional Commits** where possible:

  * `feat:`, `fix:`, `docs:`, `test:`, `refactor:`, `chore:`, `build:`, `ci:`
* Sign every commit with the **DCO**:

  ```bash
  git commit -s -m "feat: add fault classifier"
  ```

  The sign-off line is automatically added and asserts the *Developer Certificate of Origin*.

---

## 3) Quality bar (what CI expects)

* **Formatting & linting:** `make fmt` and `make lint` (uses `black`, `ruff`, `flake8`).
* **Tests:** add/maintain **pytest** unit tests; run `make test`. Target **>=80%** coverage for changed code.
* **Type hints:** for new/changed modules, add Python type hints where practical.
* **No prints in libraries:** use `logging` with sensible levels.

A PR is ready when:

* CI is **green** (unit tests + linters).
* New code is **documented** (docstrings, README/docs update).
* You’ve added **at least one test** per new feature or bug fix.
* You’ve updated `THIRD_PARTY_NOTICES.md` if you added dependencies.

---

## 4) Style guides

### Python

* PEP-8; **snake\_case** for functions/variables, **CamelCase** for classes, **UPPER\_CASE** for constants.
* One module = one purpose; keep functions short and cohesive.
* Public APIs documented with docstrings (`"""Summary\n\nArgs:\nReturns:\nRaises:\n"""`).
* Prefer **pure Python** and vectorized NumPy/Pandas; isolate heavy I/O.

### Notebooks

* Keep notebooks **deterministic** (set seeds, fixed versions).
* Clear large outputs before committing; keep outputs small.
* Move long-running experiments to scripts in `scripts/` with CLI flags.

---

## 5) Documentation

* Keep **README** up to date: overview, quick start, dependencies, how to run, reproduction notes, and license.
* Add any detailed docs to `docs/` and link from the README.

---

## 6) Data, models, and large files

* **Do not commit** confidential, proprietary, or personal data.
* Use **Git LFS** or external storage for large artefacts; provide a small sample or a download script.
* Document the **source and license** of any dataset/model you use.
* Code, data, and models must each have clear **license files** (`LICENSE`, `LICENSE-DATA`, `LICENSE-MODEL`).

---

## 7) Security & privacy

* **Never commit secrets** (API keys, tokens, passwords).

  * Use `.env` locally (already in `.gitignore`) and **GitHub Actions Secrets** in CI.
* Remove or anonymize PII; follow institutional/contractual data handling rules.
* Avoid introducing dependencies with known high-severity CVEs; Dependabot is enabled.

---

## 8) Licensing & intellectual property

* **Inbound license (default):** By contributing, you agree your contribution is licensed under **PolyForm Noncommercial 1.0.0** (for code) unless the maintainers agree otherwise in writing **before** the PR.
* Add an **SPDX header** at the top of source files:

  ```
  SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0
  Copyright (c) <YEAR> SDAIA-KFUPM JRC-AI
  ```
* If you bring in third-party code or data:

  * Ensure its license **permits** our use and redistribution.
  * **Preserve** original LICENSE/NOTICE files and add entries to `THIRD_PARTY_NOTICES.md`.
  * Avoid GPL/AGPL libraries unless explicitly approved (they may conflict with our main license).
* **Commercial use:** our default code license is **Noncommercial**.

---

## 9) Versioning & releases

* We follow **SemVer** (MAJOR.MINOR.PATCH).
* Tag releases, generate release notes, and update `CITATION.cff`.
* If you mint a DOI (e.g., Zenodo), add the badge to the README.

---

## 10) Contribution checklist (for PRs)

* [ ] Issue linked and scope agreed (for non-trivial changes)
* [ ] Code formatted & linted (`make fmt && make lint`)
* [ ] Tests added/updated and passing (`make test`)
* [ ] Docs/README updated; examples reproducible
* [ ] No secrets, PII, or licensed content without permission
* [ ] `THIRD_PARTY_NOTICES.md` updated (if applicable)
* [ ] Commits signed with **`-s`** (DCO) and use conventional messages
* [ ] CI green and reviewers requested

---

## 11) Maintainers

* Ensure branch protections and required checks are enabled.
* Enforce license and security policies during review.
* Merge with **squash** or **rebase**; keep history tidy.
* Close stale issues after reasonable follow-up.

---
