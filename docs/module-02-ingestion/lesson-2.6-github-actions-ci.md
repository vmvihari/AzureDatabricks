# Lesson 2.6: Continuous Integration — The 3-Gate Quality Model

## Table of Contents
- [The Problem: Code That Works Locally but Breaks in the Cloud](#the-problem)
- [The 3-Gate Quality Model](#the-3-gate-quality-model)
- [Gate 1 — Local: pre-commit Hooks](#gate-1--local-pre-commit-hooks)
- [Gate 2 — GitHub Actions: Lint on Every Push](#gate-2--github-actions-lint-on-every-push)
- [Gate 3 — GitHub Actions: pytest on Every PR to main](#gate-3--github-actions-pytest-on-every-pr-to-main)
- [Interview Preparation](#interview-preparation)

[⬅️ Back to Main Course Directory](../../README.md)

---

## The Problem

You now have:
- Ingestion scripts that run on a real Azure cluster (Lesson 2.2)
- Unit tests that validate your transformation logic locally (Lessons 2.2–2.5)

But nothing stops a developer from committing badly formatted code, breaking another developer's test, or pushing a syntax error that crashes the pipeline. In a team environment, **every commit is a potential liability** unless there is an automated quality gate.

The industry answer is **Continuous Integration (CI)** — an automated system that validates every code change before it can be merged.

---

## The 3-Gate Quality Model

Real enterprise data engineering teams use a **layered quality model** where each gate has a specific cost and purpose:

```
┌───────────────────────────────────────────────────────────────┐
│  GATE 1: pre-commit hooks (local, before git commit)          │
│  → Runs: black, flake8, isort                                 │
│  → Cost: ~1 second │ Catches: formatting & style violations  │
├───────────────────────────────────────────────────────────────┤
│  GATE 2: GitHub Actions — Lint (on every branch push)         │
│  → Runs: black, flake8, isort                                 │
│  → Cost: ~30 seconds │ Catches: anything Gate 1 missed       │
│  → Note: No PySpark — keeps CI fast and cheap                │
├───────────────────────────────────────────────────────────────┤
│  GATE 3: GitHub Actions — pytest (on PR to main only)         │
│  → Runs: full pytest suite with coverage reporting            │
│  → Cost: 3–5 minutes │ Blocks merge if tests fail            │
│  → Note: Uses pip caching so PySpark only downloads once     │
└───────────────────────────────────────────────────────────────┘
```

> [!IMPORTANT]
> **Gate 3 is the true merge gate.** Running the full `pytest` suite on every push to every branch would be prohibitively slow (3–5 minutes) and expensive (GitHub Actions charges per minute). The professional pattern is: lint on push, test on PR. This is the pattern used at Airbnb, Netflix, and FAANG data engineering teams.

---

## 🛠️ Gate 1 — Local: `pre-commit` Hooks

`pre-commit` is a framework that runs checks automatically every time you run `git commit`. If any check fails, the commit is aborted and you must fix the issue first.

### Step 1: Install the tools

```bash
pip install pre-commit black flake8 isort
```

### Step 2: Create the configuration

Navigate to `apps/mortgage-data-platform/` and create `.pre-commit-config.yaml`:

```yaml
repos:
  # Auto-format Python code on every git commit
  - repo: https://github.com/psf/black
    rev: 23.12.1
    hooks:
      - id: black
        language_version: python3

  # Lint for style violations
  - repo: https://github.com/PyCQA/flake8
    rev: 7.0.0
    hooks:
      - id: flake8
        args: [--max-line-length=120, --exclude=.venv]

  # Sort imports consistently
  - repo: https://github.com/PyCQA/isort
    rev: 5.13.2
    hooks:
      - id: isort
        args: [--profile=black]
```

### Step 3: Install the hooks into your local git repository

```bash
pre-commit install
```

This registers the hooks with your local `.git/hooks/` directory. From now on, `git commit` automatically runs `black`, `flake8`, and `isort` before your commit is accepted.

**Test it:**
```bash
# Try committing a poorly formatted file
git add src/
git commit -m "test commit"
# black will reformat the file and abort the commit — fix and recommit
```

> [!TIP]
> `black` auto-formats the file for you. Run `git add` again after `black` reformats, then `git commit` again and it will pass.

---

## 🛠️ Gate 2 — GitHub Actions: Lint on Every Push

Even if a developer skips `pre-commit install`, Gate 2 in the cloud catches the same issues.

### Step 1: Create the dev dependencies file

Navigate to `apps/mortgage-data-platform/` and create `requirements-dev.txt`:

```
pyspark==3.5.1
pytest==7.4.3
pytest-cov==4.1.0
responses==0.25.0
```

> [!NOTE]
> Notice `delta-spark` is **not** in this file. Our unit tests use `spark.sql()` and never call `.read.format("delta")`, so there is no reason to install the heavy Delta Lake JARs in CI. Always include only what you need.

### Step 2: Create the GitHub Actions workflow

Navigate to `apps/mortgage-data-platform/` and create `.github/workflows/ci.yml`.

The full file is already created in your workspace at [ci.yml](file://e:\Git\AzureDatabricks\apps\mortgage-data-platform\.github\workflows\ci.yml).

The lint job (Gate 2) is the first job in that file. It runs on every push to any branch and completes in ~30 seconds — giving developers immediate feedback without the cost of spinning up PySpark.

---

## 🛠️ Gate 3 — GitHub Actions: `pytest` on Every PR to `main`

The `test` job in `ci.yml` only triggers when a Pull Request is opened targeting `main`. This is the true quality gate — it must pass before a merge is allowed.

Key features of the Gate 3 job:

**1. pip caching:** PySpark is ~300MB. Without caching, every PR run would spend 3–4 minutes just downloading it. With `actions/cache`, the download happens once; subsequent runs use the cached version in ~10 seconds.

```yaml
- name: Cache pip dependencies
  uses: actions/cache@v4
  with:
    path: ~/.cache/pip
    key: ${{ runner.os }}-pip-${{ hashFiles('requirements-dev.txt') }}
```

**2. Coverage threshold:** The `--cov-fail-under=70` flag fails the CI job if code coverage drops below 70%. This prevents developers from shipping new, untested code.

```yaml
run: pytest tests/unit/ --cov=src --cov-fail-under=70
```

**3. Artifact upload:** Test results are saved as a GitHub Actions artifact so they can be inspected even if the job fails.

### Step 3: Enforce the gate in GitHub

To make Gate 3 actually block merges:
1. Go to your GitHub repository → **Settings → Branches**
2. Add a branch protection rule for `main`
3. Check **"Require status checks to pass before merging"**
4. Select **"Gate 3 — Unit Tests (pytest)"** as a required check

From this point forward, no code can merge to `main` without passing all unit tests.

---

## Workflow Summary

```
Developer writes code
       ↓
git commit → pre-commit hooks (black, flake8, isort) [Gate 1]
       ↓
git push → GitHub Actions: Lint job [Gate 2] (~30 sec)
       ↓
git PR → main → GitHub Actions: pytest + coverage [Gate 3] (3–5 min)
       ↓
PR approved + all gates green → Merge to main
       ↓
[Module 7] → Automated CD: databricks bundle deploy -t prod
```

---

## 🎯 Interview Preparation

> [!TIP]
> **Q1: Why do you run linting in CI separately from your unit tests?**
> **Answer:** "Linting checks (formatting, style) are extremely fast — under a minute. Unit tests with PySpark can take 3–5 minutes due to JVM startup costs and test volume. Running the full test suite on every branch push would be prohibitively slow and expensive, creating developer friction. The professional pattern is a layered approach: lint on every push for instant feedback, and the full pytest suite only on PRs to the protected main branch. This keeps CI fast for 90% of commits and thorough for the 10% that actually matter — merges."

> [!TIP]
> **Q2: What is a `pre-commit` hook and why use it when you already have CI?**
> **Answer:** "A `pre-commit` hook is a local Git hook that runs checks before a commit is even created. It's the cheapest gate — it runs in milliseconds and costs nothing. The reason to use it alongside CI is the 'shift left' principle: catch defects as early as possible. If a developer's `black` check fails locally, they fix it immediately without wasting CI compute time or other developers' attention. CI is the safety net; pre-commit hooks are the first line of defense."

---
[⬅️ Previous: Lesson 2.5: PySpark Transformations](lesson-2.5-pyspark-transformations.md) | [🏠 Main Directory](../../README.md) | [➡️ Next: Project Task 2](project-task-02.md)
