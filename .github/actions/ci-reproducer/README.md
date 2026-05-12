# CI Reproducer Action

Automated GitHub Actions composite action for reproducing and diagnosing LXD CI failures.

## Overview

The CI Reproducer analyzes failed CI jobs, derives the minimal reproducer command from logs, executes it safely with resource constraints, and generates a developer-friendly diagnostic report.

**Key Features:**
- Automatic log parsing to identify failed tests and error context
- Minimal reproducer command derivation (no unnecessary setup)
- Safety validation (disk space, OOM, kernel panics, network issues)
- Timeout enforcement and resource limits
- Confidence scoring (Low/Medium/High) for reproducer reliability
- Markdown report output to job summary

## Usage

### In a Workflow

Add as the final step of a job that may fail:

```yaml
- name: CI Reproducer
  uses: ./.github/actions/ci-reproducer
  if: failure()
  with:
    github-token: ${{ secrets.GITHUB_TOKEN }}
    timeout-seconds: '900'
```

### Inputs

| Input | Required | Default | Description |
|-------|----------|---------|-------------|
| `github-token` | Yes | — | GitHub token for accessing logs via `gh` CLI |
| `timeout-seconds` | No | `900` | Maximum runtime for reproducer (seconds) |
| `failed-job-name` | No | (auto-detect) | Name of failed job; auto-detected if omitted |

### Outputs

| Output | Description |
|--------|-------------|
| `report` | Full Markdown-formatted reproducer report |
| `confidence` | Confidence level: `Low`, `Medium`, or `High` |
| `reproducer-command` | The minimal command to reproduce the failure |

## Report Format

The reproducer generates a Markdown report with the following sections:

### CI Reproducer Report

**Summary** — Key facts about the failure:
- Failed job name
- Failed step type (panic, assertion, timeout, etc.)
- Failed command (if identifiable)
- Key error message (truncated to 150 chars)
- Likely root cause (inferred)
- Confidence level

**Reproducer** — Minimal command to reproduce:
```bash
# from repository root
test/main.sh system-tests:cluster btrfs
```
Plus reproduction status: `confirmed`, `not confirmed`, or `not attempted`.

**Potential Fix** — Actionable guidance based on confidence and error type.

**Validation** — Commands that were actually executed and their status.

**Notes** — Context about environment, flakiness, and how to proceed.

## How It Works

### Step 1: Log Fetching
- Uses `gh run view` to download full job logs via GitHub API
- Extracts job metadata (name, matrix context if applicable)
- Validates log is non-empty and contains error patterns

### Step 2: Log Parsing (system-tests)
- Scans for `panic`, `AssertionError`, `FAIL`, `timeout` patterns
- Identifies `sub_test` phase boundaries
- Extracts test group (e.g., `cluster`, `standalone`) and backend (e.g., `zfs`, `btrfs`)
- Assigns confidence:
  - **High**: Panic or segfault in logs
  - **Medium**: Assertion failure or clear command failure
  - **Low**: Timeout, transient, or insufficient context

### Step 3: Validation
Checks logs for environmental issues that would prevent safe reproduction:
- Disk space (< 10% free)
- OOM (out of memory)
- Kernel panic
- Hardware/driver errors
- Network connectivity issues

Skips reproducer execution if validation fails.

### Step 4: Reproducer Execution
- Derives minimal command from parsed results (e.g., `test/main.sh system-tests:cluster btrfs`)
- Validates command syntax
- Executes with timeout enforcement (default 15 minutes)
- Captures exit code and output (last 20 lines)

### Step 5: Report Generation
- Formats Markdown report per spec
- Outputs to `$GITHUB_STEP_SUMMARY` (job summary tab)
- Sets GitHub Actions outputs for downstream use

## Interpreting Results

### High Confidence

The reproducer is very likely accurate. Root cause is clear from logs.

**Actions:**
- Run the reproducer command locally to confirm
- Review stack traces or assertion messages for fix
- Check code near the panic/error location

### Medium Confidence

The reproducer is probably accurate, but there may be environmental factors.

**Actions:**
- Run locally with same backend/group
- Check for timing-dependent or flaky behavior
- Look for race conditions if applicable

### Low Confidence

The reproducer is uncertain. Common reasons:
- Test timed out (deadlock vs. slow environment)
- Insufficient error context in logs
- Likely flaky/intermittent behavior

**Actions:**
- Run multiple times locally to detect flakiness
- Increase timeout if environment is slow
- Check test code for timing assumptions
- Consider running on faster hardware

## Examples

### Example 1: Panic Reproducer

**Report Output:**
```
## Summary
- **Failed job:** `system-tests (cluster, btrfs)`
- **Failed step:** `panic`
- **Key error:** `runtime error: invalid memory address or nil pointer dereference`
- **Likely cause:** Code panic (segmentation fault or runtime error in LXD daemon)
- **Confidence:** High

## Reproducer
```bash
# from repository root
test/main.sh system-tests:cluster btrfs
```
Reproduction: **confirmed** (command reproduced failure with exit code 2)
```

### Example 2: Flaky Test

**Report Output:**
```
## Summary
- **Failed job:** `system-tests (standalone, dir)`
- **Failed step:** `timeout`
- **Key error:** `Test timed out`
- **Likely cause:** Likely flaky/intermittent test (timing or race condition)
- **Confidence:** Low

## Reproducer
```bash
# from repository root
test/main.sh system-tests:standalone dir
```
Reproduction: **not confirmed** (reproducer timed out after 900 seconds)

## Notes
- **Confidence is Low:** The reproducer is uncertain. Test may be flaky or environment-dependent.
```

## Limitations

- **Phase 1 scope**: Only parses `system-tests` failures. Extension to snap-tests, code-tests planned.
- **Log access**: Requires GitHub token with read access to workflow logs (provided by Actions).
- **No retry logic**: Single execution. Flaky tests may not reproduce on first run.
- **Environment-dependent**: Local environment may differ from CI (hardware, network, packages).

## Architecture

```
.github/actions/ci-reproducer/
├── action.yml                 # Composite action definition
├── report.sh                  # Report generation
└── parsers/
    ├── log-fetcher.sh         # Fetch logs via gh CLI
    ├── system-tests.sh        # Parse system-tests failures
    ├── validator.sh           # Check safety
    └── run-reproducer.sh      # Execute reproducer
```

Each script is independent and can be tested in isolation.

## Development & Testing

### Manual Testing

To test the action locally (requires GitHub CLI and token):

```bash
# Set required environment
export GITHUB_TOKEN="your-token"
export GITHUB_REPOSITORY="canonical/lxd"
export GITHUB_RUN_ID="1234567890"

# Run log fetcher
.github/actions/ci-reproducer/parsers/log-fetcher.sh

# Run parser
.github/actions/ci-reproducer/parsers/system-tests.sh

# Generate report
.github/actions/ci-reproducer/report.sh
```

### Unit Tests

To add unit tests (Phase 2):

```bash
test/ci-reproducer-tests.sh
```

Tests include:
- Mock log files for various failure scenarios
- Parser accuracy on edge cases
- Report format compliance
- Safety validation logic

## Future Enhancements

- **Phase 2**: Extend to snap-tests and code-tests failures
- **Phase 3**: Add automatic retry logic for flaky test detection
- **Phase 4**: Intelligent escalation (try minimal → full group if needed)
- **Phase 5**: Integration with issue tracking (auto-file bugs for High confidence failures)

## Support

Questions or issues? Open an issue in the LXD repository with the label `ci-reproducer`.
