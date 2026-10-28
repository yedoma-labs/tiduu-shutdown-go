# Security Policy

## Supported Versions

Only the latest released version receives security fixes.

## Reporting a Vulnerability

Please do not open a public issue for security problems. Report them privately through
GitHub's Private Vulnerability Reporting:

1. Open the repository on GitHub.
2. Go to the **Security** tab.
3. Click **Report a vulnerability** and describe the issue.

Include the affected version, steps to reproduce, and the expected and actual behavior. You will
get an acknowledgement, and the report stays private until a fix is released.

## Scope

In scope:

- Teardown that does not run registered steps, or runs them in a different order than documented
  (LIFO), in a way that can leave resources open or in use.
- Signal handling that exits the process while a teardown is still running and without a
  documented reason, or that fails to exit when it should.
- A `Run` call that blocks beyond its global deadline because of library code, not because of a
  step that ignores its context.
- Panics in library code that a caller cannot recover from.

Out of scope:

- Steps that ignore their context. They are reported as `timeout` and keep running in the
  background by design.
- Calling `Run` or `Register` from several goroutines in ways the documentation does not cover.
- Misconfiguration of the orchestrator's grace period.
