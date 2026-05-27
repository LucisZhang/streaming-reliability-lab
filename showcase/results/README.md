# Results JSON Contract

Every exported result JSON must include:

- `run_id`
- `git_sha`
- `started_at`
- `finished_at`
- `stack_versions`
- `command`
- `logs`

Additional phase-specific fields should be stable, typed, and documented in the phase that introduces them.

