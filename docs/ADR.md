# Architecture decision record: accepted limitations

- No key existence validation: vulnerable to distinct-key attacks that grow storage without bound.
- No code-level tuning (e.g. query optimisations) focus is only on architectural design changes.
- approximate counts are fine
