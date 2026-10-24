The original, unhealed demo pipeline. `bench.workdir.make_workdir` overlays this onto
every throwaway working copy, so tests and the MTTR benchmark always start from the
same broken-on-drift baseline no matter how many self-heal PRs have been merged into
`pipeline/` or what the running app has written to `data/schema_snapshots/`.

Do not let the agent edit this directory, and don't update it when merging heals.
