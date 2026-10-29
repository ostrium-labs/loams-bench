# Evidence archive moved

Benchmark results and trace exports are now owned by [ostrium-labs/loams-traces](https://github.com/ostrium-labs/loams-traces). Its `public/data/manifest.json` maps every archived file to source repositories, original paths, source revisions and SHA-256 checksums. Large exporter files have ordered byte-preserving parts.

The benchmark drivers continue to write local results when run. Those files are ignored here; publication goes through the trace archive. No historical Git commits have been rewritten.

All 93 result files removed by this migration were checked against the archive's source hashes before removal. The browser explorer shows failures and missing coverage as well as successful runs.
