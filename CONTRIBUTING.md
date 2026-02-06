# Contributing

## Branch Targets

- Most changes should target `dev`.
- `nightly` is for staged integration from `dev`.
- `main` is for stable releases only.

See `docs/BRANCHING.md`.

## Local Checks

Rust:

```bash
cargo fmt
cargo check --features all
cargo test --features all
```

Python:

```bash
cd horizons_py
python -m pip install -e ".[test]"
pytest
```

TypeScript:

```bash
cd horizons_ts
npm install
npm run build
npm test
```

