# Branching And Release Process

This repo uses a 3-branch flow for OSS-quality stability:

- `dev`: day-to-day integration. Most PRs target `dev`.
- `nightly`: staging/integration branch. Receives batched merges from `dev`.
- `main`: stable releases only. Receives merges from `nightly` after burn-in.

## Pull Request Flow

1. Feature work:
   - Branch off `dev`.
   - Open PR back to `dev`.
2. Nightly promotion:
   - Open PR from `dev` to `nightly` (batch merges, resolve conflicts here).
   - Let `nightly` soak with CI and downstream users.
3. Stable release:
   - Open PR from `nightly` to `main`.
   - Merge only when CI is green and the branch is deemed stable.

## Versioning

- `main` uses stable versions and stable tags: `vX.Y.Z`.
- `dev` and `nightly` use a "next" prerelease version:
  - Rust crates: `X.Y.(Z+1)-dev.0` (SemVer prerelease).
  - Python (PEP 440): `X.Y.(Z+1).dev0`.
  - NPM: `X.Y.(Z+1)-dev.0`.

Nightly builds are tagged automatically on pushes to `nightly`:

- `vX.Y.Z-dev.YYYYMMDD.<run>`

These tags are for traceability and testing, not stability guarantees.

## GitHub Settings (Required)

Configure branch protection rules in GitHub (Settings -> Branches):

- Protect `main`:
  - Require pull request reviews.
  - Require status checks to pass (CI jobs).
  - Disallow force-push.
  - Disallow direct pushes (maintainers only via PR).
- Protect `nightly`:
  - Require status checks to pass (CI jobs).
  - Disallow force-push.
- `dev`:
  - Require status checks to pass (CI jobs).
  - Optional: require PRs (recommended for external contributors).

## Release Checklist

1. Ensure `nightly` is green and stable.
2. Update versions on `nightly` to stable `X.Y.Z` (remove prerelease suffixes).
3. Merge `nightly` -> `main`.
4. Create annotated tag `vX.Y.Z` on `main`.
5. Immediately bump `dev` to `X.Y.(Z+1)-dev.0` (and Python/NPM equivalents).

