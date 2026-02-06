# rlm

Standalone reward verifier for evaluating outputs against weighted signals.

This crate is general-purpose and has **no dependency on Horizons**.

## Core Ideas

- Define a set of `RewardSignal`s with weights.
- Verify a `VerificationCase` to produce a `RewardOutcome` in `[0,1]`.
- Generate an `EvalReport` and render it as Markdown or JSON.

