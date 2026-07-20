# Rust Store Migration Change Logs

This directory records behavior and feature migrations from the upstream
C++/Go store and Transfer Engine implementations into `rust-repo`.

## File naming

Use `YYYY-MM-DD-NNN.md`, where `NNN` is a zero-padded sequence number for the
date. Example: `2026-07-20-001.md`.

Feature names belong in the document headings rather than in the file name.

## Required sections

Each entry should record:

- upstream commits or source behavior;
- the Rust gap being closed;
- implementation and compatibility notes;
- tests and verification performed;
- the resulting Rust commit, once committed.
