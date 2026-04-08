# Kumquat Blockchain

## Product Overview

Kumquat is a blockchain built from scratch that models money as individual digital cash units rather than account balances.

Each unit of value is a unique, non-fungible piece of money with a fixed denomination. This includes whole-value denominations like `100`, `50`, `20`, `10`, `5`, and `1`, as well as fractional change denominations like `0.50`, `0.25`, `0.10`, `0.05`, and `0.01`.

Users do not simply hold a balance. They hold specific digital bills and coins, each with its own identity, and transactions work by transferring those exact units between users, similar to handing over real cash.

The blockchain is being built entirely from scratch in Rust. It uses Firefly for in-memory state, and the app server layer is flexible, using whatever works best to connect with the Rust backend.

The goal of Kumquat is to create a cash-like blockchain where money is denomination-based, granular down to cents, and every unit remains uniquely owned and traceable.
