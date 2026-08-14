#!/usr/bin/env python3
"""Independent verifier for d-raft.kv-checkpoint/v1 canonical JSON."""

from __future__ import annotations

import argparse
import base64
import binascii
import hashlib
import json
import struct
import sys
from pathlib import Path
from typing import Any

COMMAND_MAGIC = b"DRAFTKV1"
COMMAND_SCHEMA = "d-raft.kv-command/v1"
STATE_SCHEMA = "d-raft.kv-state/v1"
CHAIN_SCHEMA = "d-raft.kv-chain/v1"
CHECKPOINT_SCHEMA = "d-raft.kv-checkpoint/v1"
COMMITMENT_SCHEMA = "d-raft.kv-commitment/v1"

MAX_COMMAND_BYTES = 1 << 20
MAX_CHECKPOINT_BYTES = 64 << 20
MAX_KEY_BYTES = 64 << 10
MAX_VALUE_BYTES = MAX_COMMAND_BYTES - 64
MAX_COMMANDS = 50_000
MAX_HISTORY_BYTES = 12 << 20
MAX_STATE_BYTES = 8 << 20

CHECKPOINT_FIELDS = ["schema", "commands", "chain_digest", "state_digest", "state", "history"]
PAIR_FIELDS = ["key", "value"]
BLOCK_FIELDS = ["ordinal", "command_id", "command", "command_digest", "state_digest", "digest"]

KAT_CHECKPOINT = (
    '{"schema":"d-raft.kv-checkpoint/v1","commands":"1",'
    '"chain_digest":"a7da7c9ae45a7c9560197d4237b1a65d641f128b525dfba6f77c82beb3162ccd",'
    '"state_digest":"5afa47ab2fcf92ba11bc6cb680aee8049d589f848614af90bbcf34f9bc1b4c00",'
    '"state":[{"key":"eA==","value":"MQ=="}],'
    '"history":[{"ordinal":"1","command_id":"000102030405060708090a0b0c0d0e0f",'
    '"command":"RFJBRlRLVjEBAAECAwQFBgcICQoLDA0ODwAAAAEAAAABeDE=",'
    '"command_digest":"a4af80c9764356340696c115937255fd4157e4900d57859758706e8e79f8d62a",'
    '"state_digest":"5afa47ab2fcf92ba11bc6cb680aee8049d589f848614af90bbcf34f9bc1b4c00",'
    '"digest":"a7da7c9ae45a7c9560197d4237b1a65d641f128b525dfba6f77c82beb3162ccd"}]}'
)


class VerificationError(ValueError):
    pass


def object_no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise VerificationError(f"duplicate JSON field: {key}")
        result[key] = value
    return result


def require_fields(value: Any, fields: list[str], where: str) -> dict[str, Any]:
    if not isinstance(value, dict) or list(value) != fields:
        got = list(value) if isinstance(value, dict) else type(value).__name__
        raise VerificationError(f"{where} fields/order: got {got}, want {fields}")
    return value


def decimal_counter(value: Any, where: str) -> int:
    if not isinstance(value, str) or not value or not value.isascii() or not value.isdecimal():
        raise VerificationError(f"{where} is not a decimal string")
    parsed = int(value)
    if str(parsed) != value or parsed > (1 << 64) - 1:
        raise VerificationError(f"{where} is not canonical uint64")
    return parsed


def lower_hex_digest(value: Any, where: str) -> bytes:
    if not isinstance(value, str) or len(value) != 64 or value.lower() != value:
        raise VerificationError(f"{where} is not canonical SHA-256 hex")
    try:
        decoded = bytes.fromhex(value)
    except (ValueError, binascii.Error) as error:
        raise VerificationError(f"{where} is not hexadecimal") from error
    if decoded.hex() != value:
        raise VerificationError(f"{where} is not canonical SHA-256 hex")
    return decoded


def binary(value: Any, where: str) -> bytes:
    if not isinstance(value, str):
        raise VerificationError(f"{where} is not base64 text")
    try:
        decoded = base64.b64decode(value, validate=True)
    except ValueError as error:
        raise VerificationError(f"{where} is not canonical base64") from error
    if base64.b64encode(decoded).decode("ascii") != value:
        raise VerificationError(f"{where} is not canonical padded base64")
    return decoded


def decode_command(encoded: bytes) -> tuple[int, bytes, bytes, bytes]:
    if len(encoded) < 33 or len(encoded) > MAX_COMMAND_BYTES or encoded[:8] != COMMAND_MAGIC:
        raise VerificationError("invalid command framing")
    operation = encoded[8]
    command_id = encoded[9:25]
    key_length, value_length = struct.unpack(">II", encoded[25:33])
    if command_id == bytes(16) or operation not in (1, 2):
        raise VerificationError("invalid command identity or operation")
    if key_length == 0 or key_length > MAX_KEY_BYTES or value_length > MAX_VALUE_BYTES:
        raise VerificationError("invalid command component length")
    if key_length + value_length != len(encoded) - 33:
        raise VerificationError("command length mismatch or trailing bytes")
    key = encoded[33 : 33 + key_length]
    value = encoded[33 + key_length :]
    if operation == 2 and value:
        raise VerificationError("delete command has a value")
    return operation, command_id, key, value


def state_digest(state: dict[bytes, bytes]) -> bytes:
    digest = hashlib.sha256()
    digest.update(STATE_SCHEMA.encode("ascii") + b"\0")
    digest.update(struct.pack(">Q", len(state)))
    for key in sorted(state):
        value = state[key]
        digest.update(struct.pack(">I", len(key)) + key)
        digest.update(struct.pack(">I", len(value)) + value)
    return digest.digest()


def verify(raw: bytes) -> dict[str, Any]:
    if not raw or len(raw) > MAX_CHECKPOINT_BYTES:
        raise VerificationError("checkpoint size outside bounds")
    try:
        checkpoint = json.loads(raw, object_pairs_hook=object_no_duplicates)
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise VerificationError(f"invalid UTF-8 JSON: {error}") from error
    canonical = json.dumps(checkpoint, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if canonical != raw:
        raise VerificationError("checkpoint is not canonical JSON")
    checkpoint = require_fields(checkpoint, CHECKPOINT_FIELDS, "checkpoint")
    if checkpoint["schema"] != CHECKPOINT_SCHEMA:
        raise VerificationError("unknown checkpoint schema")
    command_count = decimal_counter(checkpoint["commands"], "commands")
    expected_chain = lower_hex_digest(checkpoint["chain_digest"], "chain_digest")
    expected_state = lower_hex_digest(checkpoint["state_digest"], "state_digest")
    history = checkpoint["history"]
    pairs = checkpoint["state"]
    if not isinstance(history, list) or not isinstance(pairs, list):
        raise VerificationError("state and history must be arrays")
    if command_count != len(history) or len(history) > MAX_COMMANDS:
        raise VerificationError("history count outside bounds")

    state: dict[bytes, bytes] = {}
    state_bytes = 0
    seen: set[bytes] = set()
    chain = hashlib.sha256(CHAIN_SCHEMA.encode("ascii") + b"\0").digest()
    history_bytes = 0
    for index, untyped_block in enumerate(history, start=1):
        block = require_fields(untyped_block, BLOCK_FIELDS, f"block {index}")
        if decimal_counter(block["ordinal"], f"block {index} ordinal") != index:
            raise VerificationError(f"block {index} ordinal mismatch")
        encoded = binary(block["command"], f"block {index} command")
        history_bytes += len(encoded)
        if history_bytes > MAX_HISTORY_BYTES:
            raise VerificationError("history bytes outside bounds")
        operation, command_id, key, value = decode_command(encoded)
        if not isinstance(block["command_id"], str) or block["command_id"] != command_id.hex():
            raise VerificationError(f"block {index} command ID mismatch")
        if command_id in seen:
            raise VerificationError(f"block {index} duplicates command ID")
        seen.add(command_id)
        command_hash = hashlib.sha256(encoded).digest()
        if lower_hex_digest(block["command_digest"], f"block {index} command digest") != command_hash:
            raise VerificationError(f"block {index} command digest mismatch")
        if operation == 1:
            previous = state.get(key)
            if previous is None:
                state_bytes += len(key)
            else:
                state_bytes -= len(previous)
            state_bytes += len(value)
            state[key] = value
        else:
            previous = state.pop(key, None)
            if previous is not None:
                state_bytes -= len(key) + len(previous)
        if state_bytes > MAX_STATE_BYTES:
            raise VerificationError(f"block {index} state exceeds aggregate bound")
        post_state = state_digest(state)
        if lower_hex_digest(block["state_digest"], f"block {index} state digest") != post_state:
            raise VerificationError(f"block {index} state digest mismatch")
        step = hashlib.sha256()
        step.update(CHAIN_SCHEMA.encode("ascii") + b"\0")
        step.update(chain)
        step.update(struct.pack(">Q", index))
        step.update(struct.pack(">I", len(encoded)))
        step.update(encoded)
        step.update(post_state)
        chain = step.digest()
        if lower_hex_digest(block["digest"], f"block {index} digest") != chain:
            raise VerificationError(f"block {index} chain digest mismatch")

    if chain != expected_chain or state_digest(state) != expected_state:
        raise VerificationError("final commitment mismatch")
    decoded_pairs: list[tuple[bytes, bytes]] = []
    encoded_state_bytes = 0
    for index, untyped_pair in enumerate(pairs):
        pair = require_fields(untyped_pair, PAIR_FIELDS, f"state pair {index}")
        key = binary(pair["key"], f"state pair {index} key")
        value = binary(pair["value"], f"state pair {index} value")
        if not key or len(key) > MAX_KEY_BYTES or len(value) > MAX_VALUE_BYTES:
            raise VerificationError(f"state pair {index} outside bounds")
        encoded_state_bytes += len(key) + len(value)
        if encoded_state_bytes > MAX_STATE_BYTES:
            raise VerificationError("state bytes outside bounds")
        if decoded_pairs and decoded_pairs[-1][0] >= key:
            raise VerificationError("state keys are not strictly sorted")
        decoded_pairs.append((key, value))
    if dict(decoded_pairs) != state:
        raise VerificationError("checkpoint state does not match replayed history")
    return {
        "schema": COMMITMENT_SCHEMA,
        "commands": str(command_count),
        "chain_digest": chain.hex(),
        "state_digest": expected_state.hex(),
    }


def self_test() -> None:
    commitment = verify(KAT_CHECKPOINT.encode("ascii"))
    if commitment["chain_digest"] != "a7da7c9ae45a7c9560197d4237b1a65d641f128b525dfba6f77c82beb3162ccd":
        raise VerificationError("known-answer chain mismatch")
    corrupted = KAT_CHECKPOINT.replace("MQ==", "Mg==", 1).encode("ascii")
    try:
        verify(corrupted)
    except VerificationError:
        return
    raise VerificationError("corrupted known-answer checkpoint was accepted")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("checkpoint", nargs="?", help="canonical checkpoint JSON file; omit to read stdin")
    parser.add_argument("--self-test", action="store_true", help="verify fixed valid and corrupted vectors")
    arguments = parser.parse_args()
    try:
        if arguments.self_test:
            self_test()
            print("apporacle verifier self-test: ok")
            return 0
        raw = Path(arguments.checkpoint).read_bytes() if arguments.checkpoint else sys.stdin.buffer.read()
        commitment = verify(raw)
        print(json.dumps(commitment, separators=(",", ":")))
        return 0
    except (OSError, VerificationError) as error:
        print(f"apporacle verification failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
