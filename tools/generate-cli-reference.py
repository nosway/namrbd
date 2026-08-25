#!/usr/bin/env python3
"""Generate public and canonical-internal CLI references from fresh binaries.

The canonical internal reference preserves the untagged Community binary's raw
accepted syntax, including hidden development flags and known edition-boundary
drift.  The public reference applies the documented Community edition filter
and excludes internal, lab, fixture, and Enterprise-adjacent surfaces.  Source
inspection is used for hidden-flag and environment-variable discovery.

Generated pages are deterministic across hosts: host/user-derived defaults,
ambient NAMRBD configuration, and temporary binary paths are normalized.
"""

from __future__ import annotations

import argparse
import difflib
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[1]
PUBLIC_OUTPUT_DIR = ROOT / "docs-src" / "reference" / "cli"
INTERNAL_OUTPUT_DIR = ROOT / "docs" / "reference" / "cli"

BINARIES = {
    "namrbdctl": {
        "role": "Linux host/device control and gateway-facing volume I/O",
        "scope": "Shipped (Community and Enterprise)",
        "top_args": ["--help"],
        "top_exit": 0,
        "commands": "namrbdctl",
        "version": "first argument `version` or `--version`",
        "notes": [
            "The direct-etcd metadata commands construct independent FlagSets; "
            "they do not load `namrbdctl` context-file or environment defaults. "
            "Pass their etcd endpoint/root flags explicitly when the built-ins "
            "are not correct.",
        ],
    },
    "sbsctl": {
        "role": "SBS cluster, volume, snapshot, maintenance, and basic iSCSI administration",
        "scope": "Shipped; this page is generated from the Community build",
        "top_args": ["--help"],
        "top_exit": 0,
        "commands": "sbsctl",
        "version": "first argument `version` or `--version`",
        "public_notes": [
            "The compiled top-level/group help is not an exhaustive command "
            "inventory. The command index below applies the reviewed public "
            "Community edition filter to the compiled leaf FlagSets.",
            "Root `--json` also selects the structured fatal-error path. A leaf "
            "`--output=json` flag controls successful result formatting but does "
            "not by itself change fatal-error output.",
        ],
        "internal_notes": [
            "The compiled top-level/group help is not an exhaustive command "
            "inventory. The command index below discovers every leaf FlagSet "
            "compiled into the Community build and executes its help path.",
            "Root `--json` also selects the structured fatal-error path. A leaf "
            "`--output=json` flag controls successful result formatting but does "
            "not by itself change fatal-error output.",
        ],
    },
    "namrbd-debug": {
        "role": "Low-level inspection, workload, and break-glass utility",
        "scope": "Internal/lab only; not a v1.0 release artifact",
        "top_args": [],
        "top_exit": 2,
        "commands": "namrbd-debug",
        "version": "first argument `version` or `--version`",
    },
    "namrbd-gateway": {
        "role": "Gateway control and data-plane daemon",
        "scope": "Shipped daemon",
        "top_args": ["--help"],
        "top_exit": 0,
        "version": "any argument equal to `version` or `--version`",
    },
    "sbs-service": {
        "role": "SBS metadata and administrative authority",
        "scope": "Shipped daemon",
        "top_args": ["--help"],
        "top_exit": 0,
        "version": "any argument equal to `version` or `--version`",
    },
    "sbs-data": {
        "role": "SBS payload service",
        "scope": "Shipped daemon",
        "top_args": ["--help"],
        "top_exit": 0,
        "version": "any argument equal to `version` or `--version`",
        "notes": [
            "When `--config` is present, cluster-ID environment/CLI values are "
            "initial defaults rather than registered post-file overrides and can "
            "be replaced by YAML. The configuration reference records the field-"
            "specific precedence contract.",
        ],
    },
    "namrbd-iscsi-gateway": {
        "role": "Basic iSCSI target gateway",
        "scope": "Shipped daemon; Community supports at most three distinct exported volumes",
        "top_args": ["--help"],
        "top_exit": 0,
        "version": "first argument `version` or `--version`",
        "notes": [
            "The config override registry names `--advertise-portals`, but the "
            "actual CLI exposes `--portal`; with `--config`, a YAML portal can "
            "therefore replace an explicit `--portal` value. This is a documented "
            "implementation limitation, not recommended precedence.",
        ],
    },
    "namrbd-csi-driver": {
        "role": "Kubernetes CSI Identity, Controller, and Node service",
        "scope": "Shipped daemon",
        "top_args": ["--help"],
        "top_exit": 0,
        "version": "first argument `version` or `--version`",
    },
    "namrbd-mcp": {
        "role": "Read-only MCP operations server",
        "scope": "Shipped daemon; observe posture only in large_scale",
        "top_args": ["--help"],
        "top_exit": 2,
        "version": "first argument `version` or `--version`",
    },
}

PUBLIC_BINARIES = tuple(binary for binary in BINARIES if binary != "namrbd-debug")

# The public reference is a supported Community surface, not a dump of every
# parser branch retained in the canonical repository.  Exact entries cover
# known common-code drift.  Token checks keep newly added Enterprise families
# from silently entering docs-src before the edition boundary is corrected.
PUBLIC_DENIED_COMMAND_PREFIXES = (
    "sbsctl ec ",
    "sbsctl clone ",
    "sbsctl backup ",
    "sbsctl dr ",
    "sbsctl performance ",
    "sbsctl security ",
    "sbsctl mobility ",
    "sbsctl dedupe ",
    "sbsctl iscsi failover ",
)

PUBLIC_DENIED_FLAG_NAMES = {
    "active-iscsi-gateway-id",
    "alua-access-state",
    "alua-preferred",
    "alua-target-port-group-id",
    "ec-profile",
    "export-epoch",
    "export-lease-id",
    "redundancy-backend",
    "weak-placement",
}

PUBLIC_DENIED_SURFACE_TOKENS = {
    "alua",
    "backup",
    "clone",
    "crypto",
    "dedupe",
    "diff-index",
    "dr",
    "ec",
    "encryption",
    "failover",
    "ha",
    "journal",
    "kms",
    "materialize",
    "mobility",
    "performance",
    "qos",
    "recovery-point",
    "repack",
    "restore-warmup",
    "security",
    "shipping",
    "standby-volume",
}

PUBLIC_DENIED_ENV_TOKENS = {
    token.replace("-", "_").upper() for token in PUBLIC_DENIED_SURFACE_TOKENS
}

PUBLIC_DENIED_ENV_NAMES = {
    "NAMRBD_SBS_ASYNC_WRITE_MUTATION_FINALIZE",
    "NAMRBD_SBS_DATA_OPERATION_TRACE",
    "NAMRBD_SBS_ENABLE_LAB_STORE_DEBUG",
    "NAMRBD_SBS_LAB_CACHE_OPEN_VOLUME_SPEC",
    "NAMRBD_SBS_LAB_DISABLE_IDEMPOTENCY_SYNC",
    "NAMRBD_SBS_LAB_DISABLE_PHYSICAL_WRITE_IDEMPOTENCY",
    "NAMRBD_SBS_WRITE_EFFECTS_LANE_BUCKET_COUNT",
    "NAMRBD_SBS_WRITE_INTENT_BATCH_COALESCE_WAIT",
}

CONFIG_OVERRIDE_FUNCTIONS = {
    "namrbd-gateway": "gatewayOverrides",
    "namrbd-iscsi-gateway": "iscsiGatewayOverrides",
    "sbs-service": "sbsServiceOverrides",
    "sbs-data": "sbsDataOverrides",
    "namrbd-csi-driver": "csiDriverOverrides",
    "namrbd-mcp": "mcpOverrides",
}

DEPRECATED_FLAG_ALIASES = {
    "namrbd-gateway": [
        ("--listen", "--control-http-listen"),
        ("--sbs-admin-endpoint", "--sbs-service-endpoint"),
    ],
    "sbsctl": [
        ("--admin-endpoint", "--sbs-service-endpoint"),
        ("--admin-http-endpoint", "--sbs-service-http-endpoint"),
    ],
    "sbs-data": [
        ("--grpc-listen", "--sbs-data-listen"),
        ("--http-listen", "--sbs-data-http-listen"),
    ],
    "namrbd-iscsi-gateway": [
        ("--sbs-endpoint", "--sbs-data-endpoint"),
        ("--sbs-admin-endpoint", "--sbs-service-endpoint"),
    ],
}

GO_FLAG_METHODS = (
    "Bool", "Duration", "Float64", "Func", "Int", "Int64", "String",
    "Uint", "Uint64", "Var", "BoolVar", "DurationVar", "Float64Var",
    "FuncVar", "IntVar", "Int64Var", "StringVar", "UintVar", "Uint64Var",
)


def run(command: list[str], *, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        command,
        cwd=ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
    )
    return completed


def go_env() -> dict[str, str]:
    env = os.environ.copy()
    # The generated defaults are a source contract, not the maintainer's local
    # NAMRBD deployment.  Also defeat a persisted `GOFLAGS=-tags=enterprise` so
    # the checked-in public reference is always built from the Community shape.
    for name in list(env):
        if (
            name.startswith("NAMRBD_")
            or name.startswith("SBS_")
            or name in {"NAMRBDCTL", "HOSTNAME"}
        ):
            env.pop(name, None)
    env["GOFLAGS"] = ""
    env.setdefault("GOCACHE", str(ROOT / ".cache" / "go-build"))
    env.setdefault("GOMODCACHE", str(ROOT / ".cache" / "go-mod"))
    env["LC_ALL"] = "C"
    env["LANG"] = "C"
    env["USER"] = "namrbd-docs"
    Path(env["GOCACHE"]).mkdir(parents=True, exist_ok=True)
    Path(env["GOMODCACHE"]).mkdir(parents=True, exist_ok=True)
    return env


def build_binaries(destination: Path, env: dict[str, str]) -> dict[str, Path]:
    paths: dict[str, Path] = {}
    for binary in BINARIES:
        output = destination / binary
        completed = run(
            ["go", "build", "-buildvcs=false", "-o", str(output), f"./cmd/{binary}"],
            env=env,
        )
        if completed.returncode != 0:
            raise RuntimeError(f"build {binary} failed:\n{completed.stdout}")
        paths[binary] = output
    return paths


def package_files(binary: str, env: dict[str, str]) -> list[Path]:
    completed = run(["go", "list", "-json", f"./cmd/{binary}"], env=env)
    if completed.returncode != 0:
        raise RuntimeError(f"go list {binary} failed:\n{completed.stdout}")
    package = json.loads(completed.stdout)
    directory = Path(package["Dir"])
    return [directory / name for name in package.get("GoFiles", [])]


def normalize_help(text: str, binary_path: Path, binary: str) -> str:
    normalized = text.replace(str(binary_path), binary)
    hostname = socket.gethostname()
    if hostname:
        normalized = normalized.replace(hostname, "<hostname>")
    normalized = normalized.replace(str(ROOT), "<repo>")
    return normalized.expandtabs(4).rstrip() + "\n"


def capture_help(binary_path: Path, binary: str, args: list[str], expected: int, env: dict[str, str]) -> str:
    completed = run([str(binary_path), *args], env=env)
    if completed.returncode != expected:
        rendered = " ".join([binary, *args])
        raise RuntimeError(
            f"{rendered} exited {completed.returncode}, expected {expected}:\n{completed.stdout}"
        )
    return normalize_help(completed.stdout, binary_path, binary)


def discover_namrbdctl_commands(top_help: str) -> list[str]:
    commands = set(re.findall(r"^\s+namrbdctl ([a-z0-9][a-z0-9-]*)", top_help, re.MULTILINE))
    commands.discard("help")
    commands.discard("version")
    return sorted(commands)


def discover_literal_flagsets(files: list[Path], call_name: str) -> set[str]:
    found: set[str] = set()
    pattern = re.compile(rf"{re.escape(call_name)}\(\s*\"([^\"]+)\"")
    for path in files:
        found.update(pattern.findall(path.read_text(encoding="utf-8")))
    return found


def discover_commands(binary: str, files: list[Path], top_help: str) -> list[str]:
    if binary == "namrbdctl":
        return discover_namrbdctl_commands(top_help)
    if binary == "sbsctl":
        commands = discover_literal_flagsets(files, "flag.NewFlagSet")
        commands.discard("sbsctl")
        commands.update({
            "iscsi portal enable", "iscsi portal disable",
            "iscsi target enable", "iscsi target disable",
        })
        return sorted(command for command in commands if " " in command and command == command.strip())
    if binary == "namrbd-debug":
        commands = discover_literal_flagsets(files, "flag.NewFlagSet")
        commands.update(discover_literal_flagsets(files, "newCommandFlagSet"))
        return sorted(command for command in commands if " " in command)
    return []


def command_help_args(binary: str, command: str) -> list[str]:
    words = command.split()
    if binary in {"namrbdctl", "sbsctl"}:
        return ["help", *words]
    if binary == "namrbd-debug":
        return [*words, "--help"]
    raise ValueError(f"{binary} has no command help dispatcher")


def split_go_args(raw: str) -> list[str]:
    args: list[str] = []
    start = 0
    depth = 0
    quote = ""
    escaped = False
    for index, char in enumerate(raw):
        if quote:
            if quote == '"' and escaped:
                escaped = False
            elif quote == '"' and char == "\\":
                escaped = True
            elif char == quote:
                quote = ""
            continue
        if char in {'"', '`'}:
            quote = char
        elif char in "([{":
            depth += 1
        elif char in ")]}":
            depth -= 1
        elif char == "," and depth == 0:
            args.append(raw[start:index].strip())
            start = index + 1
    args.append(raw[start:].strip())
    return args


def extract_balanced_call(text: str, open_paren: int) -> tuple[str, int]:
    depth = 1
    quote = ""
    escaped = False
    index = open_paren + 1
    while index < len(text):
        char = text[index]
        if quote:
            if quote == '"' and escaped:
                escaped = False
            elif quote == '"' and char == "\\":
                escaped = True
            elif char == quote:
                quote = ""
        elif char in {'"', '`'}:
            quote = char
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return text[open_paren + 1:index], index + 1
        index += 1
    raise ValueError("unterminated Go call")


def go_string(raw: str) -> str | None:
    raw = raw.strip()
    if raw.startswith('"') and raw.endswith('"'):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return None
    if raw.startswith("`") and raw.endswith("`"):
        return raw[1:-1]
    return None


def go_description(raw: str) -> str:
    literals = re.findall(r'"((?:\\.|[^"\\])*)"|`([^`]*)`', raw)
    parts = []
    for quoted, backtick in literals:
        token = backtick if backtick else bytes(quoted, "utf-8").decode("unicode_escape")
        parts.append(token)
    return "".join(parts).strip() or raw.strip()


def source_flag_registrations(files: list[Path]) -> dict[str, dict[str, str]]:
    registrations: dict[str, dict[str, str]] = {}
    methods = "|".join(GO_FLAG_METHODS)
    pattern = re.compile(rf"\b(?:flag|fs)\.({methods})\s*\(")
    for path in files:
        text = path.read_text(encoding="utf-8")
        for match in pattern.finditer(text):
            raw, _ = extract_balanced_call(text, match.end() - 1)
            args = split_go_args(raw)
            method = match.group(1)
            is_var_target = method.endswith("Var") and method != "Var"
            if method == "Var":
                name_index, default_index, usage_index = 1, None, 2
            elif is_var_target:
                name_index, default_index, usage_index = 1, 2, 3
            else:
                name_index, default_index, usage_index = 0, 1, 2
            if len(args) <= name_index:
                continue
            name = go_string(args[name_index])
            if not name:
                continue
            default = "value-defined" if default_index is None or len(args) <= default_index else args[default_index]
            usage = "" if len(args) <= usage_index else go_description(args[usage_index])
            line = text.count("\n", 0, match.start()) + 1
            registrations[name] = {
                "default": default,
                "usage": usage,
                "source": f"cmd/{path.parent.name}/{path.name}:{line}",
            }
    return registrations


def visible_flags(help_text: str) -> set[str]:
    return set(re.findall(r"^\s+--?([a-z0-9][a-z0-9-]*)\b", help_text, re.MULTILINE))


def contains_bounded_token(name: str, token: str, separator: str) -> bool:
    return (
        name == token
        or name.startswith(token + separator)
        or name.endswith(separator + token)
        or (separator + token + separator) in name
    )


def public_command_allowed(binary: str, command: str) -> bool:
    rendered = f"{binary} {command} "
    if any(rendered.startswith(prefix) for prefix in PUBLIC_DENIED_COMMAND_PREFIXES):
        return False
    return not any(
        contains_bounded_token(command, token, "-")
        or token in command.split()
        for token in PUBLIC_DENIED_SURFACE_TOKENS
    )


def public_flag_allowed(name: str) -> bool:
    if name in PUBLIC_DENIED_FLAG_NAMES:
        return False
    return not any(
        contains_bounded_token(name, token, "-")
        for token in PUBLIC_DENIED_SURFACE_TOKENS
    )


def public_env_allowed(name: str) -> bool:
    if name in PUBLIC_DENIED_ENV_NAMES:
        return False
    return not any(
        contains_bounded_token(name, token, "_")
        for token in PUBLIC_DENIED_ENV_TOKENS
    )


def filter_help_flags(help_text: str) -> str:
    """Remove denied flag blocks while retaining the binary's help layout."""
    lines = help_text.rstrip("\n").splitlines()
    flag_start = re.compile(r"^\s+--?([a-z0-9][a-z0-9-]*)\b")
    filtered: list[str] = []
    index = 0
    while index < len(lines):
        match = flag_start.match(lines[index])
        if not match or public_flag_allowed(match.group(1)):
            filtered.append(lines[index])
            index += 1
            continue
        index += 1
        while index < len(lines) and not flag_start.match(lines[index]):
            index += 1
    return "\n".join(filtered).rstrip() + "\n"


def filter_environment_inventory(inventory: dict[str, set[str]]) -> dict[str, set[str]]:
    return {
        classification: {name for name in names if public_env_allowed(name)}
        for classification, names in inventory.items()
    }


def extract_function_block(text: str, function: str) -> str:
    match = re.search(rf"func\s+{re.escape(function)}\s*\([^)]*\)[^{{]*{{", text)
    if not match:
        return ""
    open_brace = text.find("{", match.start())
    depth = 1
    index = open_brace + 1
    while index < len(text):
        if text[index] == "{":
            depth += 1
        elif text[index] == "}":
            depth -= 1
            if depth == 0:
                return text[open_brace + 1:index]
        index += 1
    return ""


def envcompat_catalog() -> dict[str, tuple[str, list[str]]]:
    text = (ROOT / "internal" / "envcompat" / "catalog.go").read_text(encoding="utf-8")
    catalog: dict[str, tuple[str, list[str]]] = {}
    for match in re.finditer(r"(?m)^\s*([A-Za-z0-9_]+)\s*=\s*New\(([^)]*)\)", text):
        values = [go_string(arg) for arg in split_go_args(match.group(2))]
        clean = [value for value in values if value]
        if clean:
            catalog[match.group(1)] = (clean[0], clean[1:])
    return catalog


def environment_inventory(binary: str, files: list[Path]) -> dict[str, set[str]]:
    text = "\n".join(path.read_text(encoding="utf-8") for path in files)
    direct = set(re.findall(r"\b(?:NAMRBD|SBS)_[A-Z0-9_]+\b", text))
    for match in re.finditer(r"(?:Getenv|LookupEnv|getenv[A-Za-z0-9_]*|firstEnv[A-Za-z0-9_]*)\(\s*\"([A-Z][A-Z0-9_]*)\"", text):
        direct.add(match.group(1))

    registry_text = (ROOT / "internal" / "serviceconfig" / "registry.go").read_text(encoding="utf-8")
    registry = ""
    if binary in CONFIG_OVERRIDE_FUNCTIONS:
        registry = extract_function_block(registry_text, CONFIG_OVERRIDE_FUNCTIONS[binary])

    catalog = envcompat_catalog()
    direct_symbols = set(re.findall(r"envcompat\.([A-Za-z0-9_]+)", text))
    override_symbols = set(re.findall(r"envcompat\.([A-Za-z0-9_]+)", registry))
    override: set[str] = set(re.findall(r"\bNAMRBD_[A-Z0-9_]+\b", registry))
    legacy: set[str] = set()
    for symbol in direct_symbols:
        if symbol not in catalog:
            continue
        current, old = catalog[symbol]
        direct.add(current)
        legacy.update(old)
    for symbol in override_symbols:
        if symbol not in catalog:
            continue
        current, old = catalog[symbol]
        override.add(current)
        legacy.update(old)
    direct.difference_update(legacy)
    override.difference_update(legacy)
    return {"direct": direct, "override": override, "legacy": legacy}


def markdown_code(text: str) -> str:
    return f"```text\n{text.rstrip()}\n```\n"


def binary_rows(command_counts: dict[str, int], binaries: tuple[str, ...]) -> str:
    rows = []
    for binary in binaries:
        metadata = BINARIES[binary]
        count = command_counts.get(binary, 0)
        surface = f"{count} command paths" if count else "daemon flags"
        rows.append(
            f"| [`{binary}`]({binary}.md) | {metadata['role']} | {metadata['scope']} | {surface} |"
        )
    return "\n".join(rows)


def render_public_index(command_counts: dict[str, int]) -> str:
    return """# CLI Command Reference

This reference is generated from freshly built, untagged Community binaries at
the current source revision and filtered through the reviewed public Community
edition policy. It covers every shipped Community binary. Internal, fixture,
hidden, and Enterprise-adjacent parser surfaces are retained only in the
canonical internal reference and are not published here as supported syntax. The
[Feature Status](../../feature-status.md) page remains authoritative for release
support and edition availability.

`namrbd-iscsictl` is deprecated and not shipped in v1.0; use `sbsctl iscsi`.
Internal debug and benchmark binaries are not part of this public reference.
Historical `namrbd-meta` source is archived and is not an active command surface.

## Binary map

| Binary | Purpose | Distribution | Reference shape |
| --- | --- | --- | --- |
""" + binary_rows(command_counts, PUBLIC_BINARIES) + """

## Common invocation rules

- `namrbdctl help COMMAND`, `sbsctl help COMMAND [SUBCOMMAND ...]`, and a
  trailing `help` on their leaf commands request command help.
- `--json` is a root convenience. Where a leaf has `--output`, `sbsctl`
  rewrites it to `--output=json`; `namrbdctl` emits its JSON result/error form.
- Flags are parsed before positional leftovers by Go's `flag` package. Commands
  in this reference use the command path itself as the positional argument;
  leaf inputs are flags unless a synopsis explicitly says otherwise. Most leaf
  handlers currently do not reject surplus positional operands, so an ignored
  extra token must never be treated as a successful input.
- Configuration precedence for context-aware CLIs is built-in default, context
  file, environment, then explicit CLI flag. Daemon `--config` precedence and
  its narrower override allowlist are documented on each daemon page and in the
  configuration reference.
- Deprecated flag aliases emit a warning on stderr and are rewritten to the
  canonical spelling. Deprecated environment variables use canonical-over-
  legacy precedence, warn in v1.0.x, and are removed in v1.1.0.

## Exit status contract

| Status | General meaning | Important exceptions |
| --- | --- | --- |
| `0` | Success, graceful daemon stop, or supported help/version request | With JSON output selected, `namrbdctl validate-volume` and `namrbdctl validate-all` currently return `0` for an invalid report; their text paths return `1`. JSON consumers must inspect the result fields. |
| `1` | Runtime/operation failure or `fatalf` validation failure | Many `sbsctl` missing-required-flag checks use this status. |
| `2` | Unknown command, malformed command line, or flag parse/config admission failure | `namrbd-mcp --help` also returns `2`; `namrbd-iscsi-gateway` uses `2` for startup/config/admission validation and `1` for a running self-test or serving failure. |

Scripts must not treat every nonzero status as the same error class: parse and
admission failures (`2`) are different from an attempted operation that failed
(`1`). JSON-producing commands keep diagnostics on stderr.

## Edition and hidden surfaces

The checked-in pages are generated without `-tags=enterprise` and then filtered
through an explicit public-surface policy. Enterprise command groups, advanced
iSCSI HA/ALUA controls, Enterprise-only common-command options, and flags hidden
from normal help are absent. The generator fails if a future denied command,
flag, or environment family survives into the public pages.

## Reproducing this reference

```bash
make cli-reference
make cli-reference-check
```

The check rebuilds the Community command packages, executes every discovered
leaf help path without contacting a service, normalizes host-derived defaults,
and fails on a generated Markdown diff in either the public or canonical
internal output tree.
"""


def render_internal_index(command_counts: dict[str, int]) -> str:
    return """# Internal raw Community CLI inventory

This canonical-repository reference is generated from freshly built, untagged
Community binaries. It intentionally preserves every accepted command path,
visible flag, hidden development/fixture flag, and source-discovered environment
variable. It is an engineering inventory, not the public support contract, and
must not be copied into the Community public documentation without edition and
support review.

## Known Community edition-boundary drift

The raw untagged binaries currently retain the following parser surfaces even
though the public Community reference filters them. Their presence here is
deliberate evidence for follow-up product-boundary work, not an availability
claim.

| Binary | Raw accepted surface |
| --- | --- |
| `sbsctl` | iSCSI failover commands and EC options on `volume create` |
| `namrbd-iscsi-gateway` | active-gateway, export lease/epoch, and ALUA flags |
| `namrbd-gateway` | hidden Phase O performance-admission flags |
| `sbs-service` | EC maintenance environment inputs |
| `namrbd-debug` | internal clone I/O inspection plus unsafe/lab commands |

## Binary map

| Binary | Purpose | Distribution | Reference shape |
| --- | --- | --- | --- |
""" + binary_rows(command_counts, tuple(BINARIES)) + """

## Inventory conventions

- This is the raw untagged parser surface. A listed flag is accepted syntax,
  not a production-support statement.
- Hidden flag defaults are Go source expressions when ordinary help omits the
  flag.
- Environment classifications distinguish command-package reads from shared
  service-config overrides; legacy aliases retain their v1.0.x warning and
  v1.1.0 removal contract.
- Most leaf handlers do not reject surplus positional operands.

## Exit status summary

| Status | General meaning | Important exceptions |
| --- | --- | --- |
| `0` | Success, graceful daemon stop, or supported help/version request | With JSON output selected, both `namrbdctl validate-*` commands and `namrbd-debug validate extents` return `0` for an invalid report; text paths return `1`. |
| `1` | Runtime/operation failure or fatal validation failure | Some `sbsctl` groups use `1` for usage or edition gates. |
| `2` | Unknown command, malformed flags, or startup/config admission failure | `namrbd-mcp --help` and bare `namrbd-debug` return `2`. |

## Reproducing both references

```bash
make cli-reference
make cli-reference-check
```

The check rebuilds the untagged binaries once and compares both generated
output trees.
"""


def render_environment(inventory: dict[str, set[str]]) -> str:
    lines = [
        "## Environment variables",
        "",
        "The inventory below is source-derived. `config override` means the",
        "variable is on the shared service-config allowlist; `direct runtime`",
        "means the command package reads it while constructing defaults or a",
        "runtime option. Legacy aliases warn in v1.0.x and are removed in v1.1.0.",
        "",
        "| Variable | Classification |",
        "| --- | --- |",
    ]
    merged: dict[str, set[str]] = {}
    for classification, names in inventory.items():
        label = {
            "direct": "direct runtime/default input",
            "override": "config override (canonical)",
            "legacy": "deprecated compatibility alias",
        }[classification]
        for name in names:
            merged.setdefault(name, set()).add(label)
    if not merged:
        lines.append("| _None_ | No command-package environment input |")
    else:
        for name in sorted(merged):
            lines.append(f"| `{name}` | {', '.join(sorted(merged[name]))} |")
    return "\n".join(lines) + "\n"


def render_binary_page(
    binary: str,
    metadata: dict[str, object],
    audience: str,
    top_help: str,
    commands: list[str],
    command_help: dict[str, str],
    hidden: dict[str, dict[str, str]],
    env_inventory: dict[str, set[str]],
) -> str:
    version = metadata.get("version")
    lines = [
        f"# `{binary}` reference",
        "",
        f"**Purpose:** {metadata['role']}",
        "",
        f"**Scope:** {metadata['scope']}",
        "",
        f"**Top-level help status:** `{metadata['top_exit']}`",
    ]
    if version:
        lines.extend(["", f"**Version selector:** {version}; success status `0`"])
    if audience == "public":
        lines.extend([
            "",
            "This page is generated from the untagged Community build and filtered",
            "through the reviewed public Community edition policy. Defaults shown",
            "inside help are executable defaults after environment lookup;",
            "`<hostname>` marks a runtime-derived host value.",
        ])
    else:
        lines.extend([
            "",
            "**Audience:** canonical-repository maintainers; not public support",
            "documentation.",
            "",
            "This page preserves the raw untagged Community parser surface, including",
            "hidden development/fixture flags and known edition-boundary drift.",
            "Defaults shown inside help are executable defaults after environment",
            "lookup; `<hostname>` marks a runtime-derived host value.",
        ])
    lines.extend([
        "",
        "## Top-level help",
        "",
        markdown_code(top_help).rstrip(),
        "",
    ])

    aliases = DEPRECATED_FLAG_ALIASES.get(binary, [])
    if aliases:
        lines.extend([
            "## Deprecated flag aliases",
            "",
            "| Accepted legacy spelling | Canonical spelling |",
            "| --- | --- |",
        ])
        for legacy, canonical in aliases:
            lines.append(f"| `{legacy}` | `{canonical}` |")
        lines.append("")

    lines.append(render_environment(env_inventory).rstrip())
    lines.append("")

    notes = metadata.get(f"{audience}_notes", metadata.get("notes", []))
    if notes:
        lines.extend(["## Behavior notes", ""])
        for note in notes:
            lines.append(f"- {note}")
        lines.append("")

    if hidden:
        lines.extend([
            "## Accepted flags hidden from normal help",
            "",
            "These flags are retained for development, lab, fixture, or legacy",
            "compatibility. Their absence from normal `--help` is deliberate; do",
            "not use them as production configuration or support assumptions.",
            "Defaults are Go source expressions when the production help omits",
            "the flag.",
            "",
            "| Flag | Source default | Meaning |",
            "| --- | --- | --- |",
        ])
        for name in sorted(hidden):
            item = hidden[name]
            default = item["default"].replace("|", "\\|")
            usage = item["usage"].replace("|", "\\|")
            lines.append(f"| `--{name}` | `{default}` | {usage} |")
        lines.append("")

    if commands:
        lines.extend([
            "## Command index",
            "",
            "| Command path | Help invocation |",
            "| --- | --- |",
        ])
        for command in commands:
            help_invocation = " ".join([binary, *command_help_args(binary, command)])
            lines.append(f"| `{binary} {command}` | `{help_invocation}` |")
        lines.append("")
        lines.append("## Command flags and defaults")
        lines.append("")
        for command in commands:
            lines.extend([
                f"### `{binary} {command}`",
                "",
                markdown_code(command_help[command]).rstrip(),
                "",
            ])

    lines.extend([
        "## Source of truth",
        "",
        f"- Entry point: `cmd/{binary}`",
        "- Shared help and deprecated-flag behavior: `internal/cliux`",
        "- Environment rename behavior: `internal/envcompat`",
        "",
    ])
    return "\n".join(lines)


def validate_public_pages(pages: dict[Path, str]) -> None:
    if Path("namrbd-debug.md") in pages:
        raise ValueError("public CLI reference must not include namrbd-debug")
    forbidden_help_group = re.compile(
        r"(?m)^  (?:ec|clone|backup|dr|performance|security|mobility|dedupe)(?:\s|$)"
    )
    forbidden_fragments = (
        "redundancy backend: replicated|ec",
        "EC profile id for ec volumes",
        "allow explicit weak EC placement",
    )
    for relative, content in pages.items():
        if "namrbd-debug" in content:
            raise ValueError(f"public CLI leakage in {relative}: namrbd-debug")
        for prefix in PUBLIC_DENIED_COMMAND_PREFIXES:
            if prefix.rstrip() in content:
                raise ValueError(f"public CLI leakage in {relative}: {prefix.rstrip()}")
        if forbidden_help_group.search(content):
            raise ValueError(f"public CLI leakage in {relative}: denied command group")
        for name in re.findall(r"--([a-z0-9][a-z0-9-]*)\b", content):
            if not public_flag_allowed(name):
                raise ValueError(f"public CLI leakage in {relative}: --{name}")
        for name in re.findall(r"\b(?:NAMRBD|SBS)_[A-Z0-9_]+\b", content):
            if not public_env_allowed(name):
                raise ValueError(f"public CLI leakage in {relative}: {name}")
        for fragment in forbidden_fragments:
            if fragment in content:
                raise ValueError(f"public CLI leakage in {relative}: {fragment}")


def generated_page_sets(env: dict[str, str]) -> dict[Path, dict[Path, str]]:
    with tempfile.TemporaryDirectory(prefix="namrbd-cli-reference-") as temporary:
        binary_paths = build_binaries(Path(temporary), env)
        public_pages: dict[Path, str] = {}
        internal_pages: dict[Path, str] = {}
        public_command_counts: dict[str, int] = {}
        internal_command_counts: dict[str, int] = {}
        for binary, metadata in BINARIES.items():
            files = package_files(binary, env)
            top_help = capture_help(
                binary_paths[binary],
                binary,
                list(metadata["top_args"]),
                int(metadata["top_exit"]),
                env,
            )
            commands = discover_commands(binary, files, top_help)
            internal_command_counts[binary] = len(commands)
            help_by_command: dict[str, str] = {}
            for command in commands:
                help_by_command[command] = capture_help(
                    binary_paths[binary], binary, command_help_args(binary, command), 0, env
                )

            registrations = source_flag_registrations(files)
            shown = visible_flags(top_help)
            hidden = {name: item for name, item in registrations.items() if name not in shown}
            # Command packages contain leaf FlagSets too; only daemon pages use
            # the top-level hidden comparison.
            if commands:
                hidden = {}

            env_inventory = environment_inventory(binary, files)
            internal_pages[Path(f"{binary}.md")] = render_binary_page(
                binary,
                metadata,
                "internal",
                top_help,
                commands,
                help_by_command,
                hidden,
                env_inventory,
            )
            if binary not in PUBLIC_BINARIES:
                continue
            public_commands = [
                command for command in commands
                if public_command_allowed(binary, command)
            ]
            public_command_counts[binary] = len(public_commands)
            public_help_by_command = {
                command: filter_help_flags(help_by_command[command])
                for command in public_commands
            }
            public_pages[Path(f"{binary}.md")] = render_binary_page(
                binary,
                metadata,
                "public",
                filter_help_flags(top_help),
                public_commands,
                public_help_by_command,
                {},
                filter_environment_inventory(env_inventory),
            )
        public_pages[Path("index.md")] = render_public_index(public_command_counts)
        internal_pages[Path("index.md")] = render_internal_index(internal_command_counts)
        validate_public_pages(public_pages)
        return {
            PUBLIC_OUTPUT_DIR: public_pages,
            INTERNAL_OUTPUT_DIR: internal_pages,
        }


def check_pages(output_dir: Path, pages: dict[Path, str]) -> bool:
    ok = True
    expected = {output_dir / relative for relative in pages}
    actual = set(output_dir.glob("*.md")) if output_dir.exists() else set()
    for extra in sorted(actual - expected):
        print(f"unexpected generated CLI reference page: {extra.relative_to(ROOT)}", file=sys.stderr)
        ok = False
    for relative, wanted in sorted(pages.items(), key=lambda item: str(item[0])):
        path = output_dir / relative
        current = path.read_text(encoding="utf-8") if path.exists() else ""
        if current == wanted:
            continue
        ok = False
        print(f"CLI reference drift: {path.relative_to(ROOT)}", file=sys.stderr)
        diff = difflib.unified_diff(
            current.splitlines(), wanted.splitlines(),
            fromfile=str(path.relative_to(ROOT)),
            tofile=f"generated/{relative}",
            lineterm="",
        )
        for line in list(diff)[:200]:
            print(line, file=sys.stderr)
    return ok


def write_pages(output_dir: Path, pages: dict[Path, str]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for relative, content in pages.items():
        (output_dir / relative).write_text(content, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--write", action="store_true", help="regenerate checked-in Markdown")
    mode.add_argument("--check", action="store_true", help="fail if checked-in Markdown differs")
    parser.add_argument(
        "--public-only",
        action="store_true",
        help="write/check only docs-src (used by the exported Community mirror)",
    )
    args = parser.parse_args()

    try:
        page_sets = generated_page_sets(go_env())
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError) as error:
        print(f"generate CLI reference: {error}", file=sys.stderr)
        return 1
    if args.public_only:
        page_sets = {PUBLIC_OUTPUT_DIR: page_sets[PUBLIC_OUTPUT_DIR]}
    if args.check:
        checks = [
            check_pages(output_dir, pages)
            for output_dir, pages in page_sets.items()
        ]
        return 0 if all(checks) else 1
    # namrbd-debug used to be generated into the public tree. It is now owned
    # exclusively by the canonical internal inventory.
    old_public_debug = PUBLIC_OUTPUT_DIR / "namrbd-debug.md"
    if old_public_debug.exists():
        old_public_debug.unlink()
    for output_dir, pages in page_sets.items():
        write_pages(output_dir, pages)
    return 0


if __name__ == "__main__":
    sys.exit(main())
