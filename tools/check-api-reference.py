#!/usr/bin/env python3
"""Validate the reviewed public NAMRBD OpenAPI references.

The four specifications are intentionally checked against a small, explicit
manifest.  Discovering every net/http registration mechanically would include
lab/debug and HTML surfaces that are not part of the reviewed public API.  The
manifest therefore records the public boundary, while source markers make a
stale manifest fail when the corresponding implementation moves or disappears.

Only the Python standard library is used so this check can run in a clean
repository checkout without installing an OpenAPI validator.
"""

from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


REPO_ROOT = Path(__file__).resolve().parents[1]
REST_REFERENCE_DIR = REPO_ROOT / "docs-src" / "reference" / "api" / "rest"

HTTP_METHODS = frozenset(
    {"get", "put", "post", "delete", "options", "head", "patch", "trace"}
)
PATH_ITEM_FIELDS = frozenset({"$ref", "summary", "description", "servers", "parameters"})
REQUIRED_OPERATION_EXTENSIONS = (
    "x-namrbd-stability",
    "x-namrbd-edition",
    "x-namrbd-authority",
    "x-namrbd-feature-gate",
    "x-namrbd-source",
)
PATH_PARAMETER_RE = re.compile(r"\{([^{}]+)\}")
OPENAPI_31_RE = re.compile(r"^3\.1\.\d+$")
RESPONSE_KEY_RE = re.compile(r"^(?:default|[1-5](?:[0-9]{2}|XX))$")
CAMEL_BOUNDARY_RE = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")
MISSING_REFERENCE = object()


@dataclass(frozen=True)
class PublicOperation:
    method: str
    path: str
    source: str
    source_markers: tuple[str, ...]

    @property
    def key(self) -> tuple[str, str]:
        return (self.method.lower(), self.path)


def operation(
    method: str,
    path: str,
    source: str,
    *source_markers: str,
) -> PublicOperation:
    return PublicOperation(method.lower(), path, source, tuple(source_markers))


GATEWAY_SOURCE = "gateway/httpapi/server.go"
SBS_SERVICE_SOURCE = "cmd/sbs-service/main.go"
SBS_SERVICE_PHASE_Y_SOURCE = "cmd/sbs-service/main_phase_y_observability.go"
SBS_DATA_SOURCE = "cmd/sbs-data/main.go"
ISCSI_SOURCE = "cmd/namrbd-iscsi-gateway/main.go"


# This is the reviewed Community REST surface.  A route registered in source is
# not automatically public: debug, lab mutation, HTML console, and unsupported
# feature-claim endpoints are deliberately absent.
EXPECTED_SPECS: dict[str, tuple[PublicOperation, ...]] = {
    "namrbd-gateway-v1.openapi.json": (
        operation("get", "/healthz", GATEWAY_SOURCE, 'mux.HandleFunc("/healthz"'),
        operation("get", "/readyz", GATEWAY_SOURCE, 'mux.HandleFunc("/readyz"'),
        operation("get", "/metrics", GATEWAY_SOURCE, 'mux.HandleFunc("/metrics"'),
        operation(
            "get",
            "/api/v1/discovery/gateways",
            GATEWAY_SOURCE,
            'mux.HandleFunc("/api/v1/discovery/gateways"',
        ),
        operation(
            "get",
            "/api/v1/discovery/volumes/{volume_id}",
            GATEWAY_SOURCE,
            'mux.HandleFunc("/api/v1/discovery/volumes/"',
            "func (s *Server) handleDiscoveryVolumeRoutes",
        ),
        operation(
            "get",
            "/api/v1/volumes/{volume_id}/info",
            GATEWAY_SOURCE,
            'mux.HandleFunc("/api/v1/volumes/"',
            'case "info":',
        ),
        operation(
            "post",
            "/api/v1/volumes/{volume_id}/attach",
            GATEWAY_SOURCE,
            'mux.HandleFunc("/api/v1/volumes/"',
            'case "attach":',
        ),
        operation(
            "post",
            "/api/v1/volumes/{volume_id}/reload-size",
            GATEWAY_SOURCE,
            'mux.HandleFunc("/api/v1/volumes/"',
            'case "reload-size":',
        ),
        operation(
            "post",
            "/api/v1/volumes/{volume_id}/detach",
            GATEWAY_SOURCE,
            'mux.HandleFunc("/api/v1/volumes/"',
            'case "detach":',
        ),
        operation(
            "post",
            "/api/v1/volumes/{volume_id}/read",
            GATEWAY_SOURCE,
            'mux.HandleFunc("/api/v1/volumes/"',
            'case "read":',
        ),
        operation(
            "post",
            "/api/v1/volumes/{volume_id}/write",
            GATEWAY_SOURCE,
            'mux.HandleFunc("/api/v1/volumes/"',
            'case "write":',
        ),
        operation(
            "post",
            "/api/v1/volumes/{volume_id}/flush",
            GATEWAY_SOURCE,
            'mux.HandleFunc("/api/v1/volumes/"',
            'case "flush":',
        ),
        operation(
            "post",
            "/api/v1/volumes/{volume_id}/discard",
            GATEWAY_SOURCE,
            'mux.HandleFunc("/api/v1/volumes/"',
            'case "discard":',
        ),
        operation(
            "post",
            "/api/v1/volumes/{volume_id}/zero",
            GATEWAY_SOURCE,
            'mux.HandleFunc("/api/v1/volumes/"',
            'case "zero":',
        ),
    ),
    "sbs-service-observability-v1.openapi.json": (
        operation("get", "/healthz", SBS_SERVICE_SOURCE, 'mux.HandleFunc("/healthz"'),
        operation("get", "/readyz", SBS_SERVICE_SOURCE, 'mux.HandleFunc("/readyz"'),
        operation("get", "/dependency", SBS_SERVICE_SOURCE, 'mux.Handle("/dependency"'),
        operation("get", "/metrics", SBS_SERVICE_SOURCE, 'mux.HandleFunc("/metrics"'),
        operation(
            "get",
            "/api/v1/sbs/cluster",
            SBS_SERVICE_PHASE_Y_SOURCE,
            'mux.HandleFunc("/api/v1/sbs/cluster"',
        ),
        operation(
            "get",
            "/api/v1/sbs/nodes",
            SBS_SERVICE_PHASE_Y_SOURCE,
            'mux.HandleFunc("/api/v1/sbs/nodes"',
        ),
        operation(
            "get",
            "/api/v1/sbs/volumes",
            SBS_SERVICE_PHASE_Y_SOURCE,
            'mux.HandleFunc("/api/v1/sbs/volumes"',
        ),
        operation(
            "get",
            "/api/v1/sbs/maintenance",
            SBS_SERVICE_PHASE_Y_SOURCE,
            'mux.HandleFunc("/api/v1/sbs/maintenance"',
        ),
        operation(
            "get",
            "/api/v1/sbs/capacity",
            SBS_SERVICE_PHASE_Y_SOURCE,
            'mux.HandleFunc("/api/v1/sbs/capacity"',
        ),
        operation(
            "get",
            "/api/v1/sbs/reclaim",
            SBS_SERVICE_PHASE_Y_SOURCE,
            'mux.HandleFunc("/api/v1/sbs/reclaim"',
        ),
        operation(
            "get",
            "/api/v1/membership/status",
            SBS_SERVICE_PHASE_Y_SOURCE,
            'mux.HandleFunc("/api/v1/membership/status"',
        ),
        operation(
            "get",
            "/api/v1/operations/summary",
            SBS_SERVICE_PHASE_Y_SOURCE,
            'mux.HandleFunc("/api/v1/operations/summary"',
        ),
        operation(
            "get",
            "/api/v1/operations/warnings",
            SBS_SERVICE_PHASE_Y_SOURCE,
            'mux.HandleFunc("/api/v1/operations/warnings"',
        ),
        operation(
            "get",
            "/api/v1/query/views",
            SBS_SERVICE_PHASE_Y_SOURCE,
            'mux.HandleFunc("/api/v1/query/views"',
        ),
        operation(
            "get",
            "/api/v1/mcp/tools",
            SBS_SERVICE_PHASE_Y_SOURCE,
            'mux.HandleFunc("/api/v1/mcp/tools"',
        ),
        operation(
            "get",
            "/api/v1/gui/summary",
            SBS_SERVICE_PHASE_Y_SOURCE,
            'mux.HandleFunc("/api/v1/gui/summary"',
        ),
        operation(
            "get",
            "/api/v1/workflow/hardening",
            SBS_SERVICE_PHASE_Y_SOURCE,
            'mux.HandleFunc("/api/v1/workflow/hardening"',
        ),
    ),
    "sbs-data-operational-v1.openapi.json": (
        operation("get", "/healthz", SBS_DATA_SOURCE, 'mux.HandleFunc("/healthz"'),
        operation("get", "/readyz", SBS_DATA_SOURCE, 'mux.Handle("/readyz"'),
        operation("get", "/metrics", SBS_DATA_SOURCE, 'mux.HandleFunc("/metrics"'),
        # These two historical /debug paths are read-only Community operational
        # surfaces.  No other sbs-data debug or admin route is public.
        operation(
            "get",
            "/debug/summary",
            SBS_DATA_SOURCE,
            'mux.HandleFunc("/debug/summary"',
        ),
        operation(
            "get",
            "/debug/store-health",
            SBS_DATA_SOURCE,
            'mux.HandleFunc("/debug/store-health"',
        ),
    ),
    "namrbd-iscsi-gateway-observability-v1.openapi.json": (
        operation("get", "/healthz", ISCSI_SOURCE, 'mux.HandleFunc("/healthz"'),
        operation("get", "/readyz", ISCSI_SOURCE, 'mux.Handle("/readyz"'),
        operation("get", "/metrics", ISCSI_SOURCE, 'mux.HandleFunc("/metrics"'),
    ),
}

EXPECTED_OPERATION_COUNTS = {
    "namrbd-gateway-v1.openapi.json": 14,
    "sbs-service-observability-v1.openapi.json": 17,
    "sbs-data-operational-v1.openapi.json": 5,
    "namrbd-iscsi-gateway-observability-v1.openapi.json": 3,
}
EXPECTED_OPERATION_COUNT = 39
ALLOWED_SBS_DATA_DEBUG_PATHS = frozenset(
    {"/debug/summary", "/debug/store-health"}
)


class DuplicateJSONKey(ValueError):
    pass


def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise DuplicateJSONKey(f"duplicate JSON object key {key!r}")
        result[key] = value
    return result


def reject_nonfinite_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON number {value!r}")


def lexical_tokens(value: str) -> list[str]:
    """Split prose, kebab/snake identifiers, and camel-case identifiers."""

    expanded = CAMEL_BOUNDARY_RE.sub(" ", value)
    return [token.lower() for token in re.findall(r"[A-Za-z0-9]+", expanded)]


def json_strings(value: Any) -> Iterable[str]:
    pending = [value]
    while pending:
        current = pending.pop()
        if isinstance(current, dict):
            for key, child in current.items():
                yield str(key)
                pending.append(child)
        elif isinstance(current, list):
            pending.extend(current)
        elif isinstance(current, str):
            yield current


class Checker:
    def __init__(self) -> None:
        self.errors: list[str] = []
        self._source_cache: dict[str, str] = {}
        self._operation_ids: dict[str, str] = {}

    def error(self, location: str, message: str) -> None:
        self.errors.append(f"{location}: {message}")

    def source_text(self, relative_path: str) -> str | None:
        if relative_path in self._source_cache:
            return self._source_cache[relative_path]
        candidate = (REPO_ROOT / relative_path).resolve()
        try:
            candidate.relative_to(REPO_ROOT)
        except ValueError:
            self.error(relative_path, "source path escapes the repository")
            return None
        if not candidate.is_file():
            self.error(relative_path, "source file does not exist")
            return None
        try:
            text = candidate.read_text(encoding="utf-8")
        except (OSError, UnicodeError) as exc:
            self.error(relative_path, f"cannot read source file: {exc}")
            return None
        self._source_cache[relative_path] = text
        return text

    def resolve_local_reference(self, document: Any, reference: str) -> Any:
        if reference == "#":
            return document
        if not reference.startswith("#/"):
            return MISSING_REFERENCE
        current = document
        for encoded_token in reference[2:].split("/"):
            token = encoded_token.replace("~1", "/").replace("~0", "~")
            if isinstance(current, dict):
                if token not in current:
                    return MISSING_REFERENCE
                current = current[token]
            elif isinstance(current, list) and token.isdigit():
                index = int(token)
                if index >= len(current):
                    return MISSING_REFERENCE
                current = current[index]
            else:
                return MISSING_REFERENCE
        return current

    def check_local_references(self, location: str, document: Any) -> None:
        """Require every reference to resolve inside the standalone document."""

        stack: list[tuple[str, Any]] = [("$", document)]
        while stack:
            pointer, value = stack.pop()
            if isinstance(value, dict):
                reference = value.get("$ref")
                if reference is not None:
                    ref_location = f"{location}:{pointer}.$ref"
                    if not isinstance(reference, str) or not reference.strip():
                        self.error(ref_location, "$ref must be a non-empty string")
                    elif not reference.startswith("#/") and reference != "#":
                        self.error(ref_location, "external $ref is not allowed in a standalone spec")
                    else:
                        resolved = self.resolve_local_reference(document, reference)
                        if resolved is MISSING_REFERENCE:
                            self.error(ref_location, f"local reference {reference!r} does not resolve")
                for key, child in value.items():
                    stack.append((f"{pointer}.{key}", child))
            elif isinstance(value, list):
                for index, child in enumerate(value):
                    stack.append((f"{pointer}[{index}]", child))

    def check_manifest(self) -> None:
        manifest_names = set(EXPECTED_SPECS)
        expected_names = set(EXPECTED_OPERATION_COUNTS)
        if manifest_names != expected_names:
            self.error(
                "checker manifest",
                "spec names differ from the four reviewed OpenAPI filenames",
            )

        operation_count = sum(len(operations) for operations in EXPECTED_SPECS.values())
        if operation_count != EXPECTED_OPERATION_COUNT:
            self.error(
                "checker manifest",
                f"expected {EXPECTED_OPERATION_COUNT} operations, found {operation_count}",
            )

        for spec_name, operations in EXPECTED_SPECS.items():
            expected_count = EXPECTED_OPERATION_COUNTS.get(spec_name)
            if expected_count is not None and len(operations) != expected_count:
                self.error(
                    f"checker manifest:{spec_name}",
                    f"expected {expected_count} operations, found {len(operations)}",
                )
            seen: set[tuple[str, str]] = set()
            for index, item in enumerate(operations):
                location = f"checker manifest:{spec_name}[{index}]"
                if item.method not in HTTP_METHODS:
                    self.error(location, f"unsupported HTTP method {item.method!r}")
                if not item.path.startswith("/"):
                    self.error(location, "path must start with '/'")
                if item.key in seen:
                    self.error(location, f"duplicate operation {item.method.upper()} {item.path}")
                seen.add(item.key)
                if not item.source or Path(item.source).is_absolute():
                    self.error(location, "source must be a repository-relative path")
                if not item.source_markers:
                    self.error(location, "at least one implementation marker is required")
                for marker in item.source_markers:
                    if not marker.strip() or "\n" in marker or "\r" in marker:
                        self.error(
                            location,
                            "implementation markers must be non-empty single lines",
                        )

    def check_spec_file_set(self) -> None:
        location = REST_REFERENCE_DIR.relative_to(REPO_ROOT).as_posix()
        if not REST_REFERENCE_DIR.is_dir():
            self.error(location, "OpenAPI reference directory is missing")
            return
        try:
            actual_names = {
                path.name for path in REST_REFERENCE_DIR.glob("*.openapi.json") if path.is_file()
            }
        except OSError as exc:
            self.error(location, f"cannot enumerate OpenAPI documents: {exc}")
            return
        expected_names = set(EXPECTED_OPERATION_COUNTS)
        for name in sorted(actual_names - expected_names):
            self.error(location, f"unexpected OpenAPI document {name!r}")

    def check_manifest_sources(self) -> None:
        seen: set[tuple[str, str]] = set()
        for spec_name, operations in EXPECTED_SPECS.items():
            for expected in operations:
                source = self.source_text(expected.source)
                if source is None:
                    continue
                for marker in expected.source_markers:
                    marker_key = (expected.source, marker)
                    if marker_key in seen:
                        continue
                    seen.add(marker_key)
                    if marker not in source:
                        self.error(
                            f"{spec_name}:{expected.method.upper()} {expected.path}",
                            f"implementation marker {marker!r} is absent from {expected.source}",
                        )

    def load_json(self, path: Path) -> Any | None:
        if not path.is_file():
            self.error(path.relative_to(REPO_ROOT).as_posix(), "OpenAPI document is missing")
            return None
        try:
            with path.open("r", encoding="utf-8") as handle:
                return json.load(
                    handle,
                    object_pairs_hook=reject_duplicate_keys,
                    parse_constant=reject_nonfinite_constant,
                )
        except (OSError, UnicodeError, ValueError, RecursionError) as exc:
            self.error(path.relative_to(REPO_ROOT).as_posix(), f"invalid JSON: {exc}")
            return None

    def check_document(self, spec_name: str, document: Any) -> None:
        location = f"docs-src/reference/api/rest/{spec_name}"
        if not isinstance(document, dict):
            self.error(location, "document root must be a JSON object")
            return

        self.check_local_references(location, document)

        openapi = document.get("openapi")
        if not isinstance(openapi, str) or not OPENAPI_31_RE.fullmatch(openapi):
            self.error(location, "openapi must be an OpenAPI 3.1 patch version such as '3.1.0'")

        info = document.get("info")
        if not isinstance(info, dict):
            self.error(location, "info must be an object")
        else:
            for field in ("title", "version"):
                if not isinstance(info.get(field), str) or not info[field].strip():
                    self.error(location, f"info.{field} must be a non-empty string")

        if "webhooks" in document:
            self.error(location, "webhooks are outside the reviewed public REST surface")

        paths = document.get("paths")
        if not isinstance(paths, dict) or not paths:
            self.error(location, "paths must be a non-empty object")
            return

        expected_by_key = {item.key: item for item in EXPECTED_SPECS[spec_name]}
        if len(expected_by_key) != len(EXPECTED_SPECS[spec_name]):
            self.error(spec_name, "checker manifest contains duplicate route/method entries")
            return

        actual_by_key: dict[tuple[str, str], dict[str, Any]] = {}
        for route_path, path_item in paths.items():
            path_location = f"{location}:paths[{route_path!r}]"
            if not isinstance(route_path, str) or not route_path.startswith("/"):
                self.error(path_location, "path key must start with '/'")
                continue
            if not isinstance(path_item, dict):
                self.error(path_location, "path item must be an object")
                continue
            for field in path_item:
                lowered = field.lower()
                if lowered in HTTP_METHODS and field != lowered:
                    self.error(path_location, f"HTTP method key {field!r} must be lowercase")
                if (
                    lowered not in HTTP_METHODS
                    and field not in PATH_ITEM_FIELDS
                    and not field.startswith("x-")
                ):
                    self.error(path_location, f"unknown path-item field {field!r}")
            path_parameters = path_item.get("parameters", [])
            operation_count = sum(method in path_item for method in HTTP_METHODS)
            if operation_count == 0:
                self.error(path_location, "path item must define at least one operation")
            for method in sorted(HTTP_METHODS):
                if method not in path_item:
                    continue
                operation_data = path_item[method]
                op_location = f"{location}:{method.upper()} {route_path}"
                if not isinstance(operation_data, dict):
                    self.error(op_location, "operation must be an object")
                    continue
                key = (method, route_path)
                if key in actual_by_key:
                    self.error(op_location, "duplicate route/method operation")
                    continue
                actual_by_key[key] = operation_data
                expected = expected_by_key.get(key)
                self.check_operation(
                    document,
                    spec_name,
                    op_location,
                    route_path,
                    method,
                    operation_data,
                    path_parameters,
                    expected,
                )

        actual_keys = set(actual_by_key)
        expected_keys = set(expected_by_key)
        for method, route_path in sorted(
            expected_keys - actual_keys, key=lambda item: (item[1], item[0])
        ):
            self.error(location, f"missing public operation {method.upper()} {route_path}")
        for method, route_path in sorted(
            actual_keys - expected_keys, key=lambda item: (item[1], item[0])
        ):
            self.error(location, f"unexpected/non-public operation {method.upper()} {route_path}")

        self.check_forbidden_surface(spec_name, document)

    def check_operation(
        self,
        document: dict[str, Any],
        spec_name: str,
        location: str,
        route_path: str,
        method: str,
        operation_data: dict[str, Any],
        path_parameters: Any,
        expected: PublicOperation | None,
    ) -> None:
        operation_id = operation_data.get("operationId")
        if not isinstance(operation_id, str) or not operation_id.strip():
            self.error(location, "operationId must be a non-empty string")
        else:
            previous = self._operation_ids.get(operation_id)
            if previous is not None:
                self.error(location, f"operationId {operation_id!r} is already used by {previous}")
            else:
                self._operation_ids[operation_id] = location

        responses = operation_data.get("responses")
        if not isinstance(responses, dict) or not responses:
            self.error(location, "responses must be a non-empty object")
        else:
            self.check_responses(location, responses)

        if "callbacks" in operation_data:
            self.error(location, "callbacks are outside the reviewed public REST surface")

        for extension in REQUIRED_OPERATION_EXTENSIONS:
            value = operation_data.get(extension)
            if not isinstance(value, str) or not value.strip():
                self.error(location, f"{extension} must be present as a non-empty string")

        self.check_path_parameters(
            document,
            location,
            route_path,
            path_parameters,
            operation_data.get("parameters", []),
        )

        if expected is not None:
            self.check_operation_source(location, operation_data, expected)

    def check_responses(self, location: str, responses: dict[str, Any]) -> None:
        response_count = 0
        for key, response in responses.items():
            response_location = f"{location}:responses[{key!r}]"
            if key.startswith("x-"):
                continue
            response_count += 1
            if not RESPONSE_KEY_RE.fullmatch(key):
                self.error(response_location, "invalid HTTP response status key")
            if not isinstance(response, dict):
                self.error(response_location, "response must be an object")
                continue
            if "$ref" in response:
                reference = response.get("$ref")
                if not isinstance(reference, str) or not reference.strip():
                    self.error(response_location, "$ref must be a non-empty string")
            else:
                description = response.get("description")
                if not isinstance(description, str) or not description.strip():
                    self.error(
                        response_location,
                        "response.description must be a non-empty string",
                    )
        if response_count == 0:
            self.error(location, "responses must include at least one status or default entry")

    def resolve_parameter(
        self, document: dict[str, Any], location: str, value: Any
    ) -> dict[str, Any] | None:
        if not isinstance(value, dict):
            self.error(location, "parameter entry must be an object")
            return None
        reference = value.get("$ref")
        if reference is None:
            return value
        if not isinstance(reference, str) or not reference.startswith("#/components/parameters/"):
            self.error(location, f"unsupported parameter reference {reference!r}")
            return None
        name = (
            reference.removeprefix("#/components/parameters/")
            .replace("~1", "/")
            .replace("~0", "~")
        )
        components = document.get("components", {})
        parameters = components.get("parameters", {}) if isinstance(components, dict) else {}
        resolved = parameters.get(name) if isinstance(parameters, dict) else None
        if not isinstance(resolved, dict):
            self.error(location, f"parameter reference {reference!r} does not resolve")
            return None
        return resolved

    def parameter_map(
        self,
        document: dict[str, Any],
        location: str,
        values: Any,
    ) -> dict[tuple[str, str], dict[str, Any]]:
        if values is None:
            return {}
        if not isinstance(values, list):
            self.error(location, "parameters must be an array")
            return {}
        result: dict[tuple[str, str], dict[str, Any]] = {}
        for index, value in enumerate(values):
            parameter = self.resolve_parameter(document, f"{location}[{index}]", value)
            if parameter is None:
                continue
            name = parameter.get("name")
            where = parameter.get("in")
            if not isinstance(name, str) or not name:
                self.error(f"{location}[{index}]", "parameter.name must be a non-empty string")
                continue
            if not isinstance(where, str) or not where:
                self.error(f"{location}[{index}]", "parameter.in must be a non-empty string")
                continue
            key = (where, name)
            if key in result:
                self.error(f"{location}[{index}]", f"duplicate {where} parameter {name!r}")
                continue
            result[key] = parameter
        return result

    def check_path_parameters(
        self,
        document: dict[str, Any],
        location: str,
        route_path: str,
        path_values: Any,
        operation_values: Any,
    ) -> None:
        placeholders = PATH_PARAMETER_RE.findall(route_path)
        if len(placeholders) != len(set(placeholders)):
            self.error(location, "path template repeats a parameter name")
        declared = self.parameter_map(document, f"{location}:path parameters", path_values)
        # Operation-level parameters override path-level definitions in OpenAPI.
        declared.update(
            self.parameter_map(document, f"{location}:operation parameters", operation_values)
        )
        declared_path = {name for (where, name) in declared if where == "path"}
        expected_path = set(placeholders)
        for name in sorted(expected_path - declared_path):
            self.error(location, f"path parameter {name!r} is not declared")
        for name in sorted(declared_path - expected_path):
            self.error(
                location,
                f"declared path parameter {name!r} is absent from the path template",
            )
        for name in sorted(expected_path & declared_path):
            parameter = declared[("path", name)]
            if parameter.get("required") is not True:
                self.error(location, f"path parameter {name!r} must set required: true")
            if not isinstance(parameter.get("schema"), dict) and not isinstance(
                parameter.get("content"), dict
            ):
                self.error(location, f"path parameter {name!r} must define schema or content")

    def check_operation_source(
        self,
        location: str,
        operation_data: dict[str, Any],
        expected: PublicOperation,
    ) -> None:
        value = operation_data.get("x-namrbd-source")
        if not isinstance(value, str):
            return
        prefix = expected.source + ":"
        if not value.startswith(prefix):
            self.error(location, f"x-namrbd-source must start with {prefix!r}")
            return
        marker = value[len(prefix) :]
        if not marker.strip() or "\n" in marker or "\r" in marker:
            self.error(location, "x-namrbd-source must contain one non-empty literal marker")
            return
        if marker not in expected.source_markers:
            self.error(
                location,
                "x-namrbd-source marker is not the reviewed manifest marker for this operation",
            )
            return
        source = self.source_text(expected.source)
        if source is not None and marker not in source:
            self.error(
                location,
                f"x-namrbd-source marker {marker!r} is absent from {expected.source}",
            )

    def check_forbidden_surface(self, spec_name: str, document: dict[str, Any]) -> None:
        location = f"docs-src/reference/api/rest/{spec_name}"
        paths = document.get("paths", {})
        if isinstance(paths, dict):
            for route_path in paths:
                lowered = str(route_path).lower()
                forbidden_reason = ""
                if spec_name == "namrbd-gateway-v1.openapi.json" and lowered.startswith(
                    "/api/v1/debug/"
                ):
                    forbidden_reason = "gateway /api/v1/debug routes are not public"
                elif (
                    spec_name == "sbs-service-observability-v1.openapi.json"
                    and lowered.startswith("/debug/")
                ):
                    forbidden_reason = "sbs-service /debug routes are not public"
                elif spec_name == "sbs-data-operational-v1.openapi.json":
                    if (
                        lowered.startswith("/debug/")
                        and lowered not in ALLOWED_SBS_DATA_DEBUG_PATHS
                    ):
                        forbidden_reason = "only /debug/summary and /debug/store-health are public"
                    elif lowered.startswith("/admin/"):
                        forbidden_reason = "sbs-data admin mutation routes are not public"
                elif (
                    spec_name == "namrbd-iscsi-gateway-observability-v1.openapi.json"
                    and lowered == "/debug/registry"
                ):
                    forbidden_reason = "the iSCSI registry debug route is not public"
                if forbidden_reason:
                    self.error(f"{location}:paths[{route_path!r}]", forbidden_reason)

        strings = list(json_strings(document))
        token_lists = [lexical_tokens(value) for value in strings]
        forbidden_terms = (
            ({"clone", "clones", "cloned", "cloning"}, "clone"),
            ({"ec"}, "EC"),
            ({"ha"}, "HA"),
            ({"alua"}, "ALUA"),
        )
        for forbidden_tokens, label in forbidden_terms:
            if any(forbidden_tokens.intersection(tokens) for tokens in token_lists):
                self.error(location, f"forbidden unsupported public wording {label!r} is present")
        if any(
            any(
                left == "high" and right == "availability"
                for left, right in zip(tokens, tokens[1:])
            )
            for tokens in token_lists
        ):
            self.error(
                location,
                "forbidden unsupported public wording 'high availability' is present",
            )

        # Debug wording is forbidden for specifications that expose no reviewed
        # debug route.  sbs-data is the explicit exception for its two read-only
        # operational endpoints; exact route matching above prevents expansion.
        if spec_name != "sbs-data-operational-v1.openapi.json":
            if any("debug" in tokens for tokens in token_lists):
                self.error(location, "forbidden debug wording is present")
        else:
            route_pattern = r"/(?:debug|admin)/[A-Za-z0-9._~!$&'()*+,;=:@%/-]*"
            for item in strings:
                for match in re.finditer(route_pattern, item):
                    value = match.group(0)
                    if value not in ALLOWED_SBS_DATA_DEBUG_PATHS:
                        self.error(
                            location,
                            f"forbidden sbs-data operational route wording {value!r} is present",
                        )

    def run(self) -> bool:
        self.check_manifest()
        self.check_spec_file_set()
        self.check_manifest_sources()
        for spec_name in EXPECTED_SPECS:
            document = self.load_json(REST_REFERENCE_DIR / spec_name)
            if document is not None:
                self.check_document(spec_name, document)
        if self.errors:
            for message in sorted(set(self.errors)):
                print(f"api-reference-check: {message}", file=sys.stderr)
            print(
                f"api-reference-check: failed with {len(set(self.errors))} error(s)",
                file=sys.stderr,
            )
            return False
        print(
            "api-reference-check: ok "
            f"({len(EXPECTED_SPECS)} specs, {EXPECTED_OPERATION_COUNT} operations)",
            file=sys.stderr,
        )
        return True


def main(argv: Iterable[str]) -> int:
    arguments = list(argv)
    if arguments != ["--check"]:
        print("usage: tools/check-api-reference.py --check", file=sys.stderr)
        return 1
    return 0 if Checker().run() else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
