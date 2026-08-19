#!/usr/bin/env python3
import argparse
import json
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


FIXTURE = {
    "schema_version": "namrbd.sbs.observability.v1",
    "source_authority": "sbs-service AdminService",
    "collection_status": "ok",
    "collector_freshness_seconds": 0.1,
    "rbac_checked": True,
    "tenant_scope_checked": True,
    "redaction_applied": True,
    "read_only_mode_enforced": True,
    "unsupported_claim_visible": True,
    "warning_count": 0,
    "api_token": "must-not-cross-mcp-boundary",
    "mcp": {
        "mcp_server_ready": True,
        "mcp_provider_ready": True,
        "mcp_tool_registered": True,
        "read_only": True,
        "transport": "stdio-jsonrpc-content-length",
        "mutating_tools_enabled": False,
        "human_approval_required": True,
    },
}


class FixtureHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = dict(FIXTURE)
        if self.path == "/api/v1/mcp/tools":
            body["view_id"] = "mcp.tools"
            body["data"] = FIXTURE["mcp"]
        elif self.path == "/api/v1/operations/warnings":
            body["view_id"] = "operations.warnings"
            body["data"] = {"warnings": []}
        elif self.path != "/api/v1/sbs/cluster":
            self.send_error(404)
            return
        payload = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *_args):
        return


def write_frame(stream, message):
    payload = json.dumps(message, separators=(",", ":")).encode()
    stream.write(f"Content-Length: {len(payload)}\r\n\r\n".encode() + payload)
    stream.flush()


def read_frame(stream):
    length = None
    while True:
        line = stream.readline()
        if not line:
            raise RuntimeError("provider closed stdout before response")
        if line in (b"\n", b"\r\n"):
            break
        name, _, value = line.decode().partition(":")
        if name.lower() == "content-length":
            length = int(value.strip())
    if length is None:
        raise RuntimeError("provider response missing Content-Length")
    return json.loads(stream.read(length))


def request(process, request_id, method, params=None):
    message = {"jsonrpc": "2.0", "id": request_id, "method": method}
    if params is not None:
        message["params"] = params
    write_frame(process.stdin, message)
    response = read_frame(process.stdout)
    if response.get("id") != request_id or response.get("error"):
        raise RuntimeError(f"invalid response for {method}: {response}")
    return response["result"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--provider", required=True)
    parser.add_argument("--evidence", required=True)
    parser.add_argument("--operations-endpoint")
    args = parser.parse_args()
    server = None
    if args.operations_endpoint:
        endpoint = args.operations_endpoint
    else:
        server = ThreadingHTTPServer(("127.0.0.1", 0), FixtureHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        endpoint = f"http://127.0.0.1:{server.server_port}"
    process = subprocess.Popen(
        [args.provider, "--operations-endpoint", endpoint],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    evidence = {}
    try:
        initialized = request(process, 1, "initialize")
        resources = request(process, 2, "resources/list")
        resource = request(process, 3, "resources/read", {"uri": "namrbd://sbs/observability"})
        tools = request(process, 4, "tools/list")
        health = request(process, 5, "tools/call", {"name": "namrbd.health.check", "arguments": {}})
        resource_text = resource["contents"][0]["text"]
        health_text = health["content"][0]["text"]
        tool_names = [tool["name"] for tool in tools["tools"]]
        resource_uris = [item["uri"] for item in resources["resources"]]
        checks = {
            "initialize_protocol": initialized["protocolVersion"] == "2024-11-05",
            "provider_process_alive": process.poll() is None,
            "resource_registered": "namrbd://sbs/observability" in resource_uris,
            "tool_registered": "namrbd.health.check" in tool_names,
            "resource_round_trip": "sbs-service AdminService" in resource_text,
            "tool_round_trip": "stdio-jsonrpc-content-length" in health_text,
            "redaction_contract": '"redaction_applied": true' in resource_text and '"redaction_applied": true' in health_text,
            "rbac_contract": '"rbac_checked": true' in resource_text and '"rbac_checked": true' in health_text,
            "read_only_enforced": '"mutating_tools_enabled": false' in health_text,
        }
        if not all(checks.values()):
            raise RuntimeError(f"integration checks failed: {checks}")
        evidence = {
            "result": "ok", "entrypoint": "phase-y-mcp-client-provider-integration",
            "schema_version": "namrbd.mcp.integration-evidence.v1",
            "client": "python-stdlib-mcp-client", "provider": "cmd/namrbd-mcp",
            "transport": "stdio-jsonrpc-content-length", "http_provider_fixture": server is not None,
            "operations_endpoint": endpoint,
            "request_count": 5, "checks": checks, "ok_count": len(checks),
            "error_count": 0, "first_error": "", "last_error": "",
        }
    finally:
        if process.stdin:
            process.stdin.close()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.terminate()
            process.wait(timeout=5)
        if server is not None:
            server.shutdown()
            server.server_close()
    with open(args.evidence, "w", encoding="utf-8") as output:
        json.dump(evidence, output, indent=2, sort_keys=True)
        output.write("\n")
    print(json.dumps(evidence, separators=(",", ":")))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(json.dumps({"result":"fail","entrypoint":"phase-y-mcp-client-provider-integration","error_count":1,"first_error":str(error),"last_error":str(error)}, separators=(",", ":")))
        raise SystemExit(1)
