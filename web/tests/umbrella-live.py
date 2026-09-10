#!/usr/bin/env python3
"""Bounded real-API dashboard validation, restricted to the authorized test repo.
Requires DONKEYSPACE_DASHBOARD_LIVE_TEST=1, gh auth, built API, Docker and Chromium.
Creates a fresh issue and isolated database/network/web; never starts a worker.
"""
import hashlib
import hmac
import json
import os
from pathlib import Path
import select
import subprocess
import time
import urllib.error
import urllib.request
import uuid

SOURCE = Path(__file__).resolve().parents[2]
REPO = "EPIC-BLOCKCHAIN/umbrella"
RUN = uuid.uuid4().hex[:12]
ROOT = Path("/tmp") / f"donkeyspace-dashboard-live-{RUN}"


def command(*args, **kwargs):
    return subprocess.check_output(args, text=True, timeout=240, **kwargs).strip()


def github(path, method="GET", body=None):
    args = ["gh", "api", path, "--method", method]
    if body is not None:
        args += ["--input", "-"]
    return json.loads(command(*args, input=json.dumps(body) if body is not None else None))


def wait(predicate, description):
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.25)
    raise AssertionError(f"Timed out: {description}; logs: {ROOT}")


def main():
    assert os.environ.get("DONKEYSPACE_DASHBOARD_LIVE_TEST") == "1"
    ROOT.mkdir()
    name = f"ds-dashboard-live-{RUN}"
    image = f"{name}:web"
    db = f"{name}-db"
    web = f"{name}-web"
    containers, processes = [], []
    network = False
    issue = None
    evidence = {"source": command("git", "-C", str(SOURCE), "rev-parse", "HEAD"),
                "source_dirty": bool(command("git", "-C", str(SOURCE), "status", "--porcelain")),
                "run": RUN, "result": "failed", "repository": REPO}
    try:
        repository = github(f"repos/{REPO}")
        evidence["umbrella_revision"] = github(f"repos/{REPO}/commits/{repository['default_branch']}")["sha"]
        with (ROOT / "web-build.log").open("w") as log:
            subprocess.run(["docker", "build", "-t", image, str(SOURCE / "web")], stdout=log, stderr=subprocess.STDOUT, timeout=240, check=True)
        command("docker", "network", "create", name)
        network = True
        gateway = json.loads(command("docker", "network", "inspect", name))[0]["IPAM"]["Config"][0]["Gateway"]
        containers.append(db)
        command("docker", "run", "-d", "--name", db, "--network", name,
                "-p", "127.0.0.1::5432", "-e", "POSTGRES_PASSWORD=test-only",
                "-e", "POSTGRES_DB=donkeyspace_dashboard_test", "postgres:17")
        port = command("docker", "port", db, "5432/tcp").split(":")[-1]
        def database_ready():
            return subprocess.run(["docker", "exec", db, "pg_isready", "-U", "postgres"], capture_output=True).returncode == 0
        wait(database_ready, "isolated database")
        policy = ROOT / "policy.yml"
        # This checked-in policy is generic, has deny-by-default engagement,
        # and the test issue has no allow label. No worker is started.
        policy.write_text((SOURCE / ".donkeyspace/policy.yml").read_text() + '\nfacade:\n  display_name: "Umbrella validation"\n  tagline: "Isolated dashboard test"\n')
        secret = uuid.uuid4().hex
        env = {key: value for key, value in os.environ.items() if not key.startswith("DONKEYSPACE_")}
        env.update({"DONKEYSPACE_DEPLOYMENT_MODE": "minimal", "DONKEYSPACE_DATABASE_URL": f"postgres://postgres:test-only@127.0.0.1:{port}/donkeyspace_dashboard_test",
                    "DONKEYSPACE_POLICY_PATH": str(policy), "DONKEYSPACE_GITHUB_AUTH_MODE": "pat",
                    "DONKEYSPACE_GITHUB_TOKEN": command("gh", "auth", "token"), "DONKEYSPACE_GITHUB_REPOSITORIES": REPO,
                    "DONKEYSPACE_GITHUB_INGRESS_MODE": "webhook", "DONKEYSPACE_WEBHOOK_SECRET": secret,
                    "DONKEYSPACE_BIND_ADDR": f"{gateway}:8080", "RUST_LOG": "donkeyspace=info"})
        def start_api():
            with (ROOT / f"api-{len(processes)}.log").open("w") as log:
                process = subprocess.Popen([str(SOURCE / "target/debug/donkeyspace-api")], env=env, stdout=log, stderr=subprocess.STDOUT)
            processes.append(process)
            return process
        api = start_api()
        direct = f"http://{gateway}:8080"
        def health(url):
            try:
                with urllib.request.urlopen(url + "/healthz", timeout=1) as response:
                    return response.status == 200 and json.load(response)["service"] == "donkeyspace-api"
            except (OSError, urllib.error.URLError):
                return False
        wait(lambda: health(direct), "isolated API")
        containers.append(web)
        command("docker", "run", "-d", "--name", web, "--network", name, "--add-host", f"api:{gateway}", "-p", "127.0.0.1::80", image)
        web_port = command("docker", "port", web, "80/tcp").split(":")[-1]
        origin = f"http://127.0.0.1:{web_port}"
        wait(lambda: health(origin), "production Nginx health proxy")
        user = github("user")
        issue = github(f"repos/{REPO}/issues", "POST", {"title": f"[Dashboard validation {RUN}] disposable API recovery scenario", "body": "Authorized bounded dashboard test using an isolated API and web image. No agent execution or implementation work is requested. This fresh issue will be closed after validation."})
        evidence["issue_url"] = issue["html_url"]
        print("Test issue:", issue["html_url"], flush=True)
        payload = json.dumps({"action": "opened", "issue": issue, "repository": repository, "sender": user}).encode()
        delivery = str(uuid.uuid4())
        request = urllib.request.Request(direct + "/webhooks/github", data=payload, headers={"Content-Type": "application/json", "X-GitHub-Event": "issues", "X-GitHub-Delivery": delivery, "X-Hub-Signature-256": "sha256=" + hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()})
        for _ in range(2):
            with urllib.request.urlopen(request, timeout=20) as response:
                assert response.status in (200, 202)
        evidence["duplicate_delivery"] = delivery
        with urllib.request.urlopen(origin + "/api/workflows", timeout=10) as response:
            workflows = json.load(response)
        assert len(workflows) == 1 and workflows[0]["issue_number"] == issue["number"]
        with (ROOT / "browser.log").open("w") as log:
            browser = subprocess.Popen(["node", str(SOURCE / "web/tests/live-browser.mjs"), origin, issue["title"], str(ROOT)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=log, text=True)
        processes.append(browser)
        def browser_stage(expected):
            assert select.select([browser.stdout], [], [], 40)[0], f"browser stalled before {expected}"
            assert browser.stdout.readline().strip() == expected, f"browser did not reach {expected}; inspect browser.log"
        browser_stage("ready")
        api.terminate()
        api.wait(timeout=10)
        try:
            urllib.request.urlopen(origin + "/healthz", timeout=10)
            raise AssertionError("health proxy concealed API outage")
        except urllib.error.HTTPError as error:
            assert error.code == 502
        browser.stdin.write("down\n"); browser.stdin.flush()
        browser_stage("down")
        start_api()
        wait(lambda: health(origin), "API restart through web proxy")
        browser.stdin.write("up\n"); browser.stdin.flush()
        browser_stage("passed")
        browser.wait(timeout=10)
        assert browser.returncode == 0
        evidence["result"] = "passed"
        evidence["scenarios"] = ["real facade and one fresh umbrella workflow loaded", "duplicate signed delivery retained one workflow", "loading state before facade delivery", "real API stop produces HTTP 502 and visible desktop/mobile error", "API restart and retry restore the same workflow", "health JSON through actual production Nginx"]
    finally:
        for process in reversed(processes):
            if process.poll() is None:
                process.terminate()
                try: process.wait(timeout=5)
                except subprocess.TimeoutExpired: process.kill(); process.wait()
        if issue:
            final = github(f"repos/{REPO}/issues/{issue['number']}", "PATCH", {"state": "closed", "state_reason": "not_planned"})
            assert final["state"] == "closed" and final["labels"] == []
        for container in reversed(containers):
            command("docker", "rm", "--force", container)
        if network: command("docker", "network", "rm", name)
        subprocess.run(["docker", "image", "rm", image], capture_output=True, timeout=30)
        evidence["cleanup"] = "API/browser stopped; test web/database containers, network and image removed; fresh issue closed; no agents, labels, branches or PRs created; logs/screenshots retained"
        (ROOT / "evidence.json").write_text(json.dumps(evidence, indent=2))
        print("Evidence:", ROOT / "evidence.json", flush=True)


if __name__ == "__main__":
    main()
