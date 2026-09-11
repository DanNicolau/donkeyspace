#!/usr/bin/env python3
"""Bounded saved-connection / Compose / webhook test, restricted to umbrella.

Requires built CLI/API/worker binaries, Docker and saved gh authentication.
Optionally set DONKEYSPACE_TEST_CONNECTION to an App instance with umbrella access.
Credential files are mounted read-only;
no production configuration or services are changed. No coding agent is run.
"""
import hashlib
import hmac
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import time
import urllib.error
import urllib.request
import uuid

SOURCE = Path(__file__).resolve().parents[3]
REPO = "EPIC-BLOCKCHAIN/umbrella"
# Deliberately synthetic: never fetched or mutated on GitHub.
ORIGINAL = "EPIC-BLOCKCHAIN/retained-fixture"
RUN = uuid.uuid4().hex[:12]
ROOT = Path("/tmp") / f"donkeyspace-repositories-live-{RUN}"


def command(*args, **kwargs):
    return subprocess.check_output(args, text=True, timeout=180, **kwargs).strip()


def github(path, method="GET", body=None):
    args = ["gh", "api", path, "--method", method]
    if body is not None:
        args += ["--input", "-"]
    raw = command(*args, input=json.dumps(body) if body is not None else None)
    return json.loads(raw) if raw else None


def wait(predicate, description, timeout=45):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(0.25)
    raise AssertionError(f"Timed out: {description}; evidence in {ROOT}")


def main():
    assert os.environ.get("DONKEYSPACE_REPOSITORIES_LIVE_TEST") == "1"
    ROOT.mkdir(mode=0o700)
    if os.environ.get("DONKEYSPACE_TEST_CONNECTION"):
        auth = json.loads(Path(os.environ["DONKEYSPACE_TEST_CONNECTION"]).read_text())["github"]
        assert auth["mode"] == "app"
        for key in ("private_key_file", "webhook_secret_file"):
            assert Path(auth[key]).is_file()
    else:
        token_file = ROOT / "test-token"
        token_file.write_text(command("gh", "auth", "token"))
        token_file.chmod(0o600)
        secret_file = ROOT / "test-secret"
        secret_file.write_text(uuid.uuid4().hex)
        secret_file.chmod(0o600)
        auth = {"mode": "pat", "token_file": str(token_file), "webhook_secret_file": str(secret_file)}
    name = f"ds-repositories-{RUN}"
    image = name + ":test"
    config_dir = ROOT / "instance"
    fixture = ROOT / "source"
    context = ROOT / "image"
    for path in (config_dir, fixture / ".donkeyspace", context):
        path.mkdir(parents=True, mode=0o700)
    label = f"ds-repos-{RUN}"
    issue, started = None, False
    evidence = {"run": RUN, "source": command("git", "-C", str(SOURCE), "rev-parse", "HEAD"),
                "source_dirty": bool(command("git", "-C", str(SOURCE), "status", "--porcelain")),
                "repository": REPO, "original_repository": ORIGINAL, "result": "failed", "scenarios": []}
    env = {key: value for key, value in os.environ.items() if not key.startswith(("DONKEYSPACE_", "COMPOSE_"))}
    env["COMPOSE_PROJECT_NAME"] = name
    if auth["mode"] == "pat":
        env["DONKEYSPACE_GITHUB_TOKEN"] = Path(auth["token_file"]).read_text()
    cli = [str(SOURCE / "target/debug/donkeyspace"), "--config-dir", str(config_dir)]

    def run_cli(*args):
        return command(*cli, *args, env=env)

    def compose(*args):
        return command("docker", "compose", "--env-file", str(config_dir / "compose.env"),
                       "-f", str(fixture / "docker-compose.yml"), *args, env=env)

    def sql(query):
        return compose("exec", "-T", "postgres", "psql", "-U", "postgres", "-d", "repository_test", "-At", "-c", query)

    try:
        repository = github(f"repos/{REPO}")
        user = github("user")
        evidence["umbrella_revision"] = github(f"repos/{REPO}/commits/{repository['default_branch']}")["sha"]
        # Use the exact changed host binaries with their dynamic loader/libraries.
        # The base image supplies Node fetch for API readiness only.
        for binary in ("donkeyspace-api", "donkeyspace-worker"):
            shutil.copy2(SOURCE / "target/debug" / binary, context / binary)
            command("strip", str(context / binary))
        for library in ("ld-linux-x86-64.so.2", "libgcc_s.so.1", "libm.so.6", "libc.so.6"):
            shutil.copyfile(Path("/lib64") / library, context / library)
        shutil.copyfile("/etc/pki/tls/certs/ca-bundle.crt", context / "ca-certificates.crt")
        (context / "Dockerfile").write_text("FROM node:22-bookworm-slim\nCOPY --chmod=0755 . /tested/\nENV SSL_CERT_FILE=/tested/ca-certificates.crt\n")
        with (ROOT / "image-build.log").open("w") as log:
            subprocess.run(["docker", "build", "-t", image, str(context)], check=True,
                           timeout=180, stdout=log, stderr=subprocess.STDOUT)

        policy = (SOURCE / ".donkeyspace/policy.yml").read_text().replace('"ai"', json.dumps(label)).replace('"ai:', '"' + label + ':')
        (fixture / ".donkeyspace/policy.yml").write_text(policy)
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        if auth["mode"] == "app":
            connection = {key: auth[key] for key in ("mode", "app_id", "installation_id", "private_key_file", "webhook_secret_file")}
            connection["ingress"] = {"kind": "webhook", "public_url": "https://hooks.example/test"}
        else:
            connection = {"mode": "pat", "token_file": auth["token_file"], "ingress": {"kind": "polling", "interval_seconds": 137}}
        connection["repositories"] = [ORIGINAL]
        configuration = {"schema_version": 7, "source_tree": str(fixture), "runtime_source": "local-build",
                         "api_port": port, "web_port": port + 1, "github": connection,
                         "github_access": {ORIGINAL: [{"type": "user", "login": user["login"]}]},
                         "github_approvers": {ORIGINAL: [{"type": "user", "login": user["login"]}]},
                         "plugins": {}, "facade": {"display_name": "Repository validation"}}
        (config_dir / "instance.json").write_text(json.dumps(configuration))
        (config_dir / "instance.json").chmod(0o600)
        common = {"DONKEYSPACE_DEPLOYMENT_MODE": "generated", "DONKEYSPACE_DATABASE_URL": "postgres://postgres:test-only@postgres:5432/repository_test",
                  "DONKEYSPACE_POLICY_PATH": "/run/donkeyspace/policy.yml", "DONKEYSPACE_GITHUB_AUTH_MODE": auth["mode"],
                  "DONKEYSPACE_WEBHOOK_SECRET_FILE": "/run/secrets/github_webhook_secret",
                  "DONKEYSPACE_GITHUB_REPOSITORIES": "${DONKEYSPACE_GITHUB_REPOSITORIES}", "DONKEYSPACE_GITHUB_INGRESS_MODE": "${DONKEYSPACE_GITHUB_INGRESS_MODE}",
                  "DONKEYSPACE_GITHUB_POLL_REPOSITORIES": "${DONKEYSPACE_GITHUB_POLL_REPOSITORIES}",
                  "DONKEYSPACE_GITHUB_POLL_INTERVAL_SECONDS": "${DONKEYSPACE_GITHUB_POLL_INTERVAL_SECONDS}",
                  "DONKEYSPACE_GITHUB_POLL_MAX_PAGES": "1", "DONKEYSPACE_TRIAGE_PROVIDER": "deterministic", "RUST_LOG": "donkeyspace=info"}
        mounts = ["${DONKEYSPACE_POLICY_SOURCE}:/run/donkeyspace/policy.yml:ro", auth["webhook_secret_file"] + ":/run/secrets/github_webhook_secret:ro"]
        if auth["mode"] == "app":
            common.update(DONKEYSPACE_GITHUB_APP_ID="${DONKEYSPACE_GITHUB_APP_ID}", DONKEYSPACE_GITHUB_INSTALLATION_ID="${DONKEYSPACE_GITHUB_INSTALLATION_ID}", DONKEYSPACE_GITHUB_PRIVATE_KEY_FILE="/run/secrets/github_private_key")
            mounts.append("${DONKEYSPACE_GITHUB_PRIVATE_KEY_SOURCE}:/run/secrets/github_private_key:ro")
        else:
            common["DONKEYSPACE_GITHUB_TOKEN"] = "${DONKEYSPACE_GITHUB_TOKEN}"
        services = {"postgres": {"image": "postgres:17", "environment": {"POSTGRES_PASSWORD": "test-only", "POSTGRES_DB": "repository_test"},
                                 "healthcheck": {"test": ["CMD", "pg_isready", "-U", "postgres"], "interval": "1s", "timeout": "2s", "retries": 30}}}
        for service in ("api", "worker"):
            services[service] = {"image": image, "environment": dict(common), "volumes": mounts,
                                 "command": ["/tested/ld-linux-x86-64.so.2", "--library-path", "/tested", f"/tested/donkeyspace-{service}"],
                                 "depends_on": {"postgres": {"condition": "service_healthy"}}}
        services["api"]["environment"]["DONKEYSPACE_BIND_ADDR"] = "0.0.0.0:8080"
        services["api"]["ports"] = [f"127.0.0.1:{port}:8080"]
        services["api"]["healthcheck"] = {"test": ["CMD", "node", "-e", "fetch('http://127.0.0.1:8080/readyz').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"], "interval": "1s", "timeout": "2s", "retries": 30}
        services["worker"]["depends_on"]["api"] = {"condition": "service_healthy"}
        (fixture / "docker-compose.yml").write_text(json.dumps({"services": services}))
        # Generate only the isolated Compose inputs; never print credential configuration.
        run_cli("compose-config")
        started = True
        compose("up", "-d", "--wait", "--wait-timeout", "60")
        base = f"http://127.0.0.1:{port}"

        def api(path):
            with urllib.request.urlopen(base + path, timeout=10) as response:
                return json.load(response)

        def deliver(action, delivery=None):
            body = json.dumps({"action": action, "issue": issue, "repository": repository,
                               "sender": user, **({"installation": {"id": auth["installation_id"]}} if auth["mode"] == "app" else {})}).encode()
            secret = Path(auth["webhook_secret_file"]).read_bytes().strip()
            request = urllib.request.Request(base + "/webhooks/github", data=body, headers={"Content-Type": "application/json", "X-GitHub-Event": "issues",
                "X-GitHub-Delivery": delivery or str(uuid.uuid4()), "X-Hub-Signature-256": "sha256=" + hmac.new(secret, body, hashlib.sha256).hexdigest()})
            try:
                with urllib.request.urlopen(request, timeout=20) as response:
                    return response.status
            except urllib.error.HTTPError as error:
                return error.code

        assert api("/api/repositories") == [ORIGINAL]
        before = json.loads((config_dir / "instance.json").read_text())
        output = run_cli("configure", "repositories", "add", REPO)
        assert "awaiting controlled" in output
        after = json.loads((config_dir / "instance.json").read_text())
        assert after["github"]["repositories"] == [ORIGINAL, REPO]
        for key, value in before.items():
            if key not in ("github", "github_access", "github_approvers", "repositories_pending_apply"):
                assert after[key] == value
        assert after["github_access"][ORIGINAL] == before["github_access"][ORIGINAL]
        assert after["github_approvers"][ORIGINAL] == before["github_approvers"][ORIGINAL]
        for key in before["github"]:
            if key != "repositories":
                assert after["github"][key] == before["github"][key]
        saved_bytes = (config_dir / "instance.json").read_bytes()
        run_cli("configure", "repositories", "add", REPO.lower())
        assert (config_dir / "instance.json").read_bytes() == saved_bytes
        failed = subprocess.run([*cli, "configure", "repositories", "add", f"EPIC-BLOCKCHAIN/unavailable-{RUN}"], env=env, capture_output=True, text=True, timeout=45)
        assert failed.returncode and "existing App installation" in failed.stderr
        assert (config_dir / "instance.json").read_bytes() == saved_bytes
        assert api("/api/repositories") == [ORIGINAL], "save unexpectedly applied settings"
        refused = subprocess.run([*cli, "up"], env=env, capture_output=True, text=True, timeout=30)
        assert refused.returncode and "awaiting controlled" in refused.stderr
        evidence["scenarios"].append("saved connection reused; additive/idempotent selection; inaccessible add unchanged; live consumers unchanged before apply")
        run_cli("configure", "repositories", "apply", "--confirm-drained")
        assert api("/api/repositories") == sorted([ORIGINAL, REPO])
        for service in ("api", "worker"):
            container = compose("ps", "-q", service)
            inspected = json.loads(command("docker", "inspect", container))[0]
            assert inspected["State"]["Running"]
            assert f"DONKEYSPACE_GITHUB_REPOSITORIES={ORIGINAL},{REPO}" in inspected["Config"]["Env"]
        evidence["scenarios"].append("actual Compose apply recreated healthy API and running worker with identical repository selection")

        github(f"repos/{REPO}/labels", "POST", {"name": label, "color": "888888", "description": "Disposable repository-management validation"})
        issue = github(f"repos/{REPO}/issues", "POST", {"title": f"[Repository validation {RUN}] disposable ingestion scenario", "labels": [label],
            "body": "Authorized isolated repository-management validation. No coding agent should run. This fresh issue will be closed and its scoped labels removed after the test."})
        evidence["issue_url"] = issue["html_url"]
        print("Test issue:", issue["html_url"], flush=True)
        assert deliver("opened") in (200, 202)
        assert sql("SELECT count(*) FROM jobs") == "0"
        assert sql(f"SELECT disposition FROM engagement_decisions e JOIN workflow_items w ON w.id=e.workflow_item_id WHERE w.issue_number={issue['number']} ORDER BY e.id DESC LIMIT 1") == "denied"
        evidence["scenarios"].append("new repository ingestion persisted but deny-all prevented a job start")
        # Stop the real worker before granting starts. The test never executes agents.
        compose("stop", "worker")
        run_cli("configure", "github-access", "--repository", REPO, "add", "--user", user["login"])
        wait(lambda: api("/api/repositories") == sorted([ORIGINAL, REPO]), "API access update")
        delivery = str(uuid.uuid4())
        for _ in range(2):
            assert deliver("opened", delivery) in (200, 202)
        assert sql("SELECT count(*) FROM jobs") == "1"
        assert sql("SELECT status FROM jobs") == "queued"
        evidence["job_id"] = sql("SELECT id FROM jobs")
        evidence["duplicate_delivery"] = delivery
        evidence["scenarios"].append("authorized starter ingested a fresh issue; duplicate delivery created exactly one queued job; worker stopped")
        run_cli("configure", "repositories", "remove", REPO, "--confirm")
        assert sql("SELECT status FROM jobs") == "queued", "removal silently cancelled work"
        assert api("/api/repositories") == sorted([ORIGINAL, REPO])
        # Explicit test cleanup cancels the queued fixture before restarting a worker.
        issue = github(f"repos/{REPO}/issues/{issue['number']}", "PATCH", {"state": "closed", "state_reason": "not_planned"})
        assert deliver("closed") in (200, 202)
        run_cli("configure", "repositories", "apply", "--confirm-drained")
        assert api("/api/repositories") == [ORIGINAL]
        assert deliver("opened") == 403
        assert sql("SELECT count(*) FROM jobs") == "1"
        assert sql(f"SELECT count(*) FROM workflow_items WHERE issue_number={issue['number']}") == "1"
        evidence["scenarios"].append("removal saved without cancelling queued work; explicit fixture closure then apply rejected new deliveries while retaining history")
        evidence["result"] = "passed"
    finally:
        cleanup_errors = []
        if started:
            try:
                (ROOT / "services.log").write_text(compose("logs", "--no-color"))
                compose("down", "--volumes", "--remove-orphans")
            except Exception as error:
                cleanup_errors.append(str(error))
        if issue:
            try:
                github(f"repos/{REPO}/issues/{issue['number']}", "PATCH", {"state": "closed", "state_reason": "not_planned", "labels": []})
            except Exception as error:
                cleanup_errors.append(str(error))
        try:
            labels = github(f"repos/{REPO}/labels?per_page=100")
            for entry in labels:
                if entry["name"] == label or entry["name"].startswith(label + ":"):
                    github(f"repos/{REPO}/labels/{entry['name']}", "DELETE")
            command("docker", "image", "rm", image)
        except Exception as error:
            cleanup_errors.append(str(error))
        for private_file in (ROOT / "test-token", ROOT / "test-secret"):
            private_file.unlink(missing_ok=True)
        evidence["cleanup_errors"] = cleanup_errors
        (ROOT / "evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")
        print("Evidence:", ROOT / "evidence.json", flush=True)
        assert not cleanup_errors, cleanup_errors


if __name__ == "__main__":
    main()
