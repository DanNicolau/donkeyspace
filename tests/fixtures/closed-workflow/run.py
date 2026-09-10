#!/usr/bin/env python3
"""Opt-in GitHub/API/worker/container closure test, limited to umbrella.

Requires built debug binaries, gh authentication, Docker, busybox:latest, and
an empty disposable PostgreSQL DB named donkeyspace_cancellation_live_test.
Set DONKEYSPACE_CLOSURE_LIVE_TEST=1, DONKEYSPACE_CLOSURE_TEST_DATABASE_URL and
DONKEYSPACE_CLOSURE_TEST_DATABASE_CONTAINER. Never point this at a live stack.
"""
import hashlib
import hmac
import json
import os
from pathlib import Path
import socket
import subprocess
import time
import urllib.request
import uuid

REPO = "EPIC-BLOCKCHAIN/umbrella"
SOURCE = Path(__file__).resolve().parents[3]
RUN = uuid.uuid4().hex[:12]
ROOT = Path("/tmp") / f"donkeyspace-closure-live-{RUN}"


def command(*args, **kwargs):
    return subprocess.check_output(args, text=True, **kwargs).strip()


def github(endpoint, method="GET", body=None):
    args = ["gh", "api", endpoint, "--method", method]
    if body is not None:
        args += ["--input", "-"]
    raw = command(*args, input=json.dumps(body) if body is not None else None)
    return json.loads(raw) if raw else None


def wait(predicate, description, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(0.25)
    raise AssertionError(f"Timed out: {description}; logs: {ROOT}")


def main():
    assert os.environ.get("DONKEYSPACE_CLOSURE_LIVE_TEST") == "1", "explicit live-test opt-in required"
    database = os.environ["DONKEYSPACE_CLOSURE_TEST_DATABASE_URL"]
    assert database.endswith("/donkeyspace_cancellation_live_test")
    db_container = os.environ["DONKEYSPACE_CLOSURE_TEST_DATABASE_CONTAINER"]
    test_recovery = os.environ.get("DONKEYSPACE_RECOVERY_LIVE_TEST") == "1"
    test_reopen_prs = os.environ.get("DONKEYSPACE_REOPEN_PR_LIVE_TEST") == "1"
    assert not (test_recovery and test_reopen_prs), "run recovery and PR scenarios separately"
    ROOT.mkdir()
    prefix = f"ds-cxl-{RUN}"
    states = ["needs_info", "ready", "in_progress", "publishing", "pr_open", "needs_human", "blocked"]
    labels = [prefix] + [f"{prefix}:{state}" for state in states]
    user = github("user")
    repository = github(f"repos/{REPO}")
    secret = uuid.uuid4().hex
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        api_port = sock.getsockname()[1]
    base = f"http://127.0.0.1:{api_port}"
    manifest = ROOT / "plugin.yml"
    manifest.write_text('''api_version: 1
id: donkeyspace.closure-live-test
runtime:
  default_image: busybox:latest
roles:
  sleeper:
    command: [sh, -c, "trap '' TERM; touch .donkeyspace/live-ready; sleep 180"]
flows:
  cancellation:
    start: sleeper
    replaces_default_lifecycle: true
    work_items_path: test-output/index.json
    tasks:
      sleeper:
        role: sleeper
        write: [test-output]
''')
    policy = ROOT / "policy.yml"
    policy.write_text("version: 1\nworkflow:\n  state_labels:\n" + "".join(
        f'    {state}: "{prefix}:{state}"\n' for state in states
    ) + f'''  allow_labels: ["{prefix}"]
  engagement:
    default:
      allow: []
    repositories:
      "{REPO}":
        default:
          required_labels: ["{prefix}"]
          allow:
            - type: user
              login: "{user['login']}"
lifecycle:
  plugin:
    manifest_path: "{manifest}"
    flow: cancellation
checks:
  required_commands: []
  require_github_checks: false
risk:
  default: unknown
  agent_classification: true
  route_unknown_to_human: false
  route_high_to_human: false
  human_review_paths: []
automation:
  max_concurrent_jobs: 1
  retry_failed_jobs: false
  auto_merge: false
''')
    env = {key: value for key, value in os.environ.items() if not key.startswith("DONKEYSPACE_")}
    env.update({
        "DONKEYSPACE_DEPLOYMENT_MODE": "generated",
        "DONKEYSPACE_DATABASE_URL": database,
        "DONKEYSPACE_POLICY_PATH": str(policy),
        "DONKEYSPACE_GITHUB_AUTH_MODE": "pat",
        "DONKEYSPACE_GITHUB_TOKEN": command("gh", "auth", "token"),
        "DONKEYSPACE_GITHUB_REPOSITORIES": REPO,
        "DONKEYSPACE_GITHUB_INGRESS_MODE": "webhook",
        "DONKEYSPACE_WEBHOOK_SECRET": secret,
        "DONKEYSPACE_BIND_ADDR": f"127.0.0.1:{api_port}",
        "DONKEYSPACE_WORKSPACE_ROOT": str(ROOT / "workspaces"),
        "DONKEYSPACE_LEASE_SECONDS": "3",
        "RUST_LOG": "donkeyspace=info",
    })
    processes, created_labels, test_prs, test_branches = [], [], [], []
    issue = None
    evidence = {"source": command("git", "-C", str(SOURCE), "rev-parse", "HEAD"),
                "source_dirty": bool(command("git", "-C", str(SOURCE), "status", "--porcelain")),
                "umbrella_revision": github(f"repos/{REPO}/commits/main")["sha"],
                "result": "failed", "scenarios": []}

    def sql(query):
        return command("docker", "exec", db_container, "psql", "-U", "postgres", "-d",
                       "donkeyspace_cancellation_live_test", "-At", "-c", query)

    def jobs():
        return json.loads(sql("SELECT COALESCE(json_agg(j),'[]') FROM (SELECT id,status,generation,lease_expires_at>now() AS live_lease FROM jobs ORDER BY created_at) j"))

    def spawn(binary, *args):
        log = open(ROOT / f"{binary}-{len(processes)}.log", "w")
        process = subprocess.Popen([str(SOURCE / "target/debug" / binary), *args], env=env,
                                   stdout=log, stderr=subprocess.STDOUT)
        log.close()
        processes.append(process)
        return process

    def healthy():
        if processes and processes[0].poll() is not None:
            raise RuntimeError("API exited during startup: " + (ROOT / "donkeyspace-api-0.log").read_text())
        try:
            with urllib.request.urlopen(base + "/healthz", timeout=1) as response:
                return response.status == 200
        except (OSError, urllib.error.URLError):
            return False

    def ingress(action, snapshot, delivery=None, event="issues"):
        key = "pull_request" if event == "pull_request" else "issue"
        payload = json.dumps({"action": action, key: snapshot, "repository": repository, "sender": user}).encode()
        delivery = delivery or str(uuid.uuid4())
        signature = "sha256=" + hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()
        request = urllib.request.Request(base + "/webhooks/github", data=payload, headers={
            "Content-Type": "application/json", "X-GitHub-Event": event, "X-GitHub-Delivery": delivery,
            "X-Hub-Signature-256": signature,
        })
        with urllib.request.urlopen(request, timeout=30) as response:
            assert response.status in (200, 202)
        return delivery

    def create_test_pr(job, generation):
        # The real worker publishes its initial checkpoint before the sleeping
        # role starts. Test that exact branch, including the production formatter
        # and push path, then add a tiny commit so GitHub can open a draft PR.
        branch = sql(f"SELECT branch_name FROM agent_publications WHERE coordinator_job_id='{job}' AND kind='checkpoint' AND status='published' ORDER BY id DESC LIMIT 1")
        assert branch == f"donkeyspace/issue-{issue['number']}-{job}", branch
        test_branches.append(branch)
        existing = github(f"repos/{REPO}/git/ref/heads/{branch}")
        assert existing["object"]["sha"] == evidence["umbrella_revision"]
        base_commit = github(f"repos/{REPO}/git/commits/{evidence['umbrella_revision']}")
        tree = github(f"repos/{REPO}/git/trees", "POST", {
            "base_tree": base_commit["tree"]["sha"],
            "tree": [{"path": f".donkeyspace-tests/reopen-{RUN}-{generation}.txt",
                      "mode": "100644", "type": "blob", "content": f"Disposable generation {generation} test.\n"}],
        })
        commit = github(f"repos/{REPO}/git/commits", "POST", {
            "message": f"test: reopen isolation {RUN} generation {generation}",
            "tree": tree["sha"], "parents": [evidence["umbrella_revision"]],
        })
        github(f"repos/{REPO}/git/refs/heads/{branch}", "PATCH", {"sha": commit["sha"], "force": False})
        pr = github(f"repos/{REPO}/pulls", "POST", {
            "title": f"[Reopen isolation test {RUN}] generation {generation}",
            "head": branch, "base": repository["default_branch"], "draft": True,
            "body": f"Authorized disposable test. Refs #{issue['number']}. This PR will be closed without merge.\n<!-- donkeyspace-generated -->",
        })
        test_prs.append(pr)
        evidence.setdefault("pull_requests", []).append({"url": pr["html_url"], "head": branch,
            "commit": commit["sha"], "job": job, "generation": generation})
        print("Test PR:", pr["html_url"], flush=True)
        return pr

    def owned_containers():
        names = command("docker", "ps", "--all", "--filter", "label=donkeyspace.managed=true", "--format", "{{.Names}}").splitlines()
        owned = []
        for name in names:
            info = json.loads(command("docker", "inspect", name))[0]
            if any((mount.get("Source", "") == str(ROOT) or mount.get("Source", "").startswith(str(ROOT) + "/")) for mount in info["Mounts"]):
                owned.append(name)
        return owned

    try:
        assert sql("SELECT count(*) FROM information_schema.tables WHERE table_schema='public'") == "0", "test database must be empty"
        spawn("donkeyspace-api")
        wait(healthy, "isolated API startup")
        for label in labels:
            github(f"repos/{REPO}/labels", "POST", {"name": label, "color": "888888", "description": "Disposable cancellation test"})
            created_labels.append(label)
        issue = github(f"repos/{REPO}/issues", "POST", {
            "title": f"[Cancellation smoke test {RUN}] disposable sleeping plugin",
            "body": "Authorized Donkeyspace closure test. Uses an isolated stack and deterministic sleeping container. No implementation work is requested. This issue will be closed and retained as test evidence.",
            "labels": [prefix],
        })
        evidence["issue_url"] = issue["html_url"]
        print("Test issue:", issue["html_url"], flush=True)
        previous_closed = None
        for generation, reason in [(1, "completed"), (2, "not_planned")]:
            if generation == 2:
                time.sleep(1.1)
                issue = github(f"repos/{REPO}/issues/{issue['number']}", "PATCH", {"state": "open"})
            ingress("opened" if generation == 1 else "reopened", issue)
            if previous_closed:
                ingress("closed", previous_closed)  # An older delayed close must be ignored.
            worker = spawn("donkeyspace-worker", "--once", "--worker-id", f"closure-test-{RUN}")
            wait(lambda: len(list((ROOT / "workspaces").rglob("live-ready"))) == generation,
                 "sleeping plugin start")
            active = [job for job in jobs() if job["generation"] == generation]
            assert len(active) == 1 and active[0]["status"] == "running", active
            time.sleep(4)  # Exceeds the configured lease; heartbeats must keep it alive.
            active = [job for job in jobs() if job["generation"] == generation]
            assert active[0]["live_lease"] and active[0]["status"] == "running", active
            assert len(owned_containers()) == 1
            container = owned_containers()[0]
            identity = json.loads(sql(f"SELECT row_to_json(e) FROM container_executions e WHERE container_name='{container}'"))
            assert identity["coordinator_job_id"] == active[0]["id"]
            assert identity["generation"] == generation
            assert identity["lease_owner"] == f"closure-test-{RUN}"
            assert str(identity["workflow_item_id"]) == sql("SELECT id FROM workflow_items")
            container_labels = json.loads(command("docker", "inspect", container))[0]["Config"]["Labels"]
            for label, field in [("execution-id", "id"), ("coordinator-job-id", "coordinator_job_id"),
                                 ("workflow-id", "workflow_item_id"), ("workflow-generation", "generation"),
                                 ("execution-scope", "execution_scope")]:
                assert container_labels[f"donkeyspace.{label}"] == str(identity[field]), (label, identity)
            evidence.setdefault("container_executions", []).append(identity)
            print(f"Generation {generation}: running container; heartbeat verified", flush=True)
            if test_reopen_prs:
                pr = create_test_pr(active[0]["id"], generation)
                if generation == 2:
                    old = test_prs[0]
                    before = sql("SELECT current_state || ':' || (SELECT count(*) FROM outbound_actions) FROM workflow_items")
                    delivery = ingress("opened", old, event="pull_request")
                    ingress("opened", old, delivery, event="pull_request")
                    ingress("synchronize", old, event="pull_request")
                    assert sql(f"SELECT generation FROM pull_requests WHERE provider_pr_id='{old['id']}'") == "1"
                    assert sql("SELECT current_state || ':' || (SELECT count(*) FROM outbound_actions) FROM workflow_items") == before
                    delivery = ingress("opened", pr, event="pull_request")
                    after = sql("SELECT count(*) FROM outbound_actions")
                    ingress("opened", pr, delivery, event="pull_request")
                    assert sql("SELECT count(*) FROM outbound_actions") == after
                    assert sql(f"SELECT generation FROM pull_requests WHERE provider_pr_id='{pr['id']}'") == "2"
                    assert sql("SELECT current_state FROM workflow_items") == "pr_open"
                    assert test_branches[0] != test_branches[1]
                    old_ref = github(f"repos/{REPO}/git/ref/heads/{test_branches[0]}")
                    assert old_ref["object"]["sha"] == evidence["pull_requests"][0]["commit"]
                    evidence["reopen_pr_isolation"] = "passed: first-seen old PR and duplicates fenced; new PR accepted; original branch unchanged"
                    print("Late old PR fenced; new PR accepted; old branch preserved", flush=True)
            if test_recovery and generation == 2:
                old = evidence["container_executions"][0]
                args = ["docker", "run", "-d", "--rm", "--name", old["container_name"],
                        "--mount", f"type=bind,src={ROOT},dst=/fixture", "--label", "donkeyspace.managed=true"]
                for label, field in [("execution-id", "id"), ("coordinator-job-id", "coordinator_job_id"),
                                     ("workflow-id", "workflow_item_id"), ("workflow-generation", "generation"),
                                     ("execution-scope", "execution_scope")]:
                    args += ["--label", f"donkeyspace.{label}={old[field]}"]
                command(*args, "busybox:latest", "sleep", "180")
                wait(lambda: old["container_name"] not in command("docker", "ps", "-a", "--format", "{{.Names}}").splitlines(),
                     "background recovery while the reopened agent is running", timeout=40)
                assert container in owned_containers()
                assert jobs()[-1]["status"] == "running" and jobs()[-1]["live_lease"]
                evidence["background_recovery"] = "passed: old-generation late container removed during a live new-generation agent; current container/heartbeat preserved"
            if test_recovery:
                worker.kill()  # Actual SIGKILL: no supervisor cleanup can run.
                worker.wait(timeout=10)
                assert container in owned_containers(), "crash must leave a real orphan"
                time.sleep(4)  # Let the last heartbeat expire before recovery.

                def recover():
                    recovery = spawn("donkeyspace-worker", "--recover-once")
                    recovery.wait(timeout=40)
                    assert recovery.returncode == 0, "bounded recovery failed; inspect worker logs"

                def materialize(scope):
                    # Reproduce daemon materialization after an earlier absence
                    # check. Reuse this invocation's committed identity only.
                    args = ["docker", "run", "-d", "--rm", "--name", container,
                            "--mount", f"type=bind,src={ROOT},dst=/fixture"]
                    for key, value in container_labels.items():
                        if key == "donkeyspace.execution-scope":
                            value = scope
                        args += ["--label", f"{key}={value}"]
                    command(*args, "busybox:latest", "sleep", "180")

                if generation == 1:
                    # A same-name impostor must survive and prevent successful
                    # cleanup acknowledgement. The test owns/removes it explicitly.
                    command("docker", "rm", "--force", container)
                    materialize("ownership-mismatch")
                    recover()
                    assert jobs()[0]["status"] == "cancel_requested"
                    assert container in owned_containers()
                    assert "ownership mismatch" in sql(f"SELECT error FROM container_cleanup_observations WHERE execution_id='{identity['id']}'").lower()
                    command("docker", "rm", "--force", container)
                    materialize(identity["execution_scope"])
                    recover()
                    assert jobs()[0]["status"] == "cancelled"
                    assert not owned_containers()
                    assert sql("SELECT current_state FROM workflow_items") == "needs_human"
                    # Keep the tombstone even after cancellation was acknowledged.
                    materialize(identity["execution_scope"])
                    recover()
                    assert not owned_containers(), "late materialization escaped recovery"
                    assert sql("SELECT count(*) FROM lifecycle_events WHERE event_type='execution_recovery'") == "1"
                    flush = spawn("donkeyspace-worker", "--once")
                    flush.wait(timeout=40)
                    assert flush.returncode == 0
                    snapshot = github(f"repos/{REPO}/issues/{issue['number']}")
                    assert f"{prefix}:needs_human" in [label["name"] for label in snapshot["labels"]]
                    evidence["open_crash_recovery"] = "passed: expired execution fenced, ownership mismatch withheld cleanup/ack, retry removed orphan, late materialization removed, needs_human label published, no replay"

            closed = github(f"repos/{REPO}/issues/{issue['number']}", "PATCH", {"state": "closed", "state_reason": reason})
            started = time.monotonic()
            delivery = ingress("closed", closed)
            ingress("closed", closed, delivery)  # Duplicate delivery.
            ingress("closed", closed)  # Duplicate observation under another delivery.
            if test_recovery:
                if generation == 2:
                    env["DOCKER_HOST"] = f"unix://{ROOT}/missing-docker.sock"
                    unavailable = spawn("donkeyspace-worker", "--recover-once")
                    unavailable.wait(timeout=40)
                    env.pop("DOCKER_HOST")
                    assert unavailable.returncode != 0
                    assert jobs()[-1]["status"] == "cancel_requested"
                    assert container in owned_containers()
                    evidence["daemon_failure_retry"] = "passed: unavailable daemon did not acknowledge cancellation; subsequent recovery retried"
                recover()
                worker = spawn("donkeyspace-worker", "--once")
            wait(lambda: all(job["status"] == "cancelled" for job in jobs()), "cancellation acknowledgement")
            assert not owned_containers(), "worker acknowledged cancellation before container cleanup"
            # Cancellation cleanup must retain immutable launch provenance.
            assert json.loads(sql(f"SELECT row_to_json(e) FROM container_executions e WHERE id='{identity['id']}'")) == identity
            worker.wait(timeout=30)
            assert worker.returncode == 0
            assert sql("SELECT current_state || ':' || provider_close_reason FROM workflow_items") == f"finished:{reason}"
            assert sql("SELECT count(*) FROM outbound_actions WHERE status='pending'") == "0"
            final = github(f"repos/{REPO}/issues/{issue['number']}")
            assert not any(label["name"].startswith(prefix + ":") for label in final["labels"])
            evidence["scenarios"].append({"generation": generation, "close_reason": reason,
                "job": active[0]["id"], "seconds_to_cleanup": round(time.monotonic() - started, 2),
                "result": "passed", "duplicate_delivery": delivery, "worker_killed": test_recovery})
            previous_closed = closed
            print(f"Generation {generation}: closed, cancelled, container removed, active labels removed", flush=True)
        assert len({job["id"] for job in jobs()}) == 2
        assert sql("SELECT count(*) FROM state_transitions WHERE to_state='finished'") == "2"
        assert sql("SELECT count(*) FROM container_executions") == "2"
        assert len({entry["container_name"] for entry in evidence["container_executions"]}) == 2
        evidence["container_identity"] = "passed: persisted launch identities match Docker labels; distinct invocations/generations; records retained after cleanup"
        evidence["final_jobs"] = jobs()
        evidence["result"] = "passed"
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
        for name in owned_containers():
            command("docker", "rm", "--force", name)
        for pr in test_prs:
            github(f"repos/{REPO}/pulls/{pr['number']}", "PATCH", {"state": "closed"})
        if issue:
            # Include branches the worker published even if the harness failed
            # before creating a PR or the optional PR scenario was disabled.
            published = json.loads(sql("SELECT COALESCE(json_agg(DISTINCT branch_name),'[]') FROM agent_publications"))
            for branch in published:
                assert branch.startswith((f"donkeyspace/issue-{issue['number']}-", f"donkeyspace/attempt-{issue['number']}-")), branch
                if branch not in test_branches:
                    test_branches.append(branch)
        for branch in test_branches:
            removed = subprocess.run(["gh", "api", f"repos/{REPO}/git/refs/heads/{branch}",
                                      "--method", "DELETE"], text=True, capture_output=True)
            if removed.returncode and "HTTP 404" not in removed.stderr:
                raise RuntimeError(f"Failed to remove test branch {branch}: {removed.stderr}")
        if issue:
            github(f"repos/{REPO}/issues/{issue['number']}", "PATCH", {"state": "closed", "state_reason": "not_planned"})
        for label in created_labels:
            from urllib.parse import quote
            github(f"repos/{REPO}/labels/{quote(label, safe='')}", "DELETE")
        evidence["cleanup"] = "API/worker stopped; test containers, labels and branches removed; issue and test PRs closed; database and local logs retained for inspection"
        (ROOT / "evidence.json").write_text(json.dumps(evidence, indent=2))
        print("Evidence:", ROOT / "evidence.json", flush=True)


if __name__ == "__main__":
    main()
