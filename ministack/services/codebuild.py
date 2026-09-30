# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""
CodeBuild Service Emulator.
JSON-based API via X-Amz-Target: CodeBuild_20161006.<Operation>.

Supports:
  Projects:  CreateProject, BatchGetProjects, ListProjects,
             UpdateProject, DeleteProject
  Fleets:    CreateFleet, BatchGetFleets, ListFleets,
             UpdateFleet, DeleteFleet
  Builds:    StartBuild, BatchGetBuilds, StopBuild,
             ListBuilds, ListBuildsForProject, BatchDeleteBuilds

Builds are metadata-only by default. Set MINISTACK_CODEBUILD_EXECUTE=1 to
really run them (see "Build execution" below).
"""

import contextvars
import copy
import datetime
import hashlib
import json
import logging
import os
import re
import secrets
import threading
import time

from ministack.core import container_reaper
from ministack.core.arn import ArnParseError, is_arn, parse_arn
from ministack.core.concurrency import run_reentrant
from ministack.core.responses import (
    AccountRegionScopedDict,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
    new_uuid,
)

logger = logging.getLogger("codebuild")

# Cap any single Docker daemon call. docker-py defaults to 60s, which turns a
# slow or wedged daemon into a minutes-long stall on a request path.
_DOCKER_TIMEOUT = float(os.environ.get("MINISTACK_DOCKER_TIMEOUT", "10"))


REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

# ---------------------------------------------------------------------------
# In-memory state
# ---------------------------------------------------------------------------
_projects = AccountRegionScopedDict()    # project_name -> project record
_builds = AccountRegionScopedDict()      # build_id -> build record
_fleets = AccountRegionScopedDict()      # fleet arn -> fleet record


def reset():
    _projects.clear()
    _builds.clear()
    _fleets.clear()
    with _stopped_lock:
        _stopped_builds.clear()


def get_state():
    return copy.deepcopy({
        "projects": _projects,
        "builds": _builds,
        "fleets": _fleets,
    })


def load_persisted_state(data):
    return _restore_state(data)


def _restore_state(data):
    _projects.update(data.get("projects", {}))
    _builds.update(data.get("builds", {}))
    _fleets.update(data.get("fleets", {}))

    # An executed build lives in a container and a worker thread, neither of
    # which survives a restart, so a restored IN_PROGRESS build would poll as
    # running forever.
    for build in _builds.values():
        if build.get("buildStatus") == "IN_PROGRESS":
            build["buildStatus"] = "FAULT"
            build["currentPhase"] = "COMPLETED"
            build.setdefault("endTime", int(time.time()))




# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _project_arn(name):
    return f"arn:aws:codebuild:{get_region()}:{get_account_id()}:project/{name}"


def _build_arn(build_id):
    return f"arn:aws:codebuild:{get_region()}:{get_account_id()}:build/{build_id}"


def _fleet_arn(name, fleet_id):
    """A fleet's ARN carries its name and its id: two fleets created under one
    name in turn get distinct ARNs, so a project's fleetArn names one fleet."""
    return f"arn:aws:codebuild:{get_region()}:{get_account_id()}:fleet/{name}:{fleet_id}"


def _fleet_by_name(name):
    for fleet in _fleets.values():
        if fleet.get("name") == name:
            return fleet
    return None


def _fleet_from_identifier(value):
    """The fleet an ARN or a name identifies — BatchGetFleets takes either."""
    fleet = _fleets.get(value)
    return fleet if fleet is not None else _fleet_by_name(value)


def _project_name_from_identifier(value):
    """Resolve a project name for BatchGetProjects-style not-found semantics."""
    if not is_arn(value):
        return value
    try:
        spec = parse_arn(value)
    except ArnParseError:
        return None

    if (
        spec.partition != "aws"
        or spec.service != "codebuild"
        or spec.region != get_region()
        or spec.account_id != get_account_id()
    ):
        return None

    prefix = "project/"
    if not spec.resource.startswith(prefix):
        return None
    project_name = spec.resource[len(prefix):]
    return project_name or None


def _build_id(project_name):
    """Generate a build ID like 'project-name:build-uuid'."""
    return f"{project_name}:{new_uuid()}"


def _make_build_record(project, build_id, source_version=None):
    """Create a build record that immediately shows SUCCEEDED."""
    now = int(time.time())
    return {
        "id": build_id,
        "arn": _build_arn(build_id),
        "buildNumber": len([b for b in _builds.values()
                            if b.get("projectName") == project["name"]]) + 1,
        "startTime": now,
        "endTime": now,
        "currentPhase": "COMPLETED",
        "buildStatus": "SUCCEEDED",
        "sourceVersion": source_version or project.get("sourceVersion", "refs/heads/main"),
        "projectName": project["name"],
        "phases": [
            {"phaseType": "SUBMITTED", "phaseStatus": "SUCCEEDED", "startTime": now, "endTime": now},
            {"phaseType": "PROVISIONING", "phaseStatus": "SUCCEEDED", "startTime": now, "endTime": now},
            {"phaseType": "DOWNLOAD_SOURCE", "phaseStatus": "SUCCEEDED", "startTime": now, "endTime": now},
            {"phaseType": "INSTALL", "phaseStatus": "SUCCEEDED", "startTime": now, "endTime": now},
            {"phaseType": "PRE_BUILD", "phaseStatus": "SUCCEEDED", "startTime": now, "endTime": now},
            {"phaseType": "BUILD", "phaseStatus": "SUCCEEDED", "startTime": now, "endTime": now},
            {"phaseType": "POST_BUILD", "phaseStatus": "SUCCEEDED", "startTime": now, "endTime": now},
            {"phaseType": "UPLOAD_ARTIFACTS", "phaseStatus": "SUCCEEDED", "startTime": now, "endTime": now},
            {"phaseType": "FINALIZING", "phaseStatus": "SUCCEEDED", "startTime": now, "endTime": now},
            {"phaseType": "COMPLETED", "phaseStatus": "SUCCEEDED", "startTime": now, "endTime": now},
        ],
        "source": project.get("source", {}),
        "artifacts": project.get("artifacts", {"type": "NO_ARTIFACTS"}),
        "environment": project.get("environment", {}),
        "logs": {
            "groupName": f"/aws/codebuild/{project['name']}",
            "streamName": build_id.replace(":", "/"),
        },
        "timeoutInMinutes": project.get("timeoutInMinutes", 60),
        "initiator": f"{get_account_id()}/user",
        "encryptionKey": f"arn:aws:kms:{get_region()}:{get_account_id()}:alias/aws/codebuild",
    }


# ---------------------------------------------------------------------------
# Build execution (opt-in via MINISTACK_CODEBUILD_EXECUTE=1)
#
# The buildspec is handed to the official AWS CodeBuild local agent, which runs
# the phases in the project's image the way CodeBuild does, instead of this
# emulator reimplementing an executor.
#
# WORKSPACE must resolve to the same path on the host and in this container,
# because the agent bind-mounts it into the build container:
#   docker run -p 4566:4566 \
#     -v /var/run/docker.sock:/var/run/docker.sock \
#     -v /tmp/ministack-codebuild:/tmp/ministack-codebuild \
#     -e MINISTACK_CODEBUILD_EXECUTE=1 ministackorg/ministack
# ---------------------------------------------------------------------------

EXECUTE_BUILDS = os.environ.get("MINISTACK_CODEBUILD_EXECUTE", "0").lower() in ("1", "true", "yes")

# The fixed image AWS's own codebuild_build.sh uses to run a local build; not
# a real choice, so it is not configurable.
AGENT_IMAGE = "public.ecr.aws/codebuild/local-builds:latest"

# Internal scratch space for the source/artifacts/env files handed to the
# agent container; not an AWS concept, so not configurable.
WORKSPACE = "/tmp/ministack-codebuild"

# Extra `docker run` flags for the AGENT container, with the same syntax and the
# same parser as LAMBDA_DOCKER_FLAGS.
#
# The agent's entire job is to drive the Docker socket it is handed, so on an
# SELinux-enforcing host it needs `--security-opt label=disable` to function at
# all: it is otherwise denied both the socket connect and its own env-file
# mount, and `:z` does not help because relabelling the socket file is a
# different permission from connecting to it.
#
# Without this hook there is no configuration that makes CodeBuild work under
# SELinux, and the failure is silent — the agent exits 0 after starting nothing,
# and _execute_build reads that exit code as SUCCEEDED.
CODEBUILD_DOCKER_FLAGS = os.environ.get("CODEBUILD_DOCKER_FLAGS", "")

# A function container reaches the host as host.docker.internal on any engine,
# because ministack maps that name to the engine's host gateway (lambda_svc).
# The build container is started by the agent's docker-compose, from a compose
# file the agent generates and last rewrites itself — edit-docker-compose keeps
# only the keys it knows — so the build service maps nothing, and on a Linux
# engine, where only Docker Desktop's resolver answers the name unmapped, a
# build cannot reach a server on the host. This wrapper, mounted ahead of the
# agent's docker-compose on its PATH, gives the build service the same mapping
# in the final file, then runs the agent's own docker-compose.
_COMPOSE_WRAPPER_PATH = "/usr/local/sbin/docker-compose"
_COMPOSE_WRAPPER = """#!/bin/sh
f=/LocalBuild/agent-resources/customer-specific.yml
if [ -f "$f" ] && ! grep -q 'host.docker.internal:host-gateway' "$f"; then
  sed -i 's/^  build:$/  build:\\n    extra_hosts:\\n      - "host.docker.internal:host-gateway"/' "$f"
fi
exec /usr/local/bin/docker-compose "$@"
"""


def _compose_project(build_id):
    """The docker-compose project one build's agent brings up.

    The agent names its project after its working directory, agent-resources,
    so every build shared one: the agent's own cleanup — a `down -v` at the
    start of each run — took down whatever build was running under that name,
    on this MiniStack or another on the same daemon, and nothing took down the
    last one. A project per build is what lets the build's end remove exactly
    its own (_remove_compose_project).
    """
    suffix = re.sub(r"[^a-z0-9_-]", "-", build_id.rsplit(":", 1)[-1].lower())
    return f"ministack-codebuild-{suffix}"


def _remove_compose_project(client, project):
    """Remove what the agent's docker-compose made for one build: its agent and
    build containers, then their network and named volumes, which compose
    labels with the project. Best effort, each on its own: a build's end must
    not fail over its cleanup."""
    selector = {"label": f"com.docker.compose.project={project}"}

    def _each(kind, listed, remove):
        try:
            items = listed()
        except Exception as exc:
            logger.warning("Could not list the %ss of %s: %s", kind, project, exc)
            return
        for item in items:
            try:
                remove(item)
            except Exception as exc:
                logger.warning("Could not remove %s %s of %s: %s",
                               kind, getattr(item, "name", item), project, exc)

    # Containers first: a network or volume still in use cannot be removed.
    _each("container", lambda: client.containers.list(all=True, filters=selector),
          lambda c: c.remove(force=True, v=True))
    _each("network", lambda: client.networks.list(filters=selector), lambda n: n.remove())
    _each("volume", lambda: client.volumes.list(filters=selector),
          lambda v: v.remove(force=True))


_PHASE_COMPLETE_RE = re.compile(r"Phase complete: ([A-Z_]+) State: ([A-Z_]+)")

_docker = None

# StopBuild removes the container, which ends the worker's log stream; without
# recording the intent the worker would overwrite the requested STOPPED outcome
# with FAULT or FAILED.
_stopped_builds = set()
_stopped_lock = threading.Lock()


def _mark_stopped(build_id):
    with _stopped_lock:
        _stopped_builds.add(build_id)


def _stop_requested(build_id):
    with _stopped_lock:
        return build_id in _stopped_builds


def _clear_stopped(build_id):
    with _stopped_lock:
        _stopped_builds.discard(build_id)


def _get_docker():
    global _docker
    if _docker is None:
        try:
            import docker
            _docker = docker.from_env(timeout=_DOCKER_TIMEOUT)
        except Exception:
            logger.exception("Docker unavailable; builds stay metadata-only")
            _docker = False
    return _docker or None


def _log_sink(build):
    """Return a callable that appends a line to the build's log stream.

    The build record already advertises ``logs.groupName`` / ``logs.streamName``,
    so an executed build writes its output there and `aws logs` reads it back.
    """
    from ministack.services import cloudwatch_logs as _cwl

    group_name = build["logs"]["groupName"]
    stream_name = build["logs"]["streamName"]
    now_ms = int(time.time() * 1000)

    if group_name not in _cwl._log_groups:
        _cwl._log_groups[group_name] = {
            "arn": _cwl._make_group_arn(group_name),
            "creationTime": now_ms,
            "retentionInDays": None,
            "tags": {},
            "subscriptionFilters": {},
            "streams": {},
        }
    group = _cwl._log_groups[group_name]
    if stream_name not in group["streams"]:
        group["streams"][stream_name] = {
            "events": [],
            "uploadSequenceToken": "1",
            "creationTime": now_ms,
            "firstEventTimestamp": None,
            "lastEventTimestamp": None,
            "lastIngestionTime": None,
        }
    stream = group["streams"][stream_name]

    def emit(line):
        ts = int(time.time() * 1000)
        events = stream["events"]
        events.append({"timestamp": ts, "message": line, "ingestionTime": ts})
        if stream["firstEventTimestamp"] is None:
            stream["firstEventTimestamp"] = ts
        stream["lastEventTimestamp"] = ts
        stream["lastIngestionTime"] = ts

    return emit


def _container_for_build(build_id):
    client = _get_docker()
    if not client:
        return None
    found = client.containers.list(
        all=True, filters={"label": f"ministack.codebuild.build={build_id}"}
    )
    return found[0] if found else None


def _aws_endpoint():
    """Address the build container can reach MiniStack on, or "" if it can't.

    A build that calls the AWS CLI should reach this emulator rather than real
    AWS. The build container is a sibling started by the agent, so it reaches
    MiniStack over the default bridge gateway.
    """
    port = os.environ.get("GATEWAY_PORT") or os.environ.get("EDGE_PORT") or "4566"
    client = _get_docker()
    if not client:
        return ""
    try:
        config = client.networks.get("bridge").attrs["IPAM"]["Config"]
        gateway = next(entry["Gateway"] for entry in config if entry.get("Gateway"))
    except Exception:
        logger.debug("Could not resolve the bridge gateway; builds get no AWS endpoint")
        return ""
    return f"http://{gateway}:{port}"


# The path a build container reads its session on; the token after it names
# one build's session and is unguessable, as CodeBuild's own endpoint is.
CREDENTIALS_PATH = "/_ministack/codebuild/credentials/"

_build_sessions = {}                     # token -> session answer
_build_session_tokens = {}               # build id -> token
_build_sessions_lock = threading.Lock()


def _build_session(project, build):
    """Mint a session of the project's service role for one build.

    CodeBuild runs a build as its project's service role. The session is
    minted the way the Lambda runtime mints a function's
    (core.lambda_runtime.execution_credentials) and registered with STS, so a
    call signed with it is evaluated as the role and lands in the role's
    account. Unlike a function's, it is minted without AUTH too: CodeBuild
    always serves a session with a token, and a buildspec may require one.
    """
    role_arn = project.get("serviceRole", "")
    try:
        account_id = parse_arn(role_arn).account_id or get_account_id()
    except ArnParseError:
        account_id = get_account_id()
    role_name = role_arn.rsplit("/", 1)[-1]
    session_name = f"AWSCodeBuild-{build['id'].split(':', 1)[-1]}"[:64]
    digest = hashlib.sha256(f"{build['arn']}:{role_arn}".encode()).hexdigest()
    access_key = f"ASIA{digest[:16].upper()}"
    secret_key = hashlib.sha256(f"secret:{digest}".encode()).hexdigest()
    session_token = hashlib.sha256(f"token:{digest}".encode()).hexdigest()
    # The session outlives the build's own timeout, so no build sees it lapse.
    expires = time.time() + _timeout_seconds(project) + 900

    from ministack.services import sts as sts_svc

    sts_svc.register_session(access_key, {
        "Arn": f"arn:aws:sts::{account_id}:assumed-role/{role_name}/{session_name}",
        "UserId": f"{role_name}:{session_name}",
        "SecretAccessKey": secret_key,
        "SessionToken": session_token,
        "Expiration": expires,
        "AccountId": account_id,
        "PrincipalType": "AssumedRole",
    })
    return {
        "AccessKeyId": access_key,
        "SecretAccessKey": secret_key,
        "Token": session_token,
        "Expiration": datetime.datetime.fromtimestamp(expires, datetime.timezone.utc)
        .strftime("%Y-%m-%dT%H:%M:%SZ"),
        "RoleArn": role_arn,
    }


def _serve_build_session(project, build):
    """Mint the build's session and answer it at a token of its own; return
    the token and the session."""
    session = _build_session(project, build)
    token = secrets.token_hex(16)
    with _build_sessions_lock:
        _build_sessions[token] = session
        _build_session_tokens[build["id"]] = token
    return token, session


def _withdraw_build_session(build_id):
    """Stop answering a build's session once the build has ended."""
    with _build_sessions_lock:
        token = _build_session_tokens.pop(build_id, None)
        if token:
            _build_sessions.pop(token, None)


def serve_build_credentials(token):
    """GET <CREDENTIALS_PATH><token>: the session of the build the token names,
    in the shape a container credential endpoint answers."""
    with _build_sessions_lock:
        session = _build_sessions.get(token)
    if session is None:
        return 404, {"Content-Type": "application/json"}, json.dumps(
            {"message": "no build holds this credential token"}).encode()
    return 200, {"Content-Type": "application/json"}, json.dumps(session).encode()


def _write_env_file(path, project, build):
    env = project.get("environment", {}) or {}
    declared = {var.get("name") for var in env.get("environmentVariables") or []}
    with open(path, "w", encoding="utf-8") as fh:
        for var in env.get("environmentVariables") or []:
            fh.write(f"{var.get('name', '')}={var.get('value', '')}\n")
        fh.write(f"CODEBUILD_BUILD_ID={build['id']}\n")
        fh.write(f"CODEBUILD_BUILD_ARN={build['arn']}\n")
        fh.write(f"CODEBUILD_BUILD_NUMBER={build['buildNumber']}\n")
        fh.write(f"CODEBUILD_INITIATOR={build['initiator']}\n")
        fh.write(f"AWS_DEFAULT_REGION={get_region()}\n")

        endpoint = _aws_endpoint()
        if endpoint and "AWS_ENDPOINT_URL" not in declared:
            fh.write(f"AWS_ENDPOINT_URL={endpoint}\n")
            # The build runs as its project's service role. CodeBuild serves
            # the role's session at AWS_CONTAINER_CREDENTIALS_FULL_URI, and so
            # does this endpoint. The AWS SDKs read a plain-http full URI only
            # from a loopback or link-local host, which the bridge gateway is
            # not, so the same session is also in the SDK's own variables, the
            # way Lambda hands a function its role's session.
            token, session = _serve_build_session(project, build)
            fh.write(f"AWS_CONTAINER_CREDENTIALS_FULL_URI={endpoint}{CREDENTIALS_PATH}{token}\n")
            for name, value in (("AWS_ACCESS_KEY_ID", session["AccessKeyId"]),
                                ("AWS_SECRET_ACCESS_KEY", session["SecretAccessKey"]),
                                ("AWS_SESSION_TOKEN", session["Token"])):
                if name not in declared:
                    fh.write(f"{name}={value}\n")


def _record_phase(build, phase_type, status):
    now = int(time.time())
    for phase in build["phases"]:
        if phase["phaseType"] == phase_type and "endTime" not in phase:
            phase["phaseStatus"] = status
            phase["endTime"] = now
            break
    else:
        build["phases"].append(
            {"phaseType": phase_type, "phaseStatus": status, "startTime": now, "endTime": now}
        )
    build["currentPhase"] = phase_type


def _finish_build(build, status):
    build["buildStatus"] = status
    build["currentPhase"] = "COMPLETED"
    build["endTime"] = int(time.time())
    _record_phase(build, "COMPLETED", status)


def _timeout_seconds(project):
    try:
        minutes = int(project.get("timeoutInMinutes") or 60)
    except (TypeError, ValueError):
        minutes = 60
    return max(1, minutes) * 60


def _execute_build(build_id, project):
    """Run a build through the CodeBuild local agent; update its record live."""
    build = _builds.get(build_id)
    client = _get_docker()
    if build is None:
        return
    if not client:
        _finish_build(build, "FAULT")
        return

    env = project.get("environment", {}) or {}
    workdir = os.path.join(WORKSPACE, build_id.replace(":", "_"))
    source_dir = os.path.join(workdir, "src")
    artifacts_dir = os.path.join(workdir, "artifacts")
    env_dir = os.path.join(workdir, "env")
    compose_project = _compose_project(build_id)

    try:
        for path in (source_dir, artifacts_dir, env_dir):
            os.makedirs(path, exist_ok=True)

        buildspec = (project.get("source") or {}).get("buildspec") or ""
        if not buildspec.strip():
            logger.error("Build %s has no inline buildspec to execute", build_id)
            _finish_build(build, "FAILED")
            return

        buildspec_path = os.path.join(source_dir, "buildspec.yml")
        with open(buildspec_path, "w", encoding="utf-8") as fh:
            fh.write(buildspec)
        _write_env_file(os.path.join(env_dir, "env.list"), project, build)
        compose_wrapper = os.path.join(workdir, "docker-compose")
        with open(compose_wrapper, "w", encoding="utf-8") as fh:
            fh.write(_COMPOSE_WRAPPER)
        os.chmod(compose_wrapper, 0o755)

        _record_phase(build, "QUEUED", "SUCCEEDED")
        build["buildStatus"] = "IN_PROGRESS"

        run_kwargs = dict(
            detach=True,
            environment={
                "LOCAL_AGENT_IMAGE_NAME": AGENT_IMAGE,
                "IMAGE_NAME": env.get("image", "aws/codebuild/standard:7.0"),
                "SOURCE": source_dir,
                "MOUNT_SOURCE_DIRECTORY": "TRUE",
                "ARTIFACTS": artifacts_dir,
                "BUILDSPEC": buildspec_path,
                "ENV_VAR_FILE": "env.list",
                "INITIATOR": "ministack",
                "DOCKER_PRIVILEGED_MODE": "true" if env.get("privilegedMode") else "",
            },
            volumes={
                "/var/run/docker.sock": {"bind": "/var/run/docker.sock", "mode": "rw"},
                env_dir: {"bind": "/LocalBuild/envFile", "mode": "ro"},
                compose_wrapper: {"bind": _COMPOSE_WRAPPER_PATH, "mode": "ro"},
            },
            labels=container_reaper.own_labels("codebuild", **{"ministack.codebuild.build": build_id}),
        )

        if CODEBUILD_DOCKER_FLAGS:
            # Imported here rather than at module scope: the services package
            # loads its modules eagerly, and a top-level cross-service import
            # would order codebuild after lambda.
            from ministack.services.lambda_svc import _parse_docker_flags

            extra = _parse_docker_flags(CODEBUILD_DOCKER_FLAGS)
            # The volumes above are what the agent contract requires; a --volume
            # in the flags would silently replace them, so it is dropped.
            extra.pop("mounts", None)
            run_kwargs["environment"].update(extra.pop("environment", {}))
            run_kwargs.update(extra)

        # After the flags: the build's end removes this project, so it is not
        # the flags' to rename.
        run_kwargs["environment"]["COMPOSE_PROJECT_NAME"] = compose_project
        container = client.containers.run(AGENT_IMAGE, **run_kwargs)
    except Exception:
        logger.exception("Failed to start build %s", build_id)
        _withdraw_build_session(build_id)
        _finish_build(build, "FAULT")
        return

    try:
        emit = _log_sink(build)
    except Exception:
        logger.exception("Could not open the log stream for %s", build_id)
        emit = None

    # The log stream blocks until the container ends, so the timeout is a timer
    # that removes it; that ends the stream and is reported as TIMED_OUT.
    timed_out = threading.Event()

    def _on_timeout():
        timed_out.set()
        logger.error("Build %s exceeded timeoutInMinutes; stopping it", build_id)
        try:
            container.remove(force=True, v=True)
        except Exception:
            logger.exception("Could not stop the timed-out build %s", build_id)

    timeout = threading.Timer(_timeout_seconds(project), _on_timeout)
    timeout.daemon = True
    timeout.start()

    saw_phase = False
    try:
        for raw in container.logs(stream=True, follow=True):
            if _stop_requested(build_id):
                break
            line = raw.decode("utf-8", "replace").rstrip()
            if not line:
                continue
            logger.info("[%s] %s", build_id, line)
            if emit is not None:
                emit(line)
            match = _PHASE_COMPLETE_RE.search(line)
            if match:
                saw_phase = True
                _record_phase(build, match.group(1), match.group(2))

        if _stop_requested(build_id):
            logger.info("Build %s was stopped by request", build_id)
            _finish_build(build, "STOPPED")
        elif timed_out.is_set():
            _record_phase(build, build.get("currentPhase") or "BUILD", "TIMED_OUT")
            _finish_build(build, "TIMED_OUT")
        else:
            exit_code = container.wait().get("StatusCode", 1)
            if exit_code == 0 and not saw_phase:
                # The agent exits 0 even when it started nothing (e.g. it was
                # denied the Docker socket under SELinux), so a bare exit code
                # would report a build that never ran as SUCCEEDED. No "Phase
                # complete" line means no build phase executed: that is an
                # infrastructure failure, not a build result.
                logger.error(
                    "Build %s: agent exited 0 without completing any phase; "
                    "reporting FAULT", build_id,
                )
                _finish_build(build, "FAULT")
            else:
                _finish_build(build, "SUCCEEDED" if exit_code == 0 else "FAILED")
        logger.info("Build %s finished: %s", build_id, build["buildStatus"])
    except Exception:
        if _stop_requested(build_id):
            logger.info("Build %s was stopped by request", build_id)
            _finish_build(build, "STOPPED")
        elif timed_out.is_set():
            _record_phase(build, build.get("currentPhase") or "BUILD", "TIMED_OUT")
            _finish_build(build, "TIMED_OUT")
        else:
            logger.exception("Build %s failed while running", build_id)
            _finish_build(build, "FAULT")
    finally:
        timeout.cancel()
        _clear_stopped(build_id)
        _withdraw_build_session(build_id)
        if not saw_phase:
            logger.warning(
                "Build %s produced no 'Phase complete' lines; %s may not report "
                "phases in this format", build_id, AGENT_IMAGE
            )
        try:
            container.remove(force=True, v=True)
        except Exception:
            pass
        # After the agent: one stopped or timed out mid-build leaves its
        # project's containers running, and they hold the volumes.
        _remove_compose_project(client, compose_project)


# ---------------------------------------------------------------------------
# Request dispatcher
# ---------------------------------------------------------------------------

def _handle_request_sync(method, path, headers, body, query_params):
    target = headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "Invalid JSON", 400)

    handlers = {
        "CreateProject": _create_project,
        "BatchGetProjects": _batch_get_projects,
        "ListProjects": _list_projects,
        "UpdateProject": _update_project,
        "DeleteProject": _delete_project,
        "StartBuild": _start_build,
        "BatchGetBuilds": _batch_get_builds,
        "StopBuild": _stop_build,
        "ListBuilds": _list_builds,
        "ListBuildsForProject": _list_builds_for_project,
        "BatchDeleteBuilds": _batch_delete_builds,
        "CreateFleet": _create_fleet,
        "BatchGetFleets": _batch_get_fleets,
        "ListFleets": _list_fleets,
        "UpdateFleet": _update_fleet,
        "DeleteFleet": _delete_fleet,
    }

    handler = handlers.get(action)
    if not handler:
        return error_response_json("InvalidAction", f"Unknown action: {action}", 400)
    return handler(data)


# ---------------------------------------------------------------------------
# Project handlers
# ---------------------------------------------------------------------------

def _create_project(data):
    name = data.get("name", "")
    if not name:
        return error_response_json("InvalidInputException", "Project name is required", 400)
    if name in _projects:
        return error_response_json("ResourceAlreadyExistsException",
                                   f"Project already exists: {name}", 400)

    now = int(time.time())
    project = {
        "name": name,
        "arn": _project_arn(name),
        "description": data.get("description", ""),
        "source": data.get("source", {"type": "NO_SOURCE"}),
        "sourceVersion": data.get("sourceVersion", ""),
        "artifacts": data.get("artifacts", {"type": "NO_ARTIFACTS"}),
        "environment": data.get("environment", {
            "type": "LINUX_CONTAINER",
            "image": "aws/codebuild/standard:7.0",
            "computeType": "BUILD_GENERAL1_SMALL",
        }),
        "serviceRole": data.get("serviceRole",
                                f"arn:aws:iam::{get_account_id()}:role/codebuild-role"),
        "timeoutInMinutes": data.get("timeoutInMinutes", 60),
        "tags": data.get("tags", []),
        "created": now,
        "lastModified": now,
        "encryptionKey": data.get("encryptionKey",
                                  f"arn:aws:kms:{get_region()}:{get_account_id()}:alias/aws/codebuild"),
        "badge": {"badgeEnabled": False},
    }
    _projects[name] = project
    logger.info("CreateProject: %s", name)
    return json_response({"project": _project_shape(project)})


def _project_shape(project):
    """Return the project dict in the shape boto3 expects."""
    return {
        "name": project["name"],
        "arn": project["arn"],
        "description": project.get("description", ""),
        "source": project.get("source", {}),
        "sourceVersion": project.get("sourceVersion", ""),
        "artifacts": project.get("artifacts", {}),
        "environment": project.get("environment", {}),
        "serviceRole": project.get("serviceRole", ""),
        "timeoutInMinutes": project.get("timeoutInMinutes", 60),
        "tags": project.get("tags", []),
        "created": project["created"],
        "lastModified": project["lastModified"],
        "encryptionKey": project.get("encryptionKey", ""),
        "badge": project.get("badge", {}),
    }


def _batch_get_projects(data):
    names = data.get("names", [])
    found = []
    not_found = []
    for name in names:
        lookup = _project_name_from_identifier(name)
        project = _projects.get(lookup) if lookup else None
        if project:
            found.append(_project_shape(project))
        else:
            not_found.append(name)
    return json_response({"projects": found, "projectsNotFound": not_found})


def _list_projects(data):
    sort_by = data.get("sortBy", "NAME")
    sort_order = data.get("sortOrder", "ASCENDING")
    names = list(_projects.keys())
    if sort_by == "NAME":
        names.sort(reverse=(sort_order == "DESCENDING"))
    elif sort_by == "LAST_MODIFIED_TIME":
        names.sort(key=lambda n: _projects[n].get("lastModified", ""),
                   reverse=(sort_order == "DESCENDING"))
    return json_response({"projects": names})


def _update_project(data):
    name = data.get("name", "")
    if not name or name not in _projects:
        return error_response_json("ResourceNotFoundException",
                                   f"Project not found: {name}", 400)
    project = _projects[name]
    for key in ("description", "source", "sourceVersion", "artifacts",
                "environment", "serviceRole", "timeoutInMinutes", "tags",
                "encryptionKey"):
        if key in data:
            project[key] = data[key]
    project["lastModified"] = int(time.time())
    logger.info("UpdateProject: %s", name)
    return json_response({"project": _project_shape(project)})


def _delete_project(data):
    name = data.get("name", "")
    if not name or name not in _projects:
        return error_response_json("ResourceNotFoundException",
                                   f"Project not found: {name}", 400)
    del _projects[name]
    logger.info("DeleteProject: %s", name)
    return json_response({})


# ---------------------------------------------------------------------------
# Fleet handlers
#
# A reserved-capacity fleet is a record only: builds here run in a container
# or not at all, and a project's environment.fleet.fleetArn is stored as
# given. The record is what BatchGetFleets and ListFleets read back.
# ---------------------------------------------------------------------------

# The CreateFleet / UpdateFleet members besides name and tags, stored as given.
_FLEET_MEMBERS = (
    "baseCapacity", "environmentType", "computeType", "computeConfiguration",
    "scalingConfiguration", "overflowBehavior", "vpcConfig",
    "proxyConfiguration", "imageId", "fleetServiceRole",
)


def _create_fleet(data):
    name = data.get("name", "")
    if not name:
        return error_response_json("InvalidInputException", "Fleet name is required", 400)
    if _fleet_by_name(name) is not None:
        return error_response_json("ResourceAlreadyExistsException",
                                   f"Fleet already exists: {name}", 400)
    for member in ("baseCapacity", "environmentType", "computeType"):
        if data.get(member) in (None, ""):
            return error_response_json("InvalidInputException",
                                       f"{member} is required", 400)

    now = int(time.time())
    fleet_id = new_uuid()
    fleet = {
        "arn": _fleet_arn(name, fleet_id),
        "name": name,
        "id": fleet_id,
        "created": now,
        "lastModified": now,
        "status": {"statusCode": "ACTIVE"},
        "overflowBehavior": "QUEUE",
        "tags": data.get("tags", []),
    }
    for member in _FLEET_MEMBERS:
        if data.get(member) not in (None, ""):
            fleet[member] = data[member]
    fleet["baseCapacity"] = int(fleet["baseCapacity"])
    _fleets[fleet["arn"]] = fleet
    logger.info("CreateFleet: %s", name)
    return json_response({"fleet": copy.deepcopy(fleet)})


def _batch_get_fleets(data):
    found = []
    not_found = []
    for name in data.get("names", []):
        fleet = _fleet_from_identifier(name)
        if fleet is not None:
            found.append(copy.deepcopy(fleet))
        else:
            not_found.append(name)
    return json_response({"fleets": found, "fleetsNotFound": not_found})


def _list_fleets(data):
    sort_by = data.get("sortBy", "NAME")
    sort_order = data.get("sortOrder", "ASCENDING")
    fleets = list(_fleets.values())
    key = {
        "CREATED_TIME": lambda f: f.get("created", 0),
        "LAST_MODIFIED_TIME": lambda f: f.get("lastModified", 0),
    }.get(sort_by, lambda f: f.get("name", ""))
    fleets.sort(key=key, reverse=(sort_order == "DESCENDING"))
    return json_response({"fleets": [f["arn"] for f in fleets]})


def _update_fleet(data):
    arn = data.get("arn", "")
    fleet = _fleets.get(arn)
    if fleet is None:
        return error_response_json("ResourceNotFoundException",
                                   f"Fleet not found: {arn}", 400)
    for member in _FLEET_MEMBERS:
        if member in data:
            fleet[member] = data[member]
    if "tags" in data:
        fleet["tags"] = data["tags"]
    fleet["baseCapacity"] = int(fleet.get("baseCapacity", 1))
    fleet["lastModified"] = int(time.time())
    logger.info("UpdateFleet: %s", fleet["name"])
    return json_response({"fleet": copy.deepcopy(fleet)})


def _delete_fleet(data):
    arn = data.get("arn", "")
    if arn not in _fleets:
        return error_response_json("ResourceNotFoundException",
                                   f"Fleet not found: {arn}", 400)
    del _fleets[arn]
    logger.info("DeleteFleet: %s", arn)
    return json_response({})


# ---------------------------------------------------------------------------
# Build handlers
# ---------------------------------------------------------------------------

def _with_environment_overrides(project, overrides):
    """The project as one build runs it: each variable in StartBuild's
    environmentVariablesOverride replaces the declared variable of that name,
    or is added when none is declared."""
    if not overrides:
        return project
    effective = copy.deepcopy(project)
    env = effective.setdefault("environment", {})
    variables = {var.get("name"): var for var in env.get("environmentVariables") or []}
    for var in overrides:
        name = var.get("name")
        if name:
            variables[name] = {
                "name": name,
                "value": var.get("value", ""),
                "type": var.get("type", "PLAINTEXT"),
            }
    env["environmentVariables"] = list(variables.values())
    return effective


def _start_build(data):
    project_name = data.get("projectName", "")
    if not project_name or project_name not in _projects:
        return error_response_json("ResourceNotFoundException",
                                   f"Project not found: {project_name}", 400)
    project = _with_environment_overrides(
        _projects[project_name], data.get("environmentVariablesOverride"))
    bid = _build_id(project_name)
    build = _make_build_record(project, bid, data.get("sourceVersion"))

    if EXECUTE_BUILDS:
        now = build["startTime"]
        build.pop("endTime", None)
        build["buildStatus"] = "IN_PROGRESS"
        build["currentPhase"] = "SUBMITTED"
        build["phases"] = [
            {"phaseType": "SUBMITTED", "phaseStatus": "SUCCEEDED", "startTime": now, "endTime": now},
            {"phaseType": "QUEUED", "startTime": now},
        ]

    _builds[bid] = build
    logger.info("StartBuild: %s -> %s", project_name, bid)

    if EXECUTE_BUILDS:
        # The stores are scoped by the request's account/region contextvars, so
        # the worker has to run inside a copy of this request's context.
        context = contextvars.copy_context()
        threading.Thread(
            target=context.run,
            args=(_execute_build, bid, copy.deepcopy(project)),
            daemon=True,
        ).start()

    return json_response({"build": copy.deepcopy(build)})


def _batch_get_builds(data):
    ids = data.get("ids", [])
    found = []
    not_found = []
    for bid in ids:
        build = _builds.get(bid)
        if build:
            found.append(build)
        else:
            not_found.append(bid)
    return json_response({"builds": found, "buildsNotFound": not_found})


def _stop_build(data):
    bid = data.get("id", "")
    build = _builds.get(bid)
    if not build:
        return error_response_json("ResourceNotFoundException",
                                   f"Build not found: {bid}", 400)
    if EXECUTE_BUILDS:
        _mark_stopped(bid)

    container = _container_for_build(bid) if EXECUTE_BUILDS else None
    if container:
        try:
            container.remove(force=True, v=True)
        except Exception:
            logger.exception("Could not stop the build container for %s", bid)

    build["buildStatus"] = "STOPPED"
    build["endTime"] = int(time.time())
    build["currentPhase"] = "COMPLETED"
    logger.info("StopBuild: %s", bid)
    return json_response({"build": copy.deepcopy(build)})


def _list_builds(data):
    sort_order = data.get("sortOrder", "DESCENDING")
    ids = list(_builds.keys())
    ids.sort(key=lambda bid: _builds[bid].get("startTime", ""),
             reverse=(sort_order == "DESCENDING"))
    return json_response({"ids": ids})


def _list_builds_for_project(data):
    project_name = data.get("projectName", "")
    if not project_name or project_name not in _projects:
        return error_response_json("ResourceNotFoundException",
                                   f"Project not found: {project_name}", 400)
    sort_order = data.get("sortOrder", "DESCENDING")
    ids = [bid for bid, b in _builds.items() if b["projectName"] == project_name]
    ids.sort(key=lambda bid: _builds[bid].get("startTime", ""),
             reverse=(sort_order == "DESCENDING"))
    return json_response({"ids": ids})


def _batch_delete_builds(data):
    ids = data.get("ids", [])
    deleted = []
    not_deleted = []
    for bid in ids:
        if bid in _builds:
            del _builds[bid]
            deleted.append(bid)
        else:
            # BuildNotDeleted is a structure ({id, statusCode}), not a bare id —
            # SDK parsers crash on a plain string here. The AWS reference only
            # documents BUILD_IN_PROGRESS as an example statusCode; NOT_FOUND is
            # our value for an unknown id.
            not_deleted.append({"id": bid, "statusCode": "NOT_FOUND"})
    return json_response({"buildsDeleted": deleted, "buildsNotDeleted": not_deleted})


async def handle_request(method, path, headers, body, query_params):
    """Dispatch off the event loop.

    Request paths here reach the Docker daemon (container create/start/stop/
    inspect), which blocks for as long as the daemon takes. Measured on ECS: a
    cached-image container start held the loop for 7.3s, during which the health
    endpoint — the cheapest request in the process — could not be served.

    Uses run_reentrant, not the shared pool: containers started here are handed
    an endpoint pointing back at MiniStack, so one calls in while this
    dispatch is still running. A bounded pool would queue that nested request
    behind the call waiting on it.
    """
    return await run_reentrant(
        _handle_request_sync, method, path, headers, body, query_params, thread_name="ministack-codebuild-dispatch")
