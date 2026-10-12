import json
import os
import threading
import time
import uuid

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

from ministack.services import codebuild

# ========== CodeBuild ==========


def _client(region):
    return boto3.client(
        "codebuild",
        endpoint_url=os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566"),
        region_name=region,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=Config(retries={"mode": "standard"}),
    )


def _project_args(name, description):
    return {
        "name": name,
        "description": description,
        "source": {"type": "NO_SOURCE"},
        "artifacts": {"type": "NO_ARTIFACTS"},
        "environment": {
            "type": "LINUX_CONTAINER",
            "image": "aws/codebuild/standard:7.0",
            "computeType": "BUILD_GENERAL1_SMALL",
        },
        "serviceRole": "arn:aws:iam::000000000000:role/codebuild-role",
    }


def _ensure_codebuild_project(codebuild, name):
    if codebuild.batch_get_projects(names=[name])["projects"]:
        return
    codebuild.create_project(
        name=name,
        source={"type": "NO_SOURCE"},
        artifacts={"type": "NO_ARTIFACTS"},
        environment={
            "type": "LINUX_CONTAINER",
            "image": "aws/codebuild/standard:7.0",
            "computeType": "BUILD_GENERAL1_SMALL",
        },
        serviceRole="arn:aws:iam::000000000000:role/codebuild-role",
    )


def test_codebuild_create_project(codebuild):
    resp = codebuild.create_project(
        name="test-project",
        source={"type": "NO_SOURCE", "buildspec": "version: 0.2\nphases:\n  build:\n    commands:\n      - echo Hello"},
        artifacts={"type": "NO_ARTIFACTS"},
        environment={
            "type": "LINUX_CONTAINER",
            "image": "aws/codebuild/standard:7.0",
            "computeType": "BUILD_GENERAL1_SMALL",
        },
        serviceRole="arn:aws:iam::000000000000:role/codebuild-role",
    )
    project = resp["project"]
    assert project["name"] == "test-project"
    assert project["arn"].startswith("arn:aws:codebuild:")
    assert "created" in project


def test_codebuild_create_duplicate_project(codebuild):
    with pytest.raises(ClientError) as exc:
        codebuild.create_project(
            name="test-project",
            source={"type": "NO_SOURCE"},
            artifacts={"type": "NO_ARTIFACTS"},
            environment={"type": "LINUX_CONTAINER", "image": "aws/codebuild/standard:7.0", "computeType": "BUILD_GENERAL1_SMALL"},
            serviceRole="arn:aws:iam::000000000000:role/codebuild-role",
        )
    assert "ResourceAlreadyExistsException" in str(exc.value)


def test_codebuild_batch_get_projects(codebuild):
    resp = codebuild.batch_get_projects(names=["test-project", "nonexistent"])
    assert len(resp["projects"]) == 1
    assert resp["projects"][0]["name"] == "test-project"
    assert "nonexistent" in resp["projectsNotFound"]


def test_codebuild_batch_get_projects_by_arn(codebuild):
    arn = codebuild.batch_get_projects(names=["test-project"])["projects"][0]["arn"]
    resp = codebuild.batch_get_projects(names=[arn])
    assert len(resp["projects"]) == 1
    assert resp["projects"][0]["name"] == "test-project"
    assert resp["projectsNotFound"] == []


@pytest.mark.parametrize(
    "identifier_template",
    [
        "arn:aws:codebuild:us-east-1:000000000000:build/{name}",
        "arn:aws:codebuild:project/{name}",
        "arn:aws-us-gov:codebuild:us-east-1:000000000000:project/{name}",
        "arn:aws:lambda:us-east-1:000000000000:project/{name}",
        "arn:aws:codebuild:us-west-2:000000000000:project/{name}",
        "arn:aws:codebuild:us-east-1:111111111111:project/{name}",
    ],
)
def test_codebuild_batch_get_projects_does_not_tail_resolve_out_of_scope_arns(
    codebuild,
    identifier_template,
):
    name = "arn-parser-project"
    _ensure_codebuild_project(codebuild, name)

    identifier = identifier_template.format(name=name)
    resp = codebuild.batch_get_projects(names=[identifier])
    assert resp["projects"] == []
    assert resp["projectsNotFound"] == [identifier]


def test_codebuild_list_projects(codebuild):
    resp = codebuild.list_projects()
    assert "test-project" in resp["projects"]


def test_codebuild_update_project(codebuild):
    resp = codebuild.update_project(
        name="test-project",
        description="updated description",
    )
    assert resp["project"]["description"] == "updated description"


def test_codebuild_start_build(codebuild):
    resp = codebuild.start_build(projectName="test-project")
    build = resp["build"]
    assert build["projectName"] == "test-project"
    assert build["buildStatus"] == "SUCCEEDED"
    assert build["arn"].startswith("arn:aws:codebuild:")
    assert "phases" in build


def _build_state_changes(sqs, queue_url, build_arn, until, timeout=20):
    """``(build-status, detail)`` for each CodeBuild Build State Change the
    queue receives for ``build_arn`` until one at ``until`` has arrived."""
    got = []
    deadline = time.time() + timeout
    while time.time() < deadline and until not in [status for status, _detail in got]:
        for msg in sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10,
                                       WaitTimeSeconds=1).get("Messages", []):
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])
            event = json.loads(msg["Body"])
            if event["resources"] == [build_arn]:
                assert event["source"] == "aws.codebuild"
                assert event["detail-type"] == "CodeBuild Build State Change"
                got.append((event["detail"]["build-status"], event))
    return got


def test_codebuild_build_state_changes_reach_the_default_bus(codebuild, sqs, eb):
    """A started build puts a CodeBuild Build State Change on the default bus
    as it starts and as it ends, naming its project, its build ARN and the
    variables it ran with, so a rule on aws.codebuild learns the build ended."""
    suffix = uuid.uuid4().hex[:8]
    project = f"cb-state-{suffix}"
    rule = f"cb-state-{suffix}"
    queue_url = sqs.create_queue(QueueName=f"cb-state-{suffix}")["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    eb.put_rule(Name=rule, EventPattern=json.dumps({
        "source": ["aws.codebuild"], "detail-type": ["CodeBuild Build State Change"]}))
    eb.put_targets(Rule=rule, Targets=[{"Id": "q", "Arn": queue_arn}])
    codebuild.create_project(**_project_args(project, "state changes"))
    try:
        build = codebuild.start_build(
            projectName=project,
            environmentVariablesOverride=[{"name": "RUN", "value": suffix, "type": "PLAINTEXT"}],
        )["build"]

        changes = _build_state_changes(sqs, queue_url, build["arn"], "SUCCEEDED")

        assert [status for status, _event in changes] == ["IN_PROGRESS", "SUCCEEDED"]
        ended = changes[-1][1]
        assert ended["account"] == build["arn"].split(":")[4]
        assert ended["region"] == build["arn"].split(":")[3]
        detail = ended["detail"]
        assert detail["project-name"] == project
        assert detail["build-id"] == build["arn"]
        information = detail["additional-information"]
        assert information["build-complete"] is True
        assert {"name": "RUN", "type": "PLAINTEXT", "value": suffix} in (
            information["environment"]["environment-variables"])
        assert information["phases"][-1] == {
            "phase-type": "COMPLETED", "start-time": information["phases"][-1]["start-time"]}
        assert changes[0][1]["detail"]["additional-information"]["build-complete"] is False
    finally:
        eb.remove_targets(Rule=rule, Ids=["q"])
        eb.delete_rule(Name=rule)
        sqs.delete_queue(QueueUrl=queue_url)
        codebuild.delete_project(name=project)


def test_codebuild_batch_get_builds(codebuild):
    start_resp = codebuild.start_build(projectName="test-project")
    build_id = start_resp["build"]["id"]
    resp = codebuild.batch_get_builds(ids=[build_id, "nonexistent:fake"])
    assert len(resp["builds"]) == 1
    assert resp["builds"][0]["id"] == build_id
    assert "nonexistent:fake" in resp["buildsNotFound"]


def test_codebuild_list_builds_for_project(codebuild):
    resp = codebuild.list_builds_for_project(projectName="test-project")
    assert len(resp["ids"]) >= 1


def test_codebuild_list_builds(codebuild):
    resp = codebuild.list_builds()
    assert len(resp["ids"]) >= 1


def test_codebuild_stop_build(codebuild):
    start_resp = codebuild.start_build(projectName="test-project")
    build_id = start_resp["build"]["id"]
    resp = codebuild.stop_build(id=build_id)
    assert resp["build"]["buildStatus"] == "STOPPED"


def test_codebuild_batch_delete_builds(codebuild):
    start_resp = codebuild.start_build(projectName="test-project")
    build_id = start_resp["build"]["id"]
    resp = codebuild.batch_delete_builds(ids=[build_id])
    assert build_id in resp["buildsDeleted"]


def test_codebuild_batch_delete_builds_not_deleted_shape(codebuild):
    """An id that matches no build comes back as a BuildNotDeleted structure
    ({id, statusCode}), not a bare string — SDK parsers crash on the string."""
    resp = codebuild.batch_delete_builds(ids=["test-project:00000000-0000-0000-0000-000000000000"])
    assert resp["buildsDeleted"] == []
    entry = resp["buildsNotDeleted"][0]
    assert entry["id"] == "test-project:00000000-0000-0000-0000-000000000000"
    assert entry["statusCode"]


def test_codebuild_delete_project(codebuild):
    codebuild.delete_project(name="test-project")
    resp = codebuild.list_projects()
    assert "test-project" not in resp["projects"]


def test_codebuild_delete_nonexistent_project(codebuild):
    with pytest.raises(ClientError) as exc:
        codebuild.delete_project(name="nonexistent")
    assert "ResourceNotFoundException" in str(exc.value)


def test_codebuild_projects_and_builds_are_region_scoped():
    east = _client("us-east-1")
    west = _client("us-west-2")
    name = "same-name-regional-project"

    east.create_project(**_project_args(name, "east"))
    west.create_project(**_project_args(name, "west"))
    try:
        east_project = east.batch_get_projects(names=[name])["projects"][0]
        west_project = west.batch_get_projects(names=[name])["projects"][0]
        assert east_project["description"] == "east"
        assert west_project["description"] == "west"
        assert ":us-east-1:" in east_project["arn"]
        assert ":us-west-2:" in west_project["arn"]
        assert name in east.list_projects()["projects"]
        assert name in west.list_projects()["projects"]

        east_build = east.start_build(projectName=name)["build"]
        west_build = west.start_build(projectName=name)["build"]
        assert ":us-east-1:" in east_build["arn"]
        assert ":us-west-2:" in west_build["arn"]
        assert east_build["id"] in east.list_builds()["ids"]
        assert east_build["id"] not in west.list_builds()["ids"]
        assert west_build["id"] in west.list_builds()["ids"]
        assert west_build["id"] not in east.list_builds()["ids"]
    finally:
        east.delete_project(name=name)
        west.delete_project(name=name)


def test_codebuild_restore_legacy_state_uses_resource_arn_region():
    from ministack.core.responses import (
        AccountScopedDict,
        set_request_account_id,
        set_request_region,
    )
    from ministack.services import codebuild as service

    account_id = "111111111111"
    resource_region = "us-west-2"
    boot_region = "us-east-1"
    project_name = "legacy-project"
    build_id = f"{project_name}:legacy-build"
    projects = AccountScopedDict()
    builds = AccountScopedDict()

    set_request_account_id(account_id)
    set_request_region(boot_region)
    projects[project_name] = {
        "name": project_name,
        "arn": (
            f"arn:aws:codebuild:{resource_region}:{account_id}:"
            f"project/{project_name}"
        ),
    }
    builds[build_id] = {
        "id": build_id,
        "arn": (
            f"arn:aws:codebuild:{resource_region}:{account_id}:build/{build_id}"
        ),
    }

    service.reset()
    try:
        service.load_persisted_state({"projects": projects, "builds": builds})
        assert service._projects.get_scoped(
            account_id, resource_region, project_name
        )["name"] == project_name
        assert service._builds.get_scoped(
            account_id, resource_region, build_id
        )["id"] == build_id
        assert service._projects.get_scoped(
            account_id, boot_region, project_name
        ) is None
        assert service._builds.get_scoped(
            account_id, boot_region, build_id
        ) is None
    finally:
        service.reset()


# ========== CodeBuild execution (MINISTACK_CODEBUILD_EXECUTE=1) ==========
#
# Execution hands the buildspec to the official CodeBuild local agent, so the
# Docker client is faked here: these cover the build bookkeeping around the
# agent (phase records, status mapping, the metadata-only default), not
# Docker itself.


class _FakeContainer:
    def __init__(self, log_lines, exit_code=0):
        self._log_lines = log_lines
        self._exit_code = exit_code
        self.removed = False

    def logs(self, **_kwargs):
        return iter(line.encode("utf-8") for line in self._log_lines)

    def wait(self):
        return {"StatusCode": self._exit_code}

    def remove(self, **_kwargs):
        self.removed = True


class _FakeContainers:
    def __init__(self, container):
        self._container = container
        self.image = None
        self.kwargs = None

    def run(self, image, **kwargs):
        self.image = image
        self.kwargs = kwargs
        return self._container


class _FakeDocker:
    def __init__(self, container):
        self.containers = _FakeContainers(container)


EXECUTION_BUILDSPEC = "version: 0.2\nphases:\n  build:\n    commands:\n      - echo hi\n"


def _execution_project(buildspec=EXECUTION_BUILDSPEC):
    # A project of the test host's own architecture, which an executed build
    # runs on; one of the other is refused (below).
    return {
        "name": "demo",
        "arn": "arn:aws:codebuild:us-east-1:000000000000:project/demo",
        "source": {"type": "NO_SOURCE", "buildspec": buildspec},
        "artifacts": {"type": "NO_ARTIFACTS"},
        "environment": {
            "type": "ARM_CONTAINER" if codebuild.HOST_ARCHITECTURE == "arm64" else "LINUX_CONTAINER",
            "image": "aws/codebuild/standard:7.0",
            "computeType": "BUILD_GENERAL1_SMALL",
            "privilegedMode": True,
            "environmentVariables": [{"name": "FOO", "value": "bar"}],
        },
        "serviceRole": "arn:aws:iam::000000000000:role/codebuild-role",
    }


def _seed_execution_build(project, build_id="demo:0001"):
    build = codebuild._make_build_record(project, build_id)
    codebuild._builds[build_id] = build
    return build


# The CodeBuild agent of the emulator host's own architecture, pinned by the
# digest AWS publishes for each ("Run builds locally with the AWS CodeBuild
# agent"); a build of the other architecture is refused, never emulated.
AWS_AGENT_DIGESTS = {
    "x86_64": "sha256:ccb19bdd7af94e4dc761e4c58c267e9455c28ec68d938086b4dc1cf8fe6b0940",
    "arm64": "sha256:7d7b5d35d2ac4e062ae7ba8c662ffed15229a52d09bd0d664a7816c439679192",
}


@pytest.mark.parametrize("machine, architecture", [
    ("aarch64", "arm64"),
    ("arm64", "arm64"),
    ("x86_64", "x86_64"),
    ("AMD64", "x86_64"),
])
def test_the_host_architecture_is_its_machines(machine, architecture):
    assert codebuild._host_architecture(machine) == architecture


@pytest.mark.parametrize("architecture", ["arm64", "x86_64"])
def test_the_agent_of_each_architecture_is_pinned_by_aws_published_digest(architecture):
    image = codebuild.AGENT_IMAGES[architecture]
    assert image == f"public.ecr.aws/codebuild/local-builds@{AWS_AGENT_DIGESTS[architecture]}"


def _on_host(monkeypatch, architecture):
    """This emulator as a host of `architecture` with execution on, capturing
    the builds it would hand the agent."""
    executed = []
    monkeypatch.setattr(codebuild, "EXECUTE_BUILDS", True)
    monkeypatch.setattr(codebuild, "HOST_ARCHITECTURE", architecture)
    monkeypatch.setattr(codebuild, "AGENT_IMAGE", codebuild.AGENT_IMAGES.get(architecture))
    monkeypatch.setattr(codebuild, "_execute_build", lambda bid, project: executed.append(bid))
    return executed


def _project_on(environment_type):
    project = _execution_project()
    project["environment"]["type"] = environment_type
    codebuild._projects["demo"] = project
    return project


@pytest.mark.parametrize("host, environment_type", [
    ("arm64", "ARM_CONTAINER"),
    ("x86_64", "LINUX_CONTAINER"),
])
def test_start_build_runs_a_project_of_the_hosts_architecture(monkeypatch, host, environment_type):
    executed = _on_host(monkeypatch, host)
    _project_on(environment_type)

    status, _headers, body = codebuild._start_build({"projectName": "demo"})

    assert status == 200
    build_id = json.loads(body)["build"]["id"]
    deadline = time.time() + 5
    while not executed and time.time() < deadline:
        time.sleep(0.01)
    assert executed == [build_id]


@pytest.mark.parametrize("host, environment_type, other", [
    ("arm64", "LINUX_CONTAINER", "x86_64"),
    ("x86_64", "ARM_CONTAINER", "arm64"),
])
def test_start_build_refuses_a_project_of_the_other_architecture_naming_both(
        monkeypatch, host, environment_type, other):
    executed = _on_host(monkeypatch, host)
    _project_on(environment_type)
    before = set(codebuild._builds.keys())

    status, _headers, body = codebuild._start_build({"projectName": "demo"})

    assert status == 400
    error = json.loads(body)
    assert error["__type"] == "InvalidInputException"
    assert other in error["message"] and host in error["message"]
    assert set(codebuild._builds.keys()) == before
    assert executed == []


def test_start_build_refuses_an_override_of_the_other_architecture(monkeypatch):
    executed = _on_host(monkeypatch, "arm64")
    _project_on("ARM_CONTAINER")

    status, _headers, body = codebuild._start_build({
        "projectName": "demo", "environmentTypeOverride": "LINUX_CONTAINER",
    })

    assert status == 400
    assert "x86_64" in json.loads(body)["message"]
    assert executed == []


def test_start_build_that_is_not_executed_runs_no_agent_and_is_refused_nothing(monkeypatch):
    monkeypatch.setattr(codebuild, "EXECUTE_BUILDS", False)
    monkeypatch.setattr(codebuild, "HOST_ARCHITECTURE", "arm64")
    _project_on("LINUX_CONTAINER")

    status, _headers, _body = codebuild._start_build({"projectName": "demo"})

    assert status == 200


def test_start_build_is_metadata_only_by_default(monkeypatch):
    """Execution is opt-in: without the flag no container is ever created."""
    calls = []
    monkeypatch.setattr(codebuild, "_get_docker", lambda: calls.append(1))
    monkeypatch.setattr(codebuild, "EXECUTE_BUILDS", False)

    project = _execution_project()
    codebuild._projects["demo"] = project

    status, _headers, body = codebuild._start_build({"projectName": "demo"})

    assert status == 200
    assert json.loads(body)["build"]["buildStatus"] == "SUCCEEDED"
    assert calls == []


def test_start_build_overrides_reach_the_build(monkeypatch):
    """environmentVariablesOverride replaces a declared variable and adds a new
    one, for this build only: the record reads them and the executed build is
    handed them, while the project keeps its declared values."""
    started = threading.Event()
    executed = []

    def _capture(build_id, project):
        executed.append(project)
        started.set()

    monkeypatch.setattr(codebuild, "EXECUTE_BUILDS", True)
    monkeypatch.setattr(codebuild, "_execute_build", _capture)
    codebuild._projects["demo"] = _execution_project()

    status, _headers, body = codebuild._start_build({
        "projectName": "demo",
        "environmentVariablesOverride": [
            {"name": "FOO", "value": "overridden", "type": "PLAINTEXT"},
            {"name": "EXTRA", "value": "added", "type": "PLAINTEXT"},
        ],
    })

    assert status == 200
    expected = {"FOO": "overridden", "EXTRA": "added"}
    recorded = json.loads(body)["build"]["environment"]["environmentVariables"]
    assert {v["name"]: v["value"] for v in recorded} == expected
    assert started.wait(5)
    handed = executed[0]["environment"]["environmentVariables"]
    assert {v["name"]: v["value"] for v in handed} == expected
    declared = codebuild._projects["demo"]["environment"]["environmentVariables"]
    assert declared == [{"name": "FOO", "value": "bar"}]


def test_execute_build_records_phases_from_agent_log(monkeypatch, tmp_path):
    container = _FakeContainer([
        "Phase complete: INSTALL State: SUCCEEDED",
        "[Container] running command echo hi",
        "Phase complete: BUILD State: SUCCEEDED",
    ])
    docker = _FakeDocker(container)
    monkeypatch.setattr(codebuild, "_get_docker", lambda: docker)
    monkeypatch.setattr(codebuild, "WORKSPACE", str(tmp_path))

    project = _execution_project()
    build = _seed_execution_build(project)

    codebuild._execute_build("demo:0001", project)

    assert build["buildStatus"] == "SUCCEEDED"
    assert build["currentPhase"] == "COMPLETED"

    phases = {p["phaseType"]: p.get("phaseStatus") for p in build["phases"]}
    assert phases["INSTALL"] == "SUCCEEDED"
    assert phases["BUILD"] == "SUCCEEDED"
    assert phases["COMPLETED"] == "SUCCEEDED"
    assert all("endTime" in p for p in build["phases"])

    # The build runs in the project's image; the agent image only orchestrates.
    assert docker.containers.image == codebuild.AGENT_IMAGE
    assert docker.containers.kwargs["environment"]["IMAGE_NAME"] == "aws/codebuild/standard:7.0"
    assert docker.containers.kwargs["environment"]["DOCKER_PRIVILEGED_MODE"] == "true"
    assert container.removed is True

    buildspec_path = docker.containers.kwargs["environment"]["BUILDSPEC"]
    assert os.path.isfile(buildspec_path)
    with open(buildspec_path, encoding="utf-8") as fh:
        assert fh.read() == EXECUTION_BUILDSPEC

    env_file = os.path.join(str(tmp_path), "demo_0001", "env", "env.list")
    with open(env_file, encoding="utf-8") as fh:
        env_lines = fh.read().splitlines()
    assert "FOO=bar" in env_lines
    assert "CODEBUILD_BUILD_ID=demo:0001" in env_lines


def test_execute_build_writes_agent_output_to_cloudwatch_logs(monkeypatch, tmp_path):
    from ministack.services import cloudwatch_logs as cwl

    container = _FakeContainer([
        "[Container] running command echo hi",
        "Phase complete: BUILD State: SUCCEEDED",
    ])
    monkeypatch.setattr(codebuild, "_get_docker", lambda: _FakeDocker(container))
    monkeypatch.setattr(codebuild, "WORKSPACE", str(tmp_path))

    project = _execution_project()
    build = _seed_execution_build(project, "demo:0005")

    codebuild._execute_build("demo:0005", project)

    group = cwl._log_groups[build["logs"]["groupName"]]
    stream = group["streams"][build["logs"]["streamName"]]
    messages = [event["message"] for event in stream["events"]]

    assert "[Container] running command echo hi" in messages
    assert "Phase complete: BUILD State: SUCCEEDED" in messages
    assert stream["firstEventTimestamp"] is not None
    assert stream["lastEventTimestamp"] >= stream["firstEventTimestamp"]


def test_execute_build_reports_failed_exit_code(monkeypatch, tmp_path):
    container = _FakeContainer(["Phase complete: BUILD State: FAILED"], exit_code=1)
    monkeypatch.setattr(codebuild, "_get_docker", lambda: _FakeDocker(container))
    monkeypatch.setattr(codebuild, "WORKSPACE", str(tmp_path))

    project = _execution_project()
    build = _seed_execution_build(project, "demo:0002")

    codebuild._execute_build("demo:0002", project)

    assert build["buildStatus"] == "FAILED"
    phases = {p["phaseType"]: p.get("phaseStatus") for p in build["phases"]}
    assert phases["BUILD"] == "FAILED"


def test_execute_build_without_buildspec_never_starts_agent(monkeypatch, tmp_path):
    docker = _FakeDocker(_FakeContainer([]))
    monkeypatch.setattr(codebuild, "_get_docker", lambda: docker)
    monkeypatch.setattr(codebuild, "WORKSPACE", str(tmp_path))

    project = _execution_project(buildspec="")
    build = _seed_execution_build(project, "demo:0003")

    codebuild._execute_build("demo:0003", project)

    assert build["buildStatus"] == "FAILED"
    assert docker.containers.image is None


def test_execute_build_times_out_per_project_timeout(monkeypatch, tmp_path):
    """A build past timeoutInMinutes is stopped and reported, not left running."""
    class _HangingContainer(_FakeContainer):
        def logs(self, **_kwargs):
            # Ends only once the timeout fired and "removed" the container.
            while not self.removed:
                time.sleep(0.01)
            return iter(())

    container = _HangingContainer([])
    monkeypatch.setattr(codebuild, "_get_docker", lambda: _FakeDocker(container))
    monkeypatch.setattr(codebuild, "WORKSPACE", str(tmp_path))
    monkeypatch.setattr(codebuild, "_timeout_seconds", lambda _project: 0.2)

    project = _execution_project()
    build = _seed_execution_build(project, "demo:0006")

    codebuild._execute_build("demo:0006", project)

    assert build["buildStatus"] == "TIMED_OUT"
    assert "TIMED_OUT" in [p.get("phaseStatus") for p in build["phases"]]


def test_timeout_seconds_uses_the_project_value(monkeypatch):
    assert codebuild._timeout_seconds({"timeoutInMinutes": 5}) == 300
    assert codebuild._timeout_seconds({}) == 3600
    assert codebuild._timeout_seconds({"timeoutInMinutes": "bogus"}) == 3600


def test_env_file_points_the_build_at_ministack(monkeypatch, tmp_path):
    """A build's AWS calls should reach this emulator, not real AWS."""
    monkeypatch.setattr(codebuild, "_aws_endpoint", lambda: "http://172.17.0.1:4566")

    project = _execution_project()
    build = _seed_execution_build(project, "demo:0007")
    env_file = tmp_path / "env.list"

    codebuild._write_env_file(str(env_file), project, build)

    lines = env_file.read_text(encoding="utf-8").splitlines()
    assert "AWS_ENDPOINT_URL=http://172.17.0.1:4566" in lines


def _env(env_file):
    return dict(line.split("=", 1) for line in env_file.read_text(encoding="utf-8").splitlines())


def _get_credentials(uri):
    """GET the build's credential URI through the app's own routing."""
    import asyncio

    import ministack.app as app_mod

    path = uri.split("4566", 1)[1]
    status, _headers, body = asyncio.run(
        app_mod._handle_pre_body_request("GET", path, {}, {}, "req-1"))
    return status, json.loads(body)


def test_env_file_hands_the_build_its_role_session(monkeypatch, tmp_path):
    """The build reads its project role's session where CodeBuild serves it,
    and the SDK's own variables carry the same session, never a static key."""
    from ministack.core.iam_evaluator import resolve_caller_identity

    monkeypatch.setattr(codebuild, "_aws_endpoint", lambda: "http://172.17.0.1:4566")
    project = _execution_project()
    build = _seed_execution_build(project, "demo:0009")
    env_file = tmp_path / "env.list"

    codebuild._write_env_file(str(env_file), project, build)

    env = _env(env_file)
    uri = env["AWS_CONTAINER_CREDENTIALS_FULL_URI"]
    assert uri.startswith("http://172.17.0.1:4566/_ministack/codebuild/credentials/")
    assert env["AWS_ACCESS_KEY_ID"].startswith("ASIA")
    assert env["AWS_ACCESS_KEY_ID"] != "test"
    assert env["AWS_SESSION_TOKEN"]

    status, session = _get_credentials(uri)
    assert status == 200
    assert session["AccessKeyId"] == env["AWS_ACCESS_KEY_ID"]
    assert session["SecretAccessKey"] == env["AWS_SECRET_ACCESS_KEY"]
    assert session["Token"] == env["AWS_SESSION_TOKEN"]
    assert session["Expiration"].endswith("Z")

    caller = resolve_caller_identity(session["AccessKeyId"])
    assert caller["userArn"] == (
        "arn:aws:sts::000000000000:assumed-role/codebuild-role/AWSCodeBuild-0009")


def test_a_finished_build_no_longer_answers_its_session(monkeypatch, tmp_path):
    container = _FakeContainer(["Phase complete: BUILD State: SUCCEEDED"])
    monkeypatch.setattr(codebuild, "_get_docker", lambda: _FakeDocker(container))
    monkeypatch.setattr(codebuild, "_aws_endpoint", lambda: "http://172.17.0.1:4566")
    monkeypatch.setattr(codebuild, "WORKSPACE", str(tmp_path))
    project = _execution_project()
    _seed_execution_build(project, "demo:0011")

    codebuild._execute_build("demo:0011", project)

    env = _env(tmp_path / "demo_0011" / "env" / "env.list")
    status, _answer = _get_credentials(env["AWS_CONTAINER_CREDENTIALS_FULL_URI"])
    assert status == 404


# The agent's final compose file, as its edit-docker-compose writes it: the
# build service first, the agent service after it.
_AGENT_COMPOSE = """services:
  build:
    image: ${IMAGE_FOR_CODEBUILD_LOCAL_BUILD}
    privileged: ${BUILD_CONTAINER_PRIVILEGED_MODE}
  agent:
    image: ${LOCAL_AGENT_IMAGE}
"""


def _gnu_sed():
    import subprocess

    try:
        return "GNU" in subprocess.run(
            ["sed", "--version"], capture_output=True, text=True).stdout
    except OSError:
        return False


def test_the_build_service_reaches_the_host_by_name(monkeypatch, tmp_path):
    """The agent runs an executable docker-compose ahead of its own that maps
    host.docker.internal to the host gateway for the build service alone,
    once, in the file the agent last wrote, and then runs the agent's own."""
    import subprocess

    container = _FakeContainer(["Phase complete: BUILD State: SUCCEEDED"])
    docker = _FakeDocker(container)
    monkeypatch.setattr(codebuild, "_get_docker", lambda: docker)
    monkeypatch.setattr(codebuild, "WORKSPACE", str(tmp_path))
    project = _execution_project()
    _seed_execution_build(project, "demo:0012")

    codebuild._execute_build("demo:0012", project)

    mounted = {bind["bind"]: source for source, bind in docker.containers.kwargs["volumes"].items()}
    wrapper = mounted["/usr/local/sbin/docker-compose"]
    assert os.access(wrapper, os.X_OK)
    if not _gnu_sed():
        pytest.skip("the wrapper runs in the agent's image, whose sed is GNU sed")

    compose = tmp_path / "customer-specific.yml"
    compose.write_text(_AGENT_COMPOSE, encoding="utf-8")
    script = (tmp_path / "wrapper.sh")
    script.write_text(
        open(wrapper, encoding="utf-8").read()
        .replace("/LocalBuild/agent-resources/customer-specific.yml", str(compose))
        .replace("exec /usr/local/bin/docker-compose", "exec echo ran"),
        encoding="utf-8")
    for _ in range(2):
        ran = subprocess.run(["sh", str(script), "up"], capture_output=True, text=True)
        assert ran.stdout.strip() == "ran up"

    assert compose.read_text(encoding="utf-8") == _AGENT_COMPOSE.replace(
        "  build:\n",
        '  build:\n    extra_hosts:\n      - "host.docker.internal:host-gateway"\n')


class _ComposeDaemon:
    """The daemon an agent's docker-compose leaves resources on, each labelled
    with its compose project the way compose labels them."""

    class _Resource:
        def __init__(self, daemon, kind, name, project):
            self.daemon, self.kind, self.name = daemon, kind, name
            self.labels = {"com.docker.compose.project": project}

        def remove(self, **_kwargs):
            self.daemon.resources.remove(self)

    class _Collection:
        def __init__(self, daemon, kind):
            self.daemon, self.kind = daemon, kind

        def list(self, all=False, filters=None):
            key, _, value = filters["label"].partition("=")
            return [r for r in self.daemon.resources
                    if r.kind == self.kind and r.labels.get(key) == value]

    def __init__(self, agent):
        self.agent = agent
        self.resources = []
        self.containers = self._Collection(self, "container")
        self.containers.run = self._run_agent
        self.networks = self._Collection(self, "network")
        self.volumes = self._Collection(self, "volume")

    def compose_up(self, project):
        for kind, name in (("container", f"{project}-agent-1"), ("container", f"{project}-build-1"),
                           ("network", f"{project}_default"), ("volume", f"{project}_source_volume"),
                           ("volume", f"{project}_user_volume")):
            self.resources.append(self._Resource(self, kind, name, project))

    def _run_agent(self, image, **kwargs):
        self.compose_up(kwargs["environment"]["COMPOSE_PROJECT_NAME"])
        return self.agent

    def names(self):
        return sorted(r.name for r in self.resources)


def test_a_build_removes_its_compose_project_when_it_ends(monkeypatch, tmp_path):
    """The agent brings its docker-compose project up and never takes it down.
    The build runs it as a project of its own, and its end removes that
    project's containers, network and volumes — and nothing of another build's."""
    agent = _FakeContainer(["Phase complete: BUILD State: SUCCEEDED"])
    docker = _ComposeDaemon(agent)
    other = codebuild._compose_project("demo:0099")
    docker.compose_up(other)
    monkeypatch.setattr(codebuild, "_get_docker", lambda: docker)
    monkeypatch.setattr(codebuild, "WORKSPACE", str(tmp_path))
    project = _execution_project()
    build = _seed_execution_build(project, "demo:0013")
    # The phases an executed build starts with, as StartBuild records them.
    build["phases"] = [
        {"phaseType": "SUBMITTED", "phaseStatus": "SUCCEEDED",
         "startTime": build["startTime"], "endTime": build["startTime"]},
        {"phaseType": "QUEUED", "startTime": build["startTime"]},
    ]

    codebuild._execute_build("demo:0013", project)

    assert build["buildStatus"] == "SUCCEEDED"
    assert agent.removed
    assert docker.names() == sorted([
        f"{other}-agent-1", f"{other}-build-1", f"{other}_default",
        f"{other}_source_volume", f"{other}_user_volume",
    ])


def test_env_file_keeps_an_endpoint_the_project_declared(monkeypatch, tmp_path):
    monkeypatch.setattr(codebuild, "_aws_endpoint", lambda: "http://172.17.0.1:4566")

    project = _execution_project()
    project["environment"]["environmentVariables"].append(
        {"name": "AWS_ENDPOINT_URL", "value": "https://real.aws"}
    )
    build = _seed_execution_build(project, "demo:0008")
    env_file = tmp_path / "env.list"

    codebuild._write_env_file(str(env_file), project, build)

    lines = env_file.read_text(encoding="utf-8").splitlines()
    assert "AWS_ENDPOINT_URL=https://real.aws" in lines
    assert "AWS_ENDPOINT_URL=http://172.17.0.1:4566" not in lines


def test_restored_in_flight_builds_are_not_left_running():
    """A build cannot survive a restart, so it must not restore as IN_PROGRESS."""
    codebuild.load_persisted_state({
        "projects": {},
        "builds": {"demo:0010": {
            "id": "demo:0010",
            "projectName": "demo",
            "buildStatus": "IN_PROGRESS",
            "currentPhase": "BUILD",
        }},
    })

    restored = codebuild._builds["demo:0010"]
    assert restored["buildStatus"] == "FAULT"
    assert restored["currentPhase"] == "COMPLETED"
    assert "endTime" in restored


def test_stopped_build_is_not_overwritten_by_the_worker(monkeypatch, tmp_path):
    """StopBuild removes the container, which is not a build failure.

    Without this the worker's `container.wait()` raises on the removed container
    and reports FAULT over the user's STOPPED.
    """
    class _RemovedContainer(_FakeContainer):
        def logs(self, **_kwargs):
            yield b"Phase complete: BUILD State: SUCCEEDED"
            raise RuntimeError("container was removed")

        def wait(self):
            raise RuntimeError("container was removed")

    container = _RemovedContainer([])
    monkeypatch.setattr(codebuild, "_get_docker", lambda: _FakeDocker(container))
    monkeypatch.setattr(codebuild, "_container_for_build", lambda _bid: container)
    monkeypatch.setattr(codebuild, "WORKSPACE", str(tmp_path))
    monkeypatch.setattr(codebuild, "EXECUTE_BUILDS", True)

    project = _execution_project()
    codebuild._projects["demo"] = project
    build = _seed_execution_build(project, "demo:0011")

    codebuild._stop_build({"id": "demo:0011"})
    assert build["buildStatus"] == "STOPPED"

    codebuild._execute_build("demo:0011", project)

    assert build["buildStatus"] == "STOPPED"
    assert build["currentPhase"] == "COMPLETED"
    # The registry must not leak once the worker is done.
    assert codebuild._stop_requested("demo:0011") is False


def _capture_bus(monkeypatch):
    """The events CodeBuild puts on a bus, in order, as they are dispatched."""
    from ministack.services import eventbridge

    sent = []
    monkeypatch.setattr(eventbridge, "_dispatch_event", lambda event, view=None: sent.append(event))
    return sent


def test_an_executed_build_puts_its_end_on_the_default_bus(monkeypatch, tmp_path):
    """An executed build's end is a CodeBuild Build State Change on the default
    bus of its own account and region, at the status it ended with, timed at
    its end, listing the phase that failed."""
    sent = _capture_bus(monkeypatch)
    container = _FakeContainer(["Phase complete: BUILD State: FAILED"], exit_code=1)
    monkeypatch.setattr(codebuild, "_get_docker", lambda: _FakeDocker(container))
    monkeypatch.setattr(codebuild, "WORKSPACE", str(tmp_path))
    project = _execution_project()
    build = _seed_execution_build(project, "demo:0013")
    # The phases an executed build starts with, as StartBuild records them.
    build["phases"] = [
        {"phaseType": "SUBMITTED", "phaseStatus": "SUCCEEDED",
         "startTime": build["startTime"], "endTime": build["startTime"]},
        {"phaseType": "QUEUED", "startTime": build["startTime"]},
    ]

    codebuild._execute_build("demo:0013", project)

    assert [json.loads(e["Detail"])["build-status"] for e in sent] == ["FAILED"]
    event = sent[0]
    assert event["Source"] == "aws.codebuild"
    assert event["DetailType"] == "CodeBuild Build State Change"
    assert event["EventBusName"] == "default"
    assert event["Resources"] == [build["arn"]]
    assert (event["Account"], event["Region"]) == tuple(build["arn"].split(":")[4:2:-1])
    assert event["Time"] == build["endTime"]
    detail = json.loads(event["Detail"])
    assert detail["build-id"] == build["arn"]
    assert detail["project-name"] == "demo"
    phases = detail["additional-information"]["phases"]
    assert [(p["phase-type"], p.get("phase-status")) for p in phases] == [
        ("SUBMITTED", "SUCCEEDED"), ("QUEUED", "SUCCEEDED"), ("BUILD", "FAILED"), ("COMPLETED", None)]


def test_an_executed_build_puts_its_start_on_the_bus_before_it_runs(monkeypatch):
    """StartBuild puts IN_PROGRESS on the bus before the build can end."""
    sent = _capture_bus(monkeypatch)
    monkeypatch.setattr(codebuild, "EXECUTE_BUILDS", True)
    monkeypatch.setattr(codebuild, "_execute_build",
                        lambda _bid, _project: sent.append("executed"))
    codebuild._projects["demo"] = _execution_project()

    codebuild._start_build({"projectName": "demo"})

    deadline = time.time() + 5
    while "executed" not in sent and time.time() < deadline:
        time.sleep(0.01)
    assert sent[-1] == "executed"
    assert [json.loads(e["Detail"])["build-status"] for e in sent[:-1]] == ["IN_PROGRESS"]


def test_a_stopped_build_puts_one_end_on_the_bus(monkeypatch, tmp_path):
    """StopBuild of an executed build ends it once: the worker reports STOPPED."""
    sent = _capture_bus(monkeypatch)
    container = _FakeContainer(["Phase complete: INSTALL State: SUCCEEDED"])
    monkeypatch.setattr(codebuild, "_get_docker", lambda: _FakeDocker(container))
    monkeypatch.setattr(codebuild, "_container_for_build", lambda _bid: container)
    monkeypatch.setattr(codebuild, "WORKSPACE", str(tmp_path))
    monkeypatch.setattr(codebuild, "EXECUTE_BUILDS", True)
    project = _execution_project()
    codebuild._projects["demo"] = project
    _seed_execution_build(project, "demo:0012")

    codebuild._stop_build({"id": "demo:0012"})
    codebuild._execute_build("demo:0012", project)

    assert [json.loads(e["Detail"])["build-status"] for e in sent] == ["STOPPED"]


def test_execute_build_without_docker_reports_fault(monkeypatch, tmp_path):
    monkeypatch.setattr(codebuild, "_get_docker", lambda: None)
    monkeypatch.setattr(codebuild, "WORKSPACE", str(tmp_path))

    project = _execution_project()
    build = _seed_execution_build(project, "demo:0004")

    codebuild._execute_build("demo:0004", project)

    assert build["buildStatus"] == "FAULT"


def test_execute_build_agent_exit_zero_without_phases_is_fault(monkeypatch, tmp_path):
    """The agent exits 0 even when it started nothing (e.g. denied the Docker
    socket under SELinux). A zero exit with no 'Phase complete' line means no
    build phase ran, so the outcome is FAULT — not a silent SUCCEEDED."""
    container = _FakeContainer([
        "permission denied while trying to connect to the Docker daemon socket",
    ])
    docker = _FakeDocker(container)
    monkeypatch.setattr(codebuild, "_get_docker", lambda: docker)
    monkeypatch.setattr(codebuild, "WORKSPACE", str(tmp_path))

    project = _execution_project()
    build = _seed_execution_build(project)

    codebuild._execute_build("demo:0001", project)

    assert build["buildStatus"] == "FAULT"
    assert container.removed is True
