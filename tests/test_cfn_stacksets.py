"""
CloudFormation StackSets, self-managed: a stack set's instances are stacks in
their regions, each operation runs to SUCCEEDED or FAILED with the instance's
reason, and one operation runs at a time.
"""

import json
import os
import time
import uuid

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
ACCOUNT = "000000000000"
HOME = "us-east-1"
AWAY = "us-west-2"


def _client(service, region):
    return boto3.client(
        service,
        endpoint_url=ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name=region,
        config=Config(region_name=region, retries={"mode": "standard"}),
    )


@pytest.fixture
def home():
    return _client("cloudformation", HOME)


@pytest.fixture
def away():
    return _client("cloudformation", AWAY)


def _template(**outputs):
    """A queue, its name exported per region, and one required parameter."""
    return json.dumps({
        "Parameters": {
            "Label": {"Type": "String"},
            "Size": {"Type": "String", "Default": "small"},
        },
        "Resources": {"Queue": {"Type": "AWS::SQS::Queue"}},
        "Outputs": {
            "Label": {"Value": {"Fn::Sub": "${Label}-${Size}-${AWS::Region}"},
                      "Export": {"Name": {"Fn::Sub": "${Label}-label"}}},
            "Queue": {"Value": {"Fn::GetAtt": ["Queue", "Arn"]}},
            **outputs,
        },
    })


def _set_name():
    return f"set-{uuid.uuid4().hex[:8]}"


def _wait(cfn, name, operation_id, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        op = cfn.describe_stack_set_operation(
            StackSetName=name, OperationId=operation_id)["StackSetOperation"]
        if op["Status"] in ("SUCCEEDED", "FAILED", "STOPPED"):
            return op
        time.sleep(0.25)
    raise AssertionError(f"operation {operation_id} did not end")


def _instance_stack(home, away, name):
    summary = home.list_stack_instances(StackSetName=name, StackInstanceRegion=AWAY)["Summaries"]
    assert len(summary) == 1
    return away.describe_stacks(StackName=summary[0]["StackId"])["Stacks"][0], summary[0]


def _outputs(stack):
    return {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}


def _create(home, name, label="parts"):
    home.create_stack_set(
        StackSetName=name,
        TemplateBody=_template(),
        Parameters=[{"ParameterKey": "Label", "ParameterValue": label}],
        Capabilities=["CAPABILITY_NAMED_IAM"],
        Tags=[{"Key": "swb:stack-set", "Value": name}],
        AdministrationRoleARN=f"arn:aws:iam::{ACCOUNT}:role/{name}-administration",
        ExecutionRoleName=f"{name}-execution",
    )


def test_stack_set_records_its_template_parameters_and_roles(home):
    name = _set_name()
    _create(home, name)
    described = home.describe_stack_set(StackSetName=name)["StackSet"]
    assert described["StackSetName"] == name
    assert described["Status"] == "ACTIVE"
    assert described["PermissionModel"] == "SELF_MANAGED"
    assert described["Parameters"] == [{"ParameterKey": "Label", "ParameterValue": "parts"}]
    assert described["AdministrationRoleARN"] == f"arn:aws:iam::{ACCOUNT}:role/{name}-administration"
    assert described["ExecutionRoleName"] == f"{name}-execution"
    assert described["Capabilities"] == ["CAPABILITY_NAMED_IAM"]
    assert json.loads(described["TemplateBody"]) == json.loads(_template())
    assert name in [s["StackSetName"] for s in home.list_stack_sets()["Summaries"]]

    with pytest.raises(ClientError) as again:
        _create(home, name)
    assert again.value.response["Error"]["Code"] == "NameAlreadyExistsException"


def test_an_instance_is_a_stack_in_its_region_named_as_the_service_names_it(home, away):
    name = _set_name()
    _create(home, name)
    op = home.create_stack_instances(StackSetName=name, Accounts=[ACCOUNT], Regions=[AWAY])
    ended = _wait(home, name, op["OperationId"])
    assert ended["Status"] == "SUCCEEDED"
    assert ended["Action"] == "CREATE"

    stack, summary = _instance_stack(home, away, name)
    assert stack["StackName"].startswith(f"StackSet-{name}-")
    assert stack["StackStatus"] == "CREATE_COMPLETE"
    assert stack["StackId"].split(":")[3] == AWAY
    assert summary["Status"] == "CURRENT"
    assert summary["StackInstanceStatus"]["DetailedStatus"] == "SUCCEEDED"
    assert summary["Account"] == ACCOUNT
    # The stack set's parameters, its tags, and its region's own values.
    assert _outputs(stack)["Label"] == f"parts-small-{AWAY}"
    assert {"Key": "swb:stack-set", "Value": name} in stack["Tags"]
    # Its exports are its region's: none of them reaches the home region.
    away_exports = {e["Name"] for e in away.list_exports()["Exports"]}
    home_exports = {e["Name"] for e in home.list_exports()["Exports"]}
    assert "parts-label" in away_exports
    assert "parts-label" not in home_exports
    # No stack of it stands at home.
    assert not [s for s in home.list_stacks()["StackSummaries"]
                if s["StackName"] == stack["StackName"]]


def test_update_stack_set_moves_every_instance_to_the_new_template_and_parameters(home, away):
    name = _set_name()
    _create(home, name)
    _wait(home, name, home.create_stack_instances(
        StackSetName=name, Accounts=[ACCOUNT], Regions=[AWAY])["OperationId"])

    op = home.update_stack_set(
        StackSetName=name,
        TemplateBody=_template(Extra={"Value": "added"}),
        Parameters=[{"ParameterKey": "Label", "UsePreviousValue": True},
                    {"ParameterKey": "Size", "ParameterValue": "large"}],
        Capabilities=["CAPABILITY_NAMED_IAM"],
    )
    ended = _wait(home, name, op["OperationId"])
    assert ended["Status"] == "SUCCEEDED"
    assert ended["Action"] == "UPDATE"
    stack, _ = _instance_stack(home, away, name)
    assert stack["StackStatus"] == "UPDATE_COMPLETE"
    assert _outputs(stack)["Label"] == f"parts-large-{AWAY}"
    assert _outputs(stack)["Extra"] == "added"
    assert home.describe_stack_set(StackSetName=name)["StackSet"]["Parameters"] == [
        {"ParameterKey": "Label", "ParameterValue": "parts"},
        {"ParameterKey": "Size", "ParameterValue": "large"},
    ]


def test_an_update_that_changes_nothing_succeeds(home):
    name = _set_name()
    _create(home, name)
    _wait(home, name, home.create_stack_instances(
        StackSetName=name, Accounts=[ACCOUNT], Regions=[AWAY])["OperationId"])
    op = home.update_stack_set(
        StackSetName=name, UsePreviousTemplate=True,
        Parameters=[{"ParameterKey": "Label", "UsePreviousValue": True}],
        Capabilities=["CAPABILITY_NAMED_IAM"],
        Tags=[{"Key": "swb:stack-set", "Value": name}])
    assert _wait(home, name, op["OperationId"])["Status"] == "SUCCEEDED"


def test_an_instance_left_a_parameter_fails_naming_it_and_stands_nothing(home, away):
    name = _set_name()
    home.create_stack_set(StackSetName=name, TemplateBody=_template())
    op = home.create_stack_instances(StackSetName=name, Accounts=[ACCOUNT], Regions=[AWAY])
    ended = _wait(home, name, op["OperationId"])
    assert ended["Status"] == "FAILED"
    results = home.list_stack_set_operation_results(
        StackSetName=name, OperationId=op["OperationId"])["Summaries"]
    assert [(r["Region"], r["Status"]) for r in results] == [(AWAY, "FAILED")]
    assert "Label" in results[0]["StatusReason"]
    assert "Label" in ended["StatusReason"]
    summary = home.list_stack_instances(StackSetName=name)["Summaries"][0]
    assert summary["Status"] == "OUTDATED"
    assert "StackId" not in summary
    assert not [s for s in away.list_stacks()["StackSummaries"]
                if s["StackName"].startswith(f"StackSet-{name}-")]


def test_an_instance_whose_resource_fails_fails_the_operation_with_its_reason(home):
    name = _set_name()
    failing = json.loads(_template())
    failing["Resources"]["Bad"] = {
        "Type": "AWS::CloudFormation::CustomResource",
        "Properties": {
            "ServiceToken": f"arn:aws:lambda:{AWAY}:{ACCOUNT}:function:stackset-does-not-exist",
        },
    }
    home.create_stack_set(StackSetName=name, TemplateBody=json.dumps(failing),
                          Parameters=[{"ParameterKey": "Label", "ParameterValue": "x"}])
    op = home.create_stack_instances(StackSetName=name, Accounts=[ACCOUNT], Regions=[AWAY])
    ended = _wait(home, name, op["OperationId"], timeout=60)
    assert ended["Status"] == "FAILED"
    results = home.list_stack_set_operation_results(
        StackSetName=name, OperationId=op["OperationId"])["Summaries"]
    assert results[0]["Status"] == "FAILED"
    assert "Bad" in results[0]["StatusReason"]


def test_overrides_stand_through_an_update_of_the_stack_set(home, away):
    name = _set_name()
    _create(home, name)
    _wait(home, name, home.create_stack_instances(
        StackSetName=name, Accounts=[ACCOUNT], Regions=[AWAY],
        ParameterOverrides=[{"ParameterKey": "Size", "ParameterValue": "medium"}])["OperationId"])
    stack, _ = _instance_stack(home, away, name)
    assert _outputs(stack)["Label"] == f"parts-medium-{AWAY}"
    instance = home.describe_stack_instance(
        StackSetName=name, StackInstanceAccount=ACCOUNT, StackInstanceRegion=AWAY)["StackInstance"]
    assert instance["ParameterOverrides"] == [{"ParameterKey": "Size", "ParameterValue": "medium"}]

    _wait(home, name, home.update_stack_set(
        StackSetName=name, TemplateBody=_template(Extra={"Value": "x"}),
        Parameters=[{"ParameterKey": "Label", "UsePreviousValue": True},
                    {"ParameterKey": "Size", "ParameterValue": "large"}])["OperationId"])
    stack, _ = _instance_stack(home, away, name)
    assert _outputs(stack)["Label"] == f"parts-medium-{AWAY}"

    _wait(home, name, home.update_stack_instances(
        StackSetName=name, Accounts=[ACCOUNT], Regions=[AWAY],
        ParameterOverrides=[])["OperationId"])
    stack, _ = _instance_stack(home, away, name)
    assert _outputs(stack)["Label"] == f"parts-large-{AWAY}"


def test_one_operation_at_a_time(home):
    name = _set_name()
    slow = json.loads(_template())
    slow["Resources"]["Wait"] = {"Type": "AWS::CloudFormation::WaitConditionHandle"}
    slow["Resources"]["Held"] = {
        "Type": "AWS::CloudFormation::WaitCondition",
        "Properties": {"Handle": {"Ref": "Wait"}, "Timeout": "5"},
    }
    home.create_stack_set(StackSetName=name, TemplateBody=json.dumps(slow),
                          Parameters=[{"ParameterKey": "Label", "ParameterValue": "y"}])
    op = home.create_stack_instances(StackSetName=name, Accounts=[ACCOUNT], Regions=[AWAY])
    with pytest.raises(ClientError) as busy:
        home.update_stack_set(StackSetName=name, UsePreviousTemplate=True,
                              Parameters=[{"ParameterKey": "Label", "UsePreviousValue": True}])
    assert busy.value.response["Error"]["Code"] == "OperationInProgressException"
    _wait(home, name, op["OperationId"], timeout=30)


def test_deleting_instances_deletes_their_stacks_and_then_the_stack_set(home, away):
    name = _set_name()
    _create(home, name)
    _wait(home, name, home.create_stack_instances(
        StackSetName=name, Accounts=[ACCOUNT], Regions=[AWAY])["OperationId"])
    stack, _ = _instance_stack(home, away, name)

    with pytest.raises(ClientError) as not_empty:
        home.delete_stack_set(StackSetName=name)
    assert not_empty.value.response["Error"]["Code"] == "StackSetNotEmptyException"

    op = home.delete_stack_instances(StackSetName=name, Accounts=[ACCOUNT], Regions=[AWAY],
                                     RetainStacks=False)
    ended = _wait(home, name, op["OperationId"])
    assert ended["Status"] == "SUCCEEDED"
    assert ended["Action"] == "DELETE"
    assert home.list_stack_instances(StackSetName=name)["Summaries"] == []
    gone = away.describe_stacks(StackName=stack["StackId"])["Stacks"][0]
    assert gone["StackStatus"] == "DELETE_COMPLETE"

    home.delete_stack_set(StackSetName=name)
    with pytest.raises(ClientError) as missing:
        home.describe_stack_set(StackSetName=name)
    assert missing.value.response["Error"]["Code"] == "StackSetNotFoundException"


def test_an_unknown_operation_is_named(home):
    name = _set_name()
    _create(home, name)
    with pytest.raises(ClientError) as unknown:
        home.describe_stack_set_operation(StackSetName=name, OperationId="no-such-operation")
    assert unknown.value.response["Error"]["Code"] == "OperationNotFoundException"


def test_a_template_past_the_inline_limit_stands_its_instance(home, away):
    """A StackSet created from a TemplateURL holds a template up to the size
    that admits, and stands its instances from it: the inline TemplateBody
    limit is a request's, not the service's own operation's."""
    name = _set_name()
    big = json.loads(_template())
    big["Description"] = "x" * 1000
    for i in range(60):
        big["Resources"][f"Queue{i}"] = {
            "Type": "AWS::SQS::Queue",
            "Metadata": {"Padding": "p" * 1000},
        }
    body = json.dumps(big)
    assert len(body) > 51200
    s3 = _client("s3", HOME)
    bucket = f"stackset-templates-{uuid.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bucket)
    s3.put_object(Bucket=bucket, Key="big.json", Body=body.encode())
    home.create_stack_set(
        StackSetName=name,
        TemplateURL=f"{ENDPOINT}/{bucket}/big.json",
        Parameters=[{"ParameterKey": "Label", "ParameterValue": "big"}],
    )
    op = home.create_stack_instances(StackSetName=name, Accounts=[ACCOUNT], Regions=[AWAY])
    ended = _wait(home, name, op["OperationId"], timeout=60)
    assert ended["Status"] == "SUCCEEDED", ended.get("StatusReason")
    stack, _ = _instance_stack(home, away, name)
    assert stack["StackStatus"] == "CREATE_COMPLETE"


def test_a_failed_instance_names_its_reason_once_unescaped(home):
    name = _set_name()
    home.create_stack_set(StackSetName=name, TemplateBody=_template())
    op = home.create_stack_instances(StackSetName=name, Accounts=[ACCOUNT], Regions=[AWAY])
    _wait(home, name, op["OperationId"])
    results = home.list_stack_set_operation_results(
        StackSetName=name, OperationId=op["OperationId"])["Summaries"]
    assert "&#" not in results[0]["StatusReason"]
    assert "&amp;" not in results[0]["StatusReason"]
