# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""
CloudFormation StackSets — self-managed, operated from the region the stack
set is created in.

A stack set holds a template and its parameters; each of its instances is a
stack, in the instance's account and region, named as the service names it
(``StackSet-<stack set name>-<uuid>``), stood from the stack set's template
with the stack set's parameters, overridden by the instance's own where it
has any, and tagged with the stack set's tags. An operation — create, update
or delete of instances, or an update of the stack set itself — runs each
instance's stack operation in that instance's region and is ``RUNNING`` until
every one has finished: ``SUCCEEDED`` when each has, ``FAILED`` when any has
failed, with that instance's reason (no failure is tolerated). One operation
runs on a stack set at a time.

What is not emulated: the administration and execution roles are recorded
and never assumed or evaluated, and instances are operated together whatever
the operation preferences say. Service-managed stack sets, which deploy to an
organization's accounts, are not supported.

Supports: CreateStackSet, UpdateStackSet, DeleteStackSet, DescribeStackSet,
          ListStackSets, CreateStackInstances, UpdateStackInstances,
          DeleteStackInstances, ListStackInstances, DescribeStackInstance,
          DescribeStackSetOperation, ListStackSetOperations,
          ListStackSetOperationResults.
"""

import re
from html import unescape

from ministack.core.responses import (
    AccountRegionScopedDict,
    get_account_id,
    get_region,
    new_uuid,
    now_iso,
    request_scope,
)

from .helpers import (
    _error,
    _esc,
    _extract_members,
    _extract_string_members,
    _p,
    _resolve_template,
    _xml,
    service_operation,
)

_stack_sets = AccountRegionScopedDict()   # stack set name -> stack set dict
_operations = AccountRegionScopedDict()   # operation id -> operation dict

# A stack set operation's terminal statuses, and an instance result's.
_DONE = ("SUCCEEDED", "FAILED", "STOPPED")

# What each instance action reaches when it ends well, and when it does not.
_SUCCESS = {
    "CREATE": ("CREATE_COMPLETE",),
    "UPDATE": ("UPDATE_COMPLETE",),
    "DELETE": ("DELETE_COMPLETE",),
}
_FAILURE = {
    "CREATE": ("CREATE_FAILED", "ROLLBACK_COMPLETE", "ROLLBACK_FAILED"),
    "UPDATE": ("UPDATE_FAILED", "UPDATE_ROLLBACK_COMPLETE", "UPDATE_ROLLBACK_FAILED"),
    "DELETE": ("DELETE_FAILED",),
}

_NAME = re.compile(r"^[a-zA-Z][a-zA-Z0-9-]{0,127}$")


def reset():
    _stack_sets.clear()
    _operations.clear()


# ---------------------------------------------------------------------------
# Reading a request
# ---------------------------------------------------------------------------


def _parameters(params, prefix="Parameters"):
    """``[{ParameterKey, ParameterValue | UsePreviousValue}]`` as sent."""
    return [
        {"ParameterKey": m["Key"], "ParameterValue": m["Value"],
         "UsePreviousValue": m["UsePreviousValue"]}
        for m in _extract_members(params, prefix)
    ]


def _tags(params):
    return [{"Key": m["Key"], "Value": m["Value"]}
            for m in _extract_members(params, "Tags")]


def _not_found(name):
    return _error("StackSetNotFoundException", f"StackSet {name} not found")


def _stack_set(params):
    name = _p(params, "StackSetName")
    stack_set = _stack_sets.get(name)
    if stack_set is None:
        # The API takes the name or the stack set's id.
        for candidate in _stack_sets.values():
            if candidate["StackSetId"] == name:
                return candidate, None
        return None, _not_found(name)
    return stack_set, None


# ---------------------------------------------------------------------------
# Operations on instances
# ---------------------------------------------------------------------------


def _reason_of(stack):
    """Why a stack ended as it did: the first failed resource's reason, else
    the stack's own."""
    from ministack.services.cloudformation import _stack_events
    reasons = [
        f"{e.get('LogicalResourceId')}: {e.get('ResourceStatusReason')}"
        for e in reversed(_stack_events.get(stack.get("StackId"), []) or [])
        if str(e.get("ResourceStatus", "")).endswith("_FAILED")
        and e.get("ResourceStatusReason")
        and e.get("LogicalResourceId") != stack.get("StackName")
    ]
    if reasons:
        return reasons[0]
    return stack.get("StackStatusReason") or stack.get("StackStatus", "")


def _error_message(response):
    """The message of an error response one of the stack handlers returned."""
    body = response[2].decode("utf-8", errors="replace")
    found = re.search(r"<Message>(.*?)</Message>", body, re.S)
    return unescape(found.group(1)) if found else body


def _instance_parameters(stack_set, instance):
    """The stack set's parameters, each the instance's override where it has one."""
    overrides = {p["ParameterKey"]: p["ParameterValue"]
                 for p in instance.get("ParameterOverrides", [])}
    merged = {p["ParameterKey"]: p["ParameterValue"] for p in stack_set["Parameters"]}
    merged.update(overrides)
    return merged


def _stack_request(stack_set, instance, stack_name, update=False):
    """The query-protocol request for the instance's stack operation."""
    request = {"StackName": [stack_name], "TemplateBody": [stack_set["TemplateBody"]]}
    for i, (key, value) in enumerate(_instance_parameters(stack_set, instance).items(), 1):
        request[f"Parameters.member.{i}.ParameterKey"] = [key]
        request[f"Parameters.member.{i}.ParameterValue"] = [value]
    for i, capability in enumerate(stack_set["Capabilities"], 1):
        request[f"Capabilities.member.{i}"] = [capability]
    for i, tag in enumerate(stack_set["Tags"], 1):
        request[f"Tags.member.{i}.Key"] = [tag["Key"]]
        request[f"Tags.member.{i}.Value"] = [tag["Value"]]
    if update and not stack_set["Tags"]:
        request["Tags"] = [""]
    return request


def _operate(stack_set, instance, action):
    """Start one instance's stack operation in its region; the result it
    starts as: RUNNING, or already SUCCEEDED or FAILED with the reason."""
    account, region = instance["Account"], instance["Region"]
    # The instance's operation is the service's own, carrying the StackSet's
    # template whatever its size, never a request's inline body.
    token = service_operation.set(True)
    try:
        return _operate_in(stack_set, instance, action, account, region)
    finally:
        service_operation.reset(token)


def _operate_in(stack_set, instance, action, account, region):
    from .handlers import _create_stack, _delete_stack, _update_stack

    with request_scope(account, region):
        from ministack.services.cloudformation import _stacks
        name = instance.get("StackName")
        if action == "CREATE":
            name = f"StackSet-{stack_set['StackSetName']}-{new_uuid()}"
            response = _create_stack(_stack_request(stack_set, instance, name))
            if response[0] != 200:
                return {"Status": "FAILED", "StatusReason": _error_message(response)}
            instance["StackName"] = name
            instance["StackId"] = _stacks[name]["StackId"]
        elif action == "UPDATE":
            stack = _stacks.get(name) if name else None
            if stack is None:
                return {"Status": "FAILED",
                        "StatusReason": f"Stack {name or '(none)'} does not exist in {region}"}
            response = _update_stack(_stack_request(stack_set, instance, name, update=True))
            if response[0] != 200:
                message = _error_message(response)
                # An instance that already matches the stack set is current.
                if "No updates are to be performed" in message:
                    return {"Status": "SUCCEEDED", "StatusReason": ""}
                return {"Status": "FAILED", "StatusReason": message}
        elif action == "DELETE":
            if name is None or _stacks.get(name) is None:
                return {"Status": "SUCCEEDED", "StatusReason": ""}
            response = _delete_stack({"StackName": [name]})
            if response[0] != 200:
                return {"Status": "FAILED", "StatusReason": _error_message(response)}
    return {"Status": "RUNNING", "StatusReason": ""}


def _settle(result):
    """A RUNNING result read against its stack as it stands now."""
    if result["Status"] != "RUNNING":
        return
    with request_scope(result["Account"], result["Region"]):
        from ministack.services.cloudformation import _stacks
        stack = _stacks.get(result["StackName"]) if result.get("StackName") else None
        if stack is None:
            if result["Action"] == "DELETE":
                result["Status"] = "SUCCEEDED"
            return
        status = stack.get("StackStatus", "")
        if status in _SUCCESS[result["Action"]]:
            result["Status"] = "SUCCEEDED"
        elif status in _FAILURE[result["Action"]]:
            result["Status"] = "FAILED"
            result["StatusReason"] = _reason_of(stack)


def _refresh(operation):
    """The operation's status, read from its instances' stacks: RUNNING until
    each has ended, FAILED if any failed, else SUCCEEDED. Once ended it does not
    change."""
    if operation["Status"] in _DONE:
        return operation
    for result in operation["_results"]:
        _settle(result)
    statuses = [r["Status"] for r in operation["_results"]]
    if "RUNNING" in statuses:
        return operation
    failed = [r for r in operation["_results"] if r["Status"] == "FAILED"]
    operation["Status"] = "FAILED" if failed else "SUCCEEDED"
    operation["StatusReason"] = "; ".join(
        f"{r['Account']} {r['Region']}: {r['StatusReason']}" for r in failed)
    operation["EndTimestamp"] = now_iso()
    stack_set = _stack_sets.get(operation["_stack_set"])
    if stack_set is not None:
        for result in operation["_results"]:
            key = (result["Account"], result["Region"])
            instance = stack_set["_instances"].get(key)
            if result["Action"] == "DELETE" and result["Status"] == "SUCCEEDED":
                stack_set["_instances"].pop(key, None)
            elif instance is not None:
                instance["_detailed"] = result["Status"]
                instance["StatusReason"] = result["StatusReason"]
    return operation


def _busy(stack_set):
    for operation in _operations.values():
        if (operation["_stack_set"] == stack_set["StackSetName"]
                and _refresh(operation)["Status"] not in _DONE):
            return operation
    return None


def _start(stack_set, action, instances, params):
    """Begin an operation on the instances given, each in its region."""
    operation_id = _p(params, "OperationId") or new_uuid()
    if operation_id in _operations:
        return None, _error("OperationIdAlreadyExistsException",
                            f"Operation {operation_id} already exists")
    if busy := _busy(stack_set):
        return None, _error("OperationInProgressException",
                            f"Another Operation on StackSet {stack_set['StackSetName']} "
                            f"is in progress: {busy['OperationId']}")
    operation = {
        "OperationId": operation_id,
        "StackSetId": stack_set["StackSetId"],
        "Action": action if action != "UPDATE_SET" else "UPDATE",
        "Status": "RUNNING",
        "StatusReason": "",
        "CreationTimestamp": now_iso(),
        "AdministrationRoleARN": stack_set["AdministrationRoleARN"],
        "ExecutionRoleName": stack_set["ExecutionRoleName"],
        "_stack_set": stack_set["StackSetName"],
        "_results": [],
    }
    _operations[operation_id] = operation
    for instance in instances:
        instance_action = "UPDATE" if action == "UPDATE_SET" else action
        started = _operate(stack_set, instance, instance_action)
        instance["LastOperationId"] = operation_id
        instance["_detailed"] = "RUNNING"
        operation["_results"].append({
            "Account": instance["Account"],
            "Region": instance["Region"],
            "StackName": instance.get("StackName"),
            "Action": instance_action,
            **started,
        })
    _refresh(operation)
    return operation, None


def _targets(stack_set, params):
    """The (account, region) pairs a request names."""
    accounts = _extract_string_members(params, "Accounts")
    regions = _extract_string_members(params, "Regions")
    if not accounts or not regions:
        return None, _error("ValidationError",
                            "Accounts and Regions are required for a self-managed StackSet")
    return [(a, r) for a in accounts for r in regions], None


# ---------------------------------------------------------------------------
# The stack set
# ---------------------------------------------------------------------------


def _create_stack_set(params):
    name = _p(params, "StackSetName")
    if not _NAME.match(name or ""):
        return _error("ValidationError",
                      "StackSetName must start with a letter and contain only "
                      "alphanumeric characters and hyphens, at most 128")
    if name in _stack_sets:
        return _error("NameAlreadyExistsException", f"StackSet {name} already exists")
    if _p(params, "PermissionModel", "SELF_MANAGED") != "SELF_MANAGED":
        return _error("ValidationError", "Only SELF_MANAGED StackSets are supported")
    body, error = _resolve_template(params)
    if error:
        return error
    if not body:
        return _error("ValidationError", "TemplateBody or TemplateURL is required")
    stack_set_id = f"{name}:{new_uuid()}"
    _stack_sets[name] = {
        "StackSetName": name,
        "StackSetId": stack_set_id,
        "StackSetARN": f"arn:aws:cloudformation:{get_region()}:{get_account_id()}:"
                       f"stackset/{stack_set_id}",
        "Description": _p(params, "Description"),
        "Status": "ACTIVE",
        "TemplateBody": body,
        "Parameters": [{"ParameterKey": p["ParameterKey"], "ParameterValue": p["ParameterValue"]}
                       for p in _parameters(params)],
        "Capabilities": _extract_string_members(params, "Capabilities"),
        "Tags": _tags(params),
        "AdministrationRoleARN": _p(params, "AdministrationRoleARN") or
            f"arn:aws:iam::{get_account_id()}:role/AWSCloudFormationStackSetAdministrationRole",
        "ExecutionRoleName": _p(params, "ExecutionRoleName") or
            "AWSCloudFormationStackSetExecutionRole",
        "PermissionModel": "SELF_MANAGED",
        "_instances": {},
    }
    return _xml(200, "CreateStackSetResponse",
                f"<CreateStackSetResult><StackSetId>{_esc(stack_set_id)}</StackSetId>"
                f"</CreateStackSetResult>")


def _update_stack_set(params):
    stack_set, error = _stack_set(params)
    if error:
        return error
    if _p(params, "UsePreviousTemplate", "false").lower() == "true":
        body = stack_set["TemplateBody"]
    else:
        body, error = _resolve_template(params)
        if error:
            return error
        if not body:
            return _error("ValidationError",
                          "TemplateBody, TemplateURL or UsePreviousTemplate is required")
    previous = {p["ParameterKey"]: p["ParameterValue"] for p in stack_set["Parameters"]}
    parameters = []
    for p in _parameters(params):
        if p["UsePreviousValue"]:
            if p["ParameterKey"] not in previous:
                return _error("ValidationError",
                              f"Parameter {p['ParameterKey']} has no previous value")
            parameters.append({"ParameterKey": p["ParameterKey"],
                               "ParameterValue": previous[p["ParameterKey"]]})
        else:
            parameters.append({"ParameterKey": p["ParameterKey"],
                               "ParameterValue": p["ParameterValue"]})
    targets = None
    if _extract_string_members(params, "Accounts") or _extract_string_members(params, "Regions"):
        targets, error = _targets(stack_set, params)
        if error:
            return error
    if busy := _busy(stack_set):
        return _error("OperationInProgressException",
                      f"Another Operation on StackSet {stack_set['StackSetName']} "
                      f"is in progress: {busy['OperationId']}")
    stack_set["TemplateBody"] = body
    stack_set["Parameters"] = parameters
    stack_set["Capabilities"] = _extract_string_members(params, "Capabilities")
    if "Tags" in params or _tags(params):
        stack_set["Tags"] = _tags(params)
    if _p(params, "Description"):
        stack_set["Description"] = _p(params, "Description")
    if _p(params, "AdministrationRoleARN"):
        stack_set["AdministrationRoleARN"] = _p(params, "AdministrationRoleARN")
    if _p(params, "ExecutionRoleName"):
        stack_set["ExecutionRoleName"] = _p(params, "ExecutionRoleName")
    instances = [
        i for key, i in sorted(stack_set["_instances"].items())
        if targets is None or key in targets
    ]
    operation, error = _start(stack_set, "UPDATE_SET", instances, params)
    if error:
        return error
    return _xml(200, "UpdateStackSetResponse",
                f"<UpdateStackSetResult><OperationId>{_esc(operation['OperationId'])}"
                f"</OperationId></UpdateStackSetResult>")


def _delete_stack_set(params):
    stack_set, error = _stack_set(params)
    if error:
        return error
    _refresh_stack_set(stack_set)
    if stack_set["_instances"]:
        return _error("StackSetNotEmptyException",
                      f"StackSet {stack_set['StackSetName']} cannot be deleted "
                      f"while it has instances")
    if busy := _busy(stack_set):
        return _error("OperationInProgressException",
                      f"Another Operation on StackSet {stack_set['StackSetName']} "
                      f"is in progress: {busy['OperationId']}")
    del _stack_sets[stack_set["StackSetName"]]
    return _xml(200, "DeleteStackSetResponse", "<DeleteStackSetResult/>")


def _parameters_xml(parameters):
    return "".join(
        f"<member><ParameterKey>{_esc(p['ParameterKey'])}</ParameterKey>"
        f"<ParameterValue>{_esc(p['ParameterValue'])}</ParameterValue></member>"
        for p in parameters
    )


def _tags_xml(tags):
    return "".join(
        f"<member><Key>{_esc(t['Key'])}</Key><Value>{_esc(t['Value'])}</Value></member>"
        for t in tags
    )


def _describe_stack_set(params):
    stack_set, error = _stack_set(params)
    if error:
        return error
    s = stack_set
    capabilities = "".join(f"<member>{_esc(c)}</member>" for c in s["Capabilities"])
    return _xml(200, "DescribeStackSetResponse",
                "<DescribeStackSetResult><StackSet>"
                f"<StackSetName>{_esc(s['StackSetName'])}</StackSetName>"
                f"<StackSetId>{_esc(s['StackSetId'])}</StackSetId>"
                f"<StackSetARN>{_esc(s['StackSetARN'])}</StackSetARN>"
                f"<Description>{_esc(s['Description'])}</Description>"
                f"<Status>{s['Status']}</Status>"
                f"<TemplateBody>{_esc(s['TemplateBody'])}</TemplateBody>"
                f"<Parameters>{_parameters_xml(s['Parameters'])}</Parameters>"
                f"<Capabilities>{capabilities}</Capabilities>"
                f"<Tags>{_tags_xml(s['Tags'])}</Tags>"
                f"<AdministrationRoleARN>{_esc(s['AdministrationRoleARN'])}</AdministrationRoleARN>"
                f"<ExecutionRoleName>{_esc(s['ExecutionRoleName'])}</ExecutionRoleName>"
                f"<PermissionModel>{s['PermissionModel']}</PermissionModel>"
                "</StackSet></DescribeStackSetResult>")


def _list_stack_sets(params):
    wanted = _p(params, "Status")
    members = "".join(
        "<member>"
        f"<StackSetName>{_esc(s['StackSetName'])}</StackSetName>"
        f"<StackSetId>{_esc(s['StackSetId'])}</StackSetId>"
        f"<Description>{_esc(s['Description'])}</Description>"
        f"<Status>{s['Status']}</Status>"
        f"<PermissionModel>{s['PermissionModel']}</PermissionModel>"
        "</member>"
        for s in _stack_sets.values()
        if not wanted or s["Status"] == wanted
    )
    return _xml(200, "ListStackSetsResponse",
                f"<ListStackSetsResult><Summaries>{members}</Summaries></ListStackSetsResult>")


# ---------------------------------------------------------------------------
# Instances
# ---------------------------------------------------------------------------


def _create_stack_instances(params):
    stack_set, error = _stack_set(params)
    if error:
        return error
    targets, error = _targets(stack_set, params)
    if error:
        return error
    overrides = [{"ParameterKey": p["ParameterKey"], "ParameterValue": p["ParameterValue"]}
                 for p in _parameters(params, "ParameterOverrides")]
    if busy := _busy(stack_set):
        return _error("OperationInProgressException",
                      f"Another Operation on StackSet {stack_set['StackSetName']} "
                      f"is in progress: {busy['OperationId']}")
    instances = []
    for account, region in targets:
        if (account, region) in stack_set["_instances"]:
            # An instance that already stands is left as it is, as the
            # service does for a create naming it again.
            continue
        instance = {"Account": account, "Region": region,
                    "ParameterOverrides": overrides, "StatusReason": ""}
        stack_set["_instances"][(account, region)] = instance
        instances.append(instance)
    operation, error = _start(stack_set, "CREATE", instances, params)
    if error:
        return error
    return _xml(200, "CreateStackInstancesResponse",
                f"<CreateStackInstancesResult><OperationId>{_esc(operation['OperationId'])}"
                f"</OperationId></CreateStackInstancesResult>")


def _update_stack_instances(params):
    stack_set, error = _stack_set(params)
    if error:
        return error
    targets, error = _targets(stack_set, params)
    if error:
        return error
    missing = [t for t in targets if t not in stack_set["_instances"]]
    if missing:
        account, region = missing[0]
        return _error("StackInstanceNotFoundException",
                      f"StackSet {stack_set['StackSetName']} has no instance in "
                      f"{account} {region}")
    overrides = [{"ParameterKey": p["ParameterKey"], "ParameterValue": p["ParameterValue"]}
                 for p in _parameters(params, "ParameterOverrides")]
    if busy := _busy(stack_set):
        return _error("OperationInProgressException",
                      f"Another Operation on StackSet {stack_set['StackSetName']} "
                      f"is in progress: {busy['OperationId']}")
    instances = [stack_set["_instances"][t] for t in targets]
    for instance in instances:
        instance["ParameterOverrides"] = overrides
    operation, error = _start(stack_set, "UPDATE", instances, params)
    if error:
        return error
    return _xml(200, "UpdateStackInstancesResponse",
                f"<UpdateStackInstancesResult><OperationId>{_esc(operation['OperationId'])}"
                f"</OperationId></UpdateStackInstancesResult>")


def _delete_stack_instances(params):
    stack_set, error = _stack_set(params)
    if error:
        return error
    targets, error = _targets(stack_set, params)
    if error:
        return error
    if _p(params, "RetainStacks", "false").lower() == "true":
        if busy := _busy(stack_set):
            return _error("OperationInProgressException",
                          f"Another Operation on StackSet {stack_set['StackSetName']} "
                          f"is in progress: {busy['OperationId']}")
        # Retained: the stacks stand on, no longer the stack set's.
        operation, error = _start(stack_set, "DELETE", [], params)
        if error:
            return error
        for target in targets:
            stack_set["_instances"].pop(target, None)
    else:
        instances = [stack_set["_instances"][t] for t in targets if t in stack_set["_instances"]]
        operation, error = _start(stack_set, "DELETE", instances, params)
        if error:
            return error
    return _xml(200, "DeleteStackInstancesResponse",
                f"<DeleteStackInstancesResult><OperationId>{_esc(operation['OperationId'])}"
                f"</OperationId></DeleteStackInstancesResult>")


def _instance_status(instance):
    detailed = instance.get("_detailed", "PENDING")
    status = "CURRENT" if detailed == "SUCCEEDED" else "OUTDATED"
    return status, detailed


def _instance_xml(stack_set, instance, tag="member"):
    status, detailed = _instance_status(instance)
    overrides = (f"<ParameterOverrides>{_parameters_xml(instance['ParameterOverrides'])}"
                 f"</ParameterOverrides>" if tag == "StackInstance" else "")
    return (f"<{tag}>"
            f"<StackSetId>{_esc(stack_set['StackSetId'])}</StackSetId>"
            f"<Region>{_esc(instance['Region'])}</Region>"
            f"<Account>{_esc(instance['Account'])}</Account>"
            + (f"<StackId>{_esc(instance['StackId'])}</StackId>" if instance.get("StackId") else "")
            + f"<Status>{status}</Status>"
            f"<StatusReason>{_esc(instance.get('StatusReason', ''))}</StatusReason>"
            f"<StackInstanceStatus><DetailedStatus>{detailed}</DetailedStatus>"
            f"</StackInstanceStatus>"
            + (f"<LastOperationId>{_esc(instance['LastOperationId'])}</LastOperationId>"
               if instance.get("LastOperationId") else "")
            + overrides
            + f"</{tag}>")


def _refresh_stack_set(stack_set):
    for operation in list(_operations.values()):
        if operation["_stack_set"] == stack_set["StackSetName"]:
            _refresh(operation)


def _list_stack_instances(params):
    stack_set, error = _stack_set(params)
    if error:
        return error
    _refresh_stack_set(stack_set)
    account = _p(params, "StackInstanceAccount")
    region = _p(params, "StackInstanceRegion")
    members = "".join(
        _instance_xml(stack_set, instance)
        for (a, r), instance in sorted(stack_set["_instances"].items())
        if (not account or a == account) and (not region or r == region)
    )
    return _xml(200, "ListStackInstancesResponse",
                f"<ListStackInstancesResult><Summaries>{members}</Summaries>"
                f"</ListStackInstancesResult>")


def _describe_stack_instance(params):
    stack_set, error = _stack_set(params)
    if error:
        return error
    _refresh_stack_set(stack_set)
    key = (_p(params, "StackInstanceAccount"), _p(params, "StackInstanceRegion"))
    instance = stack_set["_instances"].get(key)
    if instance is None:
        return _error("StackInstanceNotFoundException",
                      f"StackSet {stack_set['StackSetName']} has no instance in "
                      f"{key[0]} {key[1]}")
    return _xml(200, "DescribeStackInstanceResponse",
                "<DescribeStackInstanceResult>"
                + _instance_xml(stack_set, instance, tag="StackInstance")
                + "</DescribeStackInstanceResult>")


# ---------------------------------------------------------------------------
# Operations
# ---------------------------------------------------------------------------


def _operation(params):
    stack_set, error = _stack_set(params)
    if error:
        return None, error
    operation = _operations.get(_p(params, "OperationId"))
    if operation is None or operation["_stack_set"] != stack_set["StackSetName"]:
        return None, _error("OperationNotFoundException",
                            f"Operation {_p(params, 'OperationId')} not found")
    return _refresh(operation), None


def _operation_xml(operation, tag):
    end = (f"<EndTimestamp>{operation['EndTimestamp']}</EndTimestamp>"
           if operation.get("EndTimestamp") else "")
    return (f"<{tag}>"
            f"<OperationId>{_esc(operation['OperationId'])}</OperationId>"
            f"<StackSetId>{_esc(operation['StackSetId'])}</StackSetId>"
            f"<Action>{operation['Action']}</Action>"
            f"<Status>{operation['Status']}</Status>"
            f"<StatusReason>{_esc(operation['StatusReason'])}</StatusReason>"
            f"<CreationTimestamp>{operation['CreationTimestamp']}</CreationTimestamp>"
            f"{end}"
            f"<AdministrationRoleARN>{_esc(operation['AdministrationRoleARN'])}"
            f"</AdministrationRoleARN>"
            f"<ExecutionRoleName>{_esc(operation['ExecutionRoleName'])}</ExecutionRoleName>"
            f"</{tag}>")


def _describe_stack_set_operation(params):
    operation, error = _operation(params)
    if error:
        return error
    return _xml(200, "DescribeStackSetOperationResponse",
                "<DescribeStackSetOperationResult>"
                + _operation_xml(operation, "StackSetOperation")
                + "</DescribeStackSetOperationResult>")


def _list_stack_set_operations(params):
    stack_set, error = _stack_set(params)
    if error:
        return error
    _refresh_stack_set(stack_set)
    members = "".join(
        _operation_xml(o, "member") for o in _operations.values()
        if o["_stack_set"] == stack_set["StackSetName"]
    )
    return _xml(200, "ListStackSetOperationsResponse",
                f"<ListStackSetOperationsResult><Summaries>{members}</Summaries>"
                f"</ListStackSetOperationsResult>")


def _list_stack_set_operation_results(params):
    operation, error = _operation(params)
    if error:
        return error
    members = "".join(
        "<member>"
        f"<Account>{_esc(r['Account'])}</Account>"
        f"<Region>{_esc(r['Region'])}</Region>"
        f"<Status>{r['Status']}</Status>"
        f"<StatusReason>{_esc(r['StatusReason'])}</StatusReason>"
        "</member>"
        for r in operation["_results"]
    )
    return _xml(200, "ListStackSetOperationResultsResponse",
                f"<ListStackSetOperationResultsResult><Summaries>{members}</Summaries>"
                f"</ListStackSetOperationResultsResult>")


STACK_SET_HANDLERS = {
    "CreateStackSet": _create_stack_set,
    "UpdateStackSet": _update_stack_set,
    "DeleteStackSet": _delete_stack_set,
    "DescribeStackSet": _describe_stack_set,
    "ListStackSets": _list_stack_sets,
    "CreateStackInstances": _create_stack_instances,
    "UpdateStackInstances": _update_stack_instances,
    "DeleteStackInstances": _delete_stack_instances,
    "ListStackInstances": _list_stack_instances,
    "DescribeStackInstance": _describe_stack_instance,
    "DescribeStackSetOperation": _describe_stack_set_operation,
    "ListStackSetOperations": _list_stack_set_operations,
    "ListStackSetOperationResults": _list_stack_set_operation_results,
}
