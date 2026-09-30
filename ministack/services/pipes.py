# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""
EventBridge Pipes service emulator.

REST/JSON protocol — /v1/pipes/* and /tags/* paths.

SDK surface:
  ListPipes, CreatePipe, DescribePipe, UpdatePipe, DeletePipe,
  StartPipe, StopPipe, ListTagsForResource, TagResource, UntagResource

Runtime (background poller + CloudFormation) scope is intentionally limited to:
- Source: DynamoDB Streams, with FilterCriteria and BatchSize
- Enrichment: Lambda
- Target: SNS, Step Functions state machine, EventBridge event bus (with
  EventBridgeEventBusParameters and InputTemplate)
"""

import copy
import json
import logging
import os
import threading
import time
from urllib.parse import unquote

from ministack.core.arn import ArnParseError, parse_arn
from ministack.core.responses import (
    AccountRegionScopedDict,
    AccountScopedDict,
    _request_account_id,
    _request_region,
    get_account_id,
    get_region,
    new_uuid,
)

logger = logging.getLogger("pipes")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")
CROSS_REGION_PIPE_ERROR = "Creating cross-region pipe is not permitted."

_pipes = AccountRegionScopedDict()       # pipe_name -> pipe record
_positions = AccountRegionScopedDict()   # pipe_arn -> next stream record index
_poller_started = False
_poller_lock = threading.Lock()


def get_state():
    return {
        "pipes": copy.deepcopy(_pipes),
        "positions": copy.deepcopy(_positions),
    }


def load_persisted_state(data) -> None:
    _restore_state(data)
    # Restored RUNNING pipes need the background poller — register_pipe is the
    # only other place that starts it, and it is not called on warm boot.
    if any(pipe.get("CurrentState") == "RUNNING" for pipe in _pipes.all_values()):
        _ensure_poller()


def _restore_state(data):
    if data:
        _restore_pipe_store(data.get("pipes", {}))
        _restore_position_store(data.get("positions", {}))


def _pipe_arn_scope(pipe_arn: str, default_account_id: str | None = None) -> tuple[str, str]:
    try:
        spec = parse_arn(pipe_arn)
    except ArnParseError:
        return default_account_id or get_account_id(), get_region()
    if spec.service != "pipes":
        return default_account_id or get_account_id(), get_region()
    return spec.account_id or default_account_id or get_account_id(), spec.region or get_region()


def _pipe_record_scope(pipe: dict, default_account_id: str | None = None) -> tuple[str, str]:
    return _pipe_arn_scope(pipe.get("Arn", ""), default_account_id)


def _restore_pipe_store(data) -> None:
    if isinstance(data, AccountRegionScopedDict):
        _pipes.update(data)
        return
    if isinstance(data, AccountScopedDict):
        for (account_id, name), pipe in data._data.items():
            restored_account_id, region = _pipe_record_scope(pipe, account_id)
            _pipes.set_scoped(restored_account_id, region, name, copy.deepcopy(pipe))
        return
    if isinstance(data, dict):
        for key, pipe in data.items():
            if isinstance(key, tuple) and len(key) == 3:
                account_id, region, name = key
            elif isinstance(key, tuple) and len(key) == 2:
                account_id, name = key
                account_id, region = _pipe_record_scope(pipe, account_id)
            else:
                name = key
                account_id, region = _pipe_record_scope(pipe)
            _pipes.set_scoped(account_id, region, name, copy.deepcopy(pipe))


def _restore_position_store(data) -> None:
    if isinstance(data, AccountRegionScopedDict):
        _positions.update(data)
        return
    if isinstance(data, AccountScopedDict):
        for (account_id, pipe_arn), position in data._data.items():
            restored_account_id, region = _pipe_arn_scope(pipe_arn, account_id)
            _positions.set_scoped(restored_account_id, region, pipe_arn, position)
        return
    if isinstance(data, dict):
        for key, position in data.items():
            if isinstance(key, tuple) and len(key) == 3:
                account_id, region, pipe_arn = key
            elif isinstance(key, tuple) and len(key) == 2:
                account_id, pipe_arn = key
                account_id, region = _pipe_arn_scope(pipe_arn, account_id)
            else:
                pipe_arn = key
                account_id, region = _pipe_arn_scope(pipe_arn)
            _positions.set_scoped(account_id, region, pipe_arn, position)


def _iter_all_pipes():
    for scoped_key, pipe in list(_pipes.all_items()):
        account_id, region, _name = scoped_key
        yield account_id, region, pipe




def reset():
    _pipes.clear()
    _positions.clear()


def register_pipe(
    *,
    name: str,
    source: str,
    target: str,
    role_arn: str = "",
    desired_state: str = "RUNNING",
    starting_position: str = "LATEST",
    tags: dict | None = None,
    description: str = "",
    source_parameters: dict | None = None,
    enrichment: str = "",
    enrichment_parameters: dict | None = None,
    target_parameters: dict | None = None,
):
    pipe_region = get_region()
    # AWS rejects cross-region source/target ARNs before role validation.
    for component_arn in (source, target):
        try:
            component_region = parse_arn(component_arn).region
        except ArnParseError:
            continue
        if component_region and component_region != pipe_region:
            raise ValueError(CROSS_REGION_PIPE_ERROR)
    filter_error = _filter_criteria_error(source_parameters)
    if filter_error:
        raise ValueError(filter_error)

    arn = f"arn:aws:pipes:{pipe_region}:{get_account_id()}:pipe/{name}"
    state = "STOPPED" if str(desired_state).upper() == "STOPPED" else "RUNNING"
    start = str(starting_position or "LATEST").upper()
    if start not in ("LATEST", "TRIM_HORIZON"):
        start = "LATEST"

    now = int(time.time())
    _pipes[name] = {
        "Name": name,
        "Arn": arn,
        "RoleArn": role_arn,
        "Description": description or "",
        "Source": source,
        "Target": target,
        "DesiredState": state,
        "CurrentState": state,
        "StartingPosition": start,
        "Tags": tags or {},
        "CreationTime": now,
        "LastModifiedTime": now,
    }
    _set_pipe_parameters(
        _pipes[name],
        source_parameters=source_parameters,
        enrichment=enrichment,
        enrichment_parameters=enrichment_parameters,
        target_parameters=target_parameters,
    )
    _positions[arn] = _initial_position(_pipes[name])

    _ensure_poller()
    return _pipes[name]


def _set_pipe_parameters(pipe: dict, **parameters) -> None:
    """Store the optional pipe members as the API returns them: present when
    set, absent otherwise (DescribePipe omits an unset member)."""
    members = {
        "source_parameters": "SourceParameters",
        "enrichment": "Enrichment",
        "enrichment_parameters": "EnrichmentParameters",
        "target_parameters": "TargetParameters",
    }
    for arg, member in members.items():
        if arg not in parameters:
            continue
        value = parameters[arg]
        if value:
            pipe[member] = copy.deepcopy(value)
        else:
            pipe.pop(member, None)


def _filter_patterns(pipe_or_parameters: dict | None) -> list:
    """The FilterCriteria patterns of a pipe record or of its SourceParameters."""
    params = pipe_or_parameters or {}
    if "SourceParameters" in params:
        params = params.get("SourceParameters") or {}
    criteria = params.get("FilterCriteria") or {} if isinstance(params, dict) else {}
    filters = criteria.get("Filters") or [] if isinstance(criteria, dict) else []
    return [f.get("Pattern") for f in filters if isinstance(f, dict) and "Pattern" in f]


def _filter_criteria_error(source_parameters: dict | None) -> str:
    """Why a FilterCriteria pattern cannot be applied, or "" when every one can.
    A filter pattern is an EventBridge event pattern; one the pattern compiler
    refuses is refused here instead of silently matching nothing."""
    from ministack.services import eventbridge as _eb

    for pattern in _filter_patterns(source_parameters):
        _alternatives, reason = _eb._parse_pattern_text(pattern)
        if reason:
            return f"Invalid FilterCriteria pattern {pattern!r}: {reason}"
    return ""


def delete_pipe(name: str):
    pipe = _pipes.pop(name, None)
    if pipe:
        _positions.pop(pipe["Arn"], None)


def _ensure_poller():
    global _poller_started
    with _poller_lock:
        if not _poller_started:
            t = threading.Thread(target=_poll_loop, daemon=True)
            t.start()
            _poller_started = True


def _poll_loop():
    while True:
        try:
            _poll_once()
        except Exception as e:
            logger.error("Pipes poller error: %s", e)
        time.sleep(1 if _pipes.has_any() else 5)


def _poll_once():
    from ministack.services import dynamodb as _ddb

    stream_records = getattr(_ddb, "_stream_records", None)
    if stream_records is None:
        return

    for pipe_account_id, pipe_region, pipe in _iter_all_pipes():
        account_token = _request_account_id.set(pipe_account_id)
        region_token = _request_region.set(pipe_region)
        try:
            _poll_pipe(_ddb, pipe, pipe_account_id)
        finally:
            _request_region.reset(region_token)
            _request_account_id.reset(account_token)


def _poll_pipe(_ddb, pipe: dict, pipe_account_id: str) -> None:
    if pipe.get("CurrentState") != "RUNNING":
        return
    if _arn_service(pipe.get("Source", "")) != "dynamodb":
        return

    source = _dynamodb_stream_source(pipe.get("Source", ""))
    if source is None:
        return
    source_spec, table_name = source
    if source_spec.account_id != pipe_account_id:
        return

    scope = {"account_id": pipe_account_id, "region": source_spec.region}
    # Positions are absolute stream positions: records expiring off the
    # front of the stream must not shift a pipe's read position.
    horizon = _ddb.stream_start_position(table_name, **scope)
    end = _ddb.stream_end_position(table_name, **scope)
    pos = max(int(_positions.get(pipe["Arn"], 0)), horizon)
    if pos >= end:
        return

    count = end - pos
    batch_size = _batch_size(pipe)
    if batch_size:
        count = min(count, batch_size)
    batch = _ddb.stream_records_since(table_name, pos, count, **scope)
    if _deliver_batch(pipe, batch):
        _positions[pipe["Arn"]] = pos + len(batch)


def _batch_size(pipe: dict) -> int:
    """DynamoDBStreamParameters.BatchSize, or 0 when the pipe sets none."""
    params = (pipe.get("SourceParameters") or {}).get("DynamoDBStreamParameters") or {}
    try:
        return max(0, int(params.get("BatchSize") or 0))
    except (TypeError, ValueError):
        return 0


def _deliver_batch(pipe: dict, batch: list) -> bool:
    """True once every record in the batch has reached the target. A batch that
    did not is left on the stream: the position stays on it and the next poll
    retries it, until the records age out of the retention window.

    The batch goes through the pipe's stages in order: the records no filter
    admits are dropped, the rest go to the enrichment when there is one, and
    what the enrichment answers is what the target receives. A batch the
    filter empties, or one the enrichment maps to an empty array, has reached
    its target: there is nothing to deliver."""
    target_arn = pipe.get("Target", "")
    target_service = _arn_service(target_arn)
    if target_service not in ("states", "sns", "events"):
        logger.warning(
            "Pipes %s: holding %d record(s); MiniStack delivers to sns, states "
            "and events, not to %s", pipe.get("Name"), len(batch),
            target_service or target_arn)
        return False

    events = _filtered_records(pipe, batch)
    if not events:
        return True
    if pipe.get("Enrichment"):
        enriched, events = _enrich(pipe, events)
        if not enriched:
            return False
        if not events:
            return True

    if target_service == "states":
        return _start_state_machine_from_records(target_arn, pipe, events)
    if target_service == "events":
        return _put_events_on_bus(target_arn, pipe, events)
    for rec in events:
        _publish_record_to_sns(target_arn, pipe, rec)
    return True


def _filtered_records(pipe: dict, records: list) -> list:
    """The records at least one FilterCriteria pattern matches; all of them
    when the pipe has no filter. A pattern is matched against the whole
    record, so a DynamoDB filter names ``dynamodb``, ``eventName`` and so on."""
    patterns = _filter_patterns(pipe)
    if not patterns:
        return list(records)
    from ministack.services import eventbridge as _eb

    def admitted(record):
        for pattern in patterns:
            alternatives = _eb._compiled_pattern(pattern) if isinstance(pattern, str) else None
            if alternatives and any(_eb._matches_detail(record, alt) for alt in alternatives):
                return True
        return False

    return [record for record in records if admitted(record)]


def _enrich(pipe: dict, records: list) -> tuple[bool, list]:
    """Invoke the enrichment function synchronously with the batch, as a JSON
    array, and answer ``(True, events)`` with what it returned — an array is
    the batch, anything else one event — or ``(False, [])`` when it did not
    answer, which holds the batch on the stream."""
    enrichment_arn = pipe.get("Enrichment", "")
    if _arn_service(enrichment_arn) != "lambda":
        logger.warning("Pipes %s: holding %d record(s); MiniStack enriches with "
                       "lambda, not %s", pipe.get("Name"), len(records), enrichment_arn)
        return False, []
    from ministack.services import lambda_svc

    func, config, func_name = lambda_svc._get_func_record_for_ref(enrichment_arn)
    if not func or not config:
        logger.warning("Pipes %s: enrichment function %s not found",
                       pipe.get("Name"), func_name)
        return False, []
    result = lambda_svc._execute_function_with_config_scope(
        lambda_svc._execution_record_for_config(func, config), records)
    if result.get("error"):
        logger.warning("Pipes %s: enrichment %s failed: %s",
                       pipe.get("Name"), func_name, result.get("body"))
        return False, []
    body = result.get("body")
    if isinstance(body, (bytes, str)):
        try:
            body = json.loads(body) if body else None
        except (json.JSONDecodeError, TypeError):
            pass
    if body is None:
        return True, []
    return True, body if isinstance(body, list) else [body]


def _json_path_value(path: str, document):
    """The value a Pipes JSON path (``$``, ``$.a.b``, ``$.a[0]``) names in
    ``document``, and whether it resolved."""
    if path == "$":
        return document, True
    if not path.startswith("$."):
        return None, False
    value = document
    for part in path[2:].split("."):
        name, _, rest = part.partition("[")
        try:
            if name:
                value = value[name]
            while rest:
                index, _, rest = rest.partition("]")
                value = value[int(index)]
                rest = rest[1:] if rest.startswith("[") else rest
        except (KeyError, IndexError, TypeError, ValueError):
            return None, False
    return value, True


def _parameter_value(value, event):
    """A target parameter as the target reads it: a string beginning ``$.`` is
    a dynamic path parameter, read off the event; anything else is literal.
    ``None`` when a dynamic path does not resolve."""
    if isinstance(value, str) and value.startswith("$."):
        resolved, found = _json_path_value(value, event)
        return resolved if found else None
    return value


def _render_target_input(template: str, event) -> str:
    """The pipe's InputTemplate rendered against one event: each ``<$.path>``
    placeholder is replaced by what the path names, the way EventBridge renders
    an input transformer's template — a string outside quotes verbatim, an
    object or array in a string with its quotes dropped. A placeholder whose
    path does not resolve stays as written."""
    from ministack.services import eventbridge as _eb

    replacements = {}
    for token in _eb._PLACEHOLDER_RE.findall(template):
        if token.startswith("$"):
            value, found = _json_path_value(token, event)
            if found:
                replacements[token] = value
    return _eb._render_input_template(template, replacements)


def _put_events_on_bus(bus_arn: str, pipe: dict, events: list) -> bool:
    """Put one entry per event on the target bus through PutEvents, so the
    bus's rules, archives and targets see them as any other put.

    Every entry is built before any is put: an entry whose Source or
    DetailType path does not resolve, or whose Detail is not a JSON object
    (PutEvents' ``MalformedDetail``), holds the whole batch, so a retry does
    not put its good neighbours twice."""
    from ministack.services import eventbridge as _eb

    params = pipe.get("TargetParameters") or {}
    bus_params = params.get("EventBridgeEventBusParameters") or {}
    template = params.get("InputTemplate")
    entries = []
    for event in events:
        source = _parameter_value(bus_params.get("Source", ""), event)
        detail_type = _parameter_value(bus_params.get("DetailType", ""), event)
        if source is None or detail_type is None:
            logger.warning("Pipes %s: holding the batch; a dynamic path in "
                           "EventBridgeEventBusParameters does not resolve on %s",
                           pipe.get("Name"), json.dumps(event, default=str)[:200])
            return False
        if template is not None:
            detail = _render_target_input(template, event)
        else:
            detail = json.dumps(event)
        try:
            parsed = json.loads(detail)
        except (json.JSONDecodeError, TypeError):
            parsed = None
        if not isinstance(parsed, dict):
            logger.warning("Pipes %s: holding the batch; MalformedDetail — the "
                           "target input is not a JSON object: %s",
                           pipe.get("Name"), str(detail)[:200])
            return False
        entry = {
            "EventBusName": bus_arn,
            "Source": str(source),
            "DetailType": str(detail_type),
            "Detail": detail,
        }
        if bus_params.get("Resources"):
            entry["Resources"] = list(bus_params["Resources"])
        entries.append(entry)

    for start in range(0, len(entries), 10):
        status, _headers, body = _eb._put_events({"Entries": entries[start:start + 10]})
        answer = json.loads(body) if status < 400 and body else {}
        if status >= 400 or answer.get("FailedEntryCount"):
            logger.warning("Pipes %s: PutEvents on %s failed (%s): %s",
                           pipe.get("Name"), bus_arn, status, body)
            return False
    return True


def _start_state_machine_from_records(sm_arn: str, pipe: dict, records: list) -> bool:
    """Start one execution carrying the batch as the JSON array Pipes delivers.

    `_start_execution` answers the `(status, headers, body)` triple every
    MiniStack handler answers; anything from 400 up means no execution started.
    """
    from ministack.services import stepfunctions as _sfn

    status, _headers, body = _sfn._start_execution({
        "stateMachineArn": sm_arn,
        "input": json.dumps(records),
    })
    if status >= 400:
        logger.warning("Pipes %s: StartExecution on %s failed (%s): %s",
                       pipe.get("Name"), sm_arn, status, body)
        return False
    return True


def _publish_record_to_sns(topic_arn: str, pipe: dict, record: dict):
    from ministack.services import sns as _sns

    topic = _sns._topics.get(topic_arn)
    if not topic:
        logger.warning("Pipes %s: SNS topic not found %s", pipe.get("Name"), topic_arn)
        return

    msg_id = new_uuid()
    message = json.dumps(record)
    subject = f"Pipes {pipe.get('Name', '')}"

    topic["messages"].append({
        "id": msg_id,
        "message": message,
        "subject": subject,
        "message_structure": "",
        "message_attributes": {},
        "timestamp": int(time.time()),
    })
    _sns._fanout(topic_arn, msg_id, message, subject, "", {})


def _arn_service(arn: str) -> str:
    """Classify a target ARN for dispatch; invalid stored targets are ignored."""
    try:
        return parse_arn(arn).service
    except ArnParseError:
        return ""


def _table_name_from_stream_arn(stream_arn: str) -> str:
    """Return a DynamoDB table name for Pipes runtime dispatch, or empty string."""
    source = _dynamodb_stream_source(stream_arn)
    return "" if source is None else source[1]


def _dynamodb_stream_source(stream_arn: str):
    """Return the parsed source ARN and table name for a DynamoDB stream."""
    try:
        spec = parse_arn(stream_arn)
    except ArnParseError:
        return None
    if spec.service != "dynamodb":
        return None
    parts = spec.resource.split("/")
    if (
        len(parts) < 4
        or parts[0] != "table"
        or parts[2] != "stream"
        or not parts[1]
        or not parts[3]
    ):
        return None
    return spec, parts[1]


def _pipe_account_id(pipe: dict) -> str:
    try:
        spec = parse_arn(pipe.get("Arn", ""))
    except ArnParseError:
        return get_account_id()
    if spec.service != "pipes" or not spec.account_id:
        return get_account_id()
    return spec.account_id


def _initial_position(pipe: dict) -> int:
    from ministack.services import dynamodb as _ddb

    source = _dynamodb_stream_source(pipe.get("Source", ""))
    if source is None:
        return 0
    source_spec, table_name = source
    pipe_account_id = _pipe_account_id(pipe)
    if source_spec.account_id != pipe_account_id:
        return 0

    stream_records = getattr(_ddb, "_stream_records", None)
    if stream_records is None:
        return 0
    scope = {"account_id": pipe_account_id, "region": source_spec.region}
    if pipe.get("StartingPosition") == "TRIM_HORIZON":
        return _ddb.stream_start_position(table_name, **scope)
    return _ddb.stream_end_position(table_name, **scope)


# ---------------------------------------------------------------------------
# REST/JSON request handler (endpointPrefix "pipes", protocol rest-json).
#
# Op / method / requestUri (botocore pipes/2015-10-07/service-2.json):
#   ListPipes             GET    /v1/pipes
#   CreatePipe            POST   /v1/pipes/{Name}
#   DescribePipe          GET    /v1/pipes/{Name}
#   UpdatePipe            PUT    /v1/pipes/{Name}
#   DeletePipe            DELETE /v1/pipes/{Name}
#   StartPipe             POST   /v1/pipes/{Name}/start
#   StopPipe              POST   /v1/pipes/{Name}/stop
#   ListTagsForResource   GET    /tags/{resourceArn}
#   TagResource           POST   /tags/{resourceArn}
#   UntagResource         DELETE /tags/{resourceArn}
#
# JSON-protocol timestamps are int epoch seconds (Timestamp shape).
# ---------------------------------------------------------------------------

_VALID_REQUESTED_STATE = ("RUNNING", "STOPPED")


def _json_resp(status, body):
    return status, {"Content-Type": "application/json"}, json.dumps(body).encode()


def _error(status, code, message):
    return (
        status,
        {"Content-Type": "application/json", "x-amzn-errortype": code},
        json.dumps({"__type": code, "message": message}).encode(),
    )


def _not_found(name):
    # Matches the real AWS NotFoundException message for DescribePipe/DeletePipe
    # etc. (member: "message").
    return _error(404, "NotFoundException", f"Pipe {name} does not exist.")


def _single(v):
    return v[0] if isinstance(v, list) else v


def _lifecycle_response(pipe):
    """Shape shared by CreatePipe/UpdatePipe/DeletePipe/StartPipe/StopPipe."""
    return {
        "Arn": pipe.get("Arn", ""),
        "Name": pipe.get("Name", ""),
        "DesiredState": pipe.get("DesiredState", "RUNNING"),
        "CurrentState": pipe.get("CurrentState", "RUNNING"),
        "CreationTime": int(pipe.get("CreationTime", 0)),
        "LastModifiedTime": int(pipe.get("LastModifiedTime", pipe.get("CreationTime", 0))),
    }


def _summary(pipe):
    """ListPipes Pipe summary member shape."""
    out = {
        "Name": pipe.get("Name", ""),
        "Arn": pipe.get("Arn", ""),
        "DesiredState": pipe.get("DesiredState", "RUNNING"),
        "CurrentState": pipe.get("CurrentState", "RUNNING"),
        "StateReason": pipe.get("StateReason", ""),
        "CreationTime": int(pipe.get("CreationTime", 0)),
        "LastModifiedTime": int(pipe.get("LastModifiedTime", pipe.get("CreationTime", 0))),
        "Source": pipe.get("Source", ""),
        "Target": pipe.get("Target", ""),
    }
    if pipe.get("Enrichment"):
        out["Enrichment"] = pipe["Enrichment"]
    return out


def _describe_response(pipe):
    return {
        "Arn": pipe.get("Arn", ""),
        "Name": pipe.get("Name", ""),
        "Description": pipe.get("Description", ""),
        "DesiredState": pipe.get("DesiredState", "RUNNING"),
        "CurrentState": pipe.get("CurrentState", "RUNNING"),
        "StateReason": pipe.get("StateReason", ""),
        "Source": pipe.get("Source", ""),
        "Target": pipe.get("Target", ""),
        "RoleArn": pipe.get("RoleArn", ""),
        "Tags": pipe.get("Tags", {}) or {},
        "CreationTime": int(pipe.get("CreationTime", 0)),
        "LastModifiedTime": int(pipe.get("LastModifiedTime", pipe.get("CreationTime", 0))),
        **{member: pipe[member] for member in _OPTIONAL_MEMBERS if pipe.get(member)},
    }


_OPTIONAL_MEMBERS = ("SourceParameters", "Enrichment", "EnrichmentParameters", "TargetParameters")


def _find_pipe_by_arn(arn):
    for pipe in _pipes.values():
        if pipe.get("Arn") == arn:
            return pipe
    return None


def _create_pipe(name, body):
    if name in _pipes:
        return _error(409, "ConflictException", f"Pipe {name} already exists.")
    source = body.get("Source", "")
    target = body.get("Target", "")
    role_arn = body.get("RoleArn", "")
    desired = str(body.get("DesiredState", "RUNNING")).upper()
    if desired not in _VALID_REQUESTED_STATE:
        return _error(
            400,
            "ValidationException",
            f"DesiredState must be one of {list(_VALID_REQUESTED_STATE)}.",
        )
    try:
        pipe = register_pipe(
            name=name,
            source=source,
            target=target,
            role_arn=role_arn,
            desired_state=desired,
            tags=body.get("Tags") or {},
            description=body.get("Description", "") or "",
            starting_position=_starting_position(body.get("SourceParameters")),
            source_parameters=body.get("SourceParameters") or None,
            enrichment=body.get("Enrichment", "") or "",
            enrichment_parameters=body.get("EnrichmentParameters") or None,
            target_parameters=body.get("TargetParameters") or None,
        )
    except ValueError as e:
        return _error(400, "ValidationException", str(e))
    return _json_resp(200, _lifecycle_response(pipe))


def _starting_position(source_parameters) -> str:
    params = (source_parameters or {}).get("DynamoDBStreamParameters") or {}
    return params.get("StartingPosition") or "LATEST"


def _describe_pipe(name):
    pipe = _pipes.get(name)
    if pipe is None:
        return _not_found(name)
    return _json_resp(200, _describe_response(pipe))


def _update_pipe(name, body):
    pipe = _pipes.get(name)
    if pipe is None:
        return _not_found(name)
    filter_error = _filter_criteria_error(body.get("SourceParameters"))
    if filter_error:
        return _error(400, "ValidationException", filter_error)
    if "Description" in body:
        pipe["Description"] = body.get("Description", "") or ""
    if "RoleArn" in body:
        pipe["RoleArn"] = body.get("RoleArn", "") or ""
    if "Target" in body and body.get("Target"):
        pipe["Target"] = body["Target"]
    _set_pipe_parameters(pipe, **{
        arg: body.get(member)
        for arg, member in (
            ("source_parameters", "SourceParameters"),
            ("enrichment", "Enrichment"),
            ("enrichment_parameters", "EnrichmentParameters"),
            ("target_parameters", "TargetParameters"),
        )
        if member in body
    })
    if "DesiredState" in body:
        desired = str(body.get("DesiredState", "")).upper()
        if desired not in _VALID_REQUESTED_STATE:
            return _error(
                400,
                "ValidationException",
                f"DesiredState must be one of {list(_VALID_REQUESTED_STATE)}.",
            )
        pipe["DesiredState"] = desired
        pipe["CurrentState"] = desired
    pipe["LastModifiedTime"] = int(time.time())
    _pipes[name] = pipe
    return _json_resp(200, _lifecycle_response(pipe))


def _delete_pipe_op(name):
    pipe = _pipes.get(name)
    if pipe is None:
        return _not_found(name)
    resp = {
        "Arn": pipe.get("Arn", ""),
        "Name": pipe.get("Name", ""),
        "DesiredState": "DELETED",
        "CurrentState": "DELETING",
        "CreationTime": int(pipe.get("CreationTime", 0)),
        "LastModifiedTime": int(pipe.get("LastModifiedTime", pipe.get("CreationTime", 0))),
    }
    delete_pipe(name)
    return _json_resp(200, resp)


def _set_state(name, desired):
    pipe = _pipes.get(name)
    if pipe is None:
        return _not_found(name)
    pipe["DesiredState"] = desired
    pipe["CurrentState"] = desired
    pipe["LastModifiedTime"] = int(time.time())
    _pipes[name] = pipe
    return _json_resp(200, _lifecycle_response(pipe))


def _list_pipes(query):
    name_prefix = _single(query.get("NamePrefix"))
    desired_state = _single(query.get("DesiredState"))
    current_state = _single(query.get("CurrentState"))
    source_prefix = _single(query.get("SourcePrefix"))
    target_prefix = _single(query.get("TargetPrefix"))
    limit = _single(query.get("Limit"))

    pipes = sorted(_pipes.values(), key=lambda p: p.get("Name", ""))
    result = []
    for pipe in pipes:
        if name_prefix and not pipe.get("Name", "").startswith(name_prefix):
            continue
        if desired_state and pipe.get("DesiredState") != desired_state:
            continue
        if current_state and pipe.get("CurrentState") != current_state:
            continue
        if source_prefix and not pipe.get("Source", "").startswith(source_prefix):
            continue
        if target_prefix and not pipe.get("Target", "").startswith(target_prefix):
            continue
        result.append(_summary(pipe))

    next_token = None
    if limit:
        try:
            n = int(limit)
            if 0 < n < len(result):
                next_token = result[n]["Name"]
                result = result[:n]
        except (TypeError, ValueError):
            pass

    body = {"Pipes": result}
    if next_token is not None:
        body["NextToken"] = next_token
    return _json_resp(200, body)


def _list_tags(arn):
    pipe = _find_pipe_by_arn(arn)
    if pipe is None:
        return _not_found(arn)
    return _json_resp(200, {"tags": pipe.get("Tags", {}) or {}})


def _tag_resource(arn, body):
    pipe = _find_pipe_by_arn(arn)
    if pipe is None:
        return _not_found(arn)
    pipe.setdefault("Tags", {}).update(body.get("tags", {}) or {})
    return _json_resp(200, {})


def _untag_resource(arn, query):
    pipe = _find_pipe_by_arn(arn)
    if pipe is None:
        return _not_found(arn)
    keys = query.get("tagKeys", [])
    if not isinstance(keys, list):
        keys = [keys]
    tags = pipe.setdefault("Tags", {})
    for k in keys:
        tags.pop(k, None)
    return _json_resp(200, {})


async def handle_request(method, path, headers, body_bytes, query_params):
    try:
        body = json.loads(body_bytes) if body_bytes else {}
    except (json.JSONDecodeError, TypeError):
        body = {}
    if not isinstance(body, dict):
        body = {}

    # Pipe lifecycle sub-actions: /v1/pipes/{Name}/start | /stop
    if path.startswith("/v1/pipes/"):
        rest = path[len("/v1/pipes/"):]
        if rest.endswith("/start"):
            return _set_state(unquote(rest[: -len("/start")]), "RUNNING")
        if rest.endswith("/stop"):
            return _set_state(unquote(rest[: -len("/stop")]), "STOPPED")
        name = unquote(rest)
        if name:
            if method == "POST":
                return _create_pipe(name, body)
            if method == "GET":
                return _describe_pipe(name)
            if method == "PUT":
                return _update_pipe(name, body)
            if method == "DELETE":
                return _delete_pipe_op(name)

    if path == "/v1/pipes" and method == "GET":
        return _list_pipes(query_params)

    # Tag routes: /tags/{resourceArn+}
    if path.startswith("/tags/"):
        arn = unquote(path[len("/tags/"):])
        if method == "GET":
            return _list_tags(arn)
        if method == "POST":
            return _tag_resource(arn, body)
        if method == "DELETE":
            return _untag_resource(arn, query_params)

    return _error(400, "ValidationException", f"No route for {method} {path}")
