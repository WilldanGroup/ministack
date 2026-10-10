# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""
AWS AppSync Service Emulator.

GraphQL API management service — REST/JSON protocol via /v1/apis/* paths.

Supports:
  GraphQL APIs:  CreateGraphQLApi, GetGraphQLApi, ListGraphQLApis,
                 UpdateGraphQLApi, DeleteGraphQLApi
  API Keys:      CreateApiKey, ListApiKeys, DeleteApiKey
  Data Sources:  CreateDataSource, GetDataSource, ListDataSources, DeleteDataSource
  Resolvers:     CreateResolver, GetResolver, ListResolvers, DeleteResolver
  Types:         CreateType, ListTypes, GetType
  Tags:          TagResource, UntagResource, ListTagsForResource
  Domain names:  CreateDomainName, GetDomainName, ListDomainNames,
                 UpdateDomainName, DeleteDomainName, AssociateApi,
                 GetApiAssociation, DisassociateApi

Wire protocol:
  REST/JSON — path-based routing under /v1/apis.
  Credential scope: appsync
"""

import asyncio
import base64
import copy
import json
import logging
import os
import re
import threading
import time

from ministack.core.arn import ArnParseError, parse_arn
from ministack.core.responses import (
    AccountRegionScopedDict,
    AccountScopedDict,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
    new_uuid,
    set_request_region,
)

logger = logging.getLogger("appsync")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

# ---------------------------------------------------------------------------
# In-memory state
# ---------------------------------------------------------------------------

_apis = AccountRegionScopedDict()            # apiId -> api record
_api_keys = AccountRegionScopedDict()        # apiId -> {keyId -> key record}
_data_sources = AccountRegionScopedDict()    # apiId -> {name -> data source record}
_resolvers = AccountRegionScopedDict()       # apiId -> {typeName -> {fieldName -> resolver record}}
_types = AccountRegionScopedDict()           # apiId -> {typeName -> type record}
_functions = AccountRegionScopedDict()       # apiId -> {functionId -> function record}
_schemas = AccountRegionScopedDict()         # apiId -> {"definition": str, "status": str, "details": str}
_caches = AccountRegionScopedDict()           # apiId -> ApiCache record
_domain_names = AccountRegionScopedDict()     # domainName -> DomainNameConfig record
_api_associations = AccountRegionScopedDict() # domainName -> ApiAssociation record
# apiId -> {cache key -> (expires_at, value)}. Separate from _caches, which holds
# the ApiCache configuration; this is the cached data itself. Not persisted: a
# restart is a cold cache, as replacing the cache instance would be on AWS.
# Read and written from the worker threads resolver execution runs on, so every
# access takes the lock — the expiry check-then-pop is not atomic without it.
_cache_entries: dict = {}
_cache_entries_lock = threading.Lock()
_tags = AccountScopedDict()            # resource_arn -> {key: value}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now():
    return int(time.time())


def _api_arn(api_id):
    return f"arn:aws:appsync:{get_region()}:{get_account_id()}:apis/{api_id}"


def _api_id_from_local_arn(arn):
    try:
        spec = parse_arn(arn)
    except ArnParseError:
        return None
    if spec.service != "appsync" or spec.account_id != get_account_id() or spec.region != get_region():
        return None

    prefix = "apis/"
    if not spec.resource.startswith(prefix):
        return None
    api_id = spec.resource[len(prefix):]
    return api_id if api_id and "/" not in api_id else None


def _validate_tag_resource_arn(arn):
    api_id = _api_id_from_local_arn(arn)
    api = _apis.get(api_id) if api_id else None
    if not api or api.get("arn") != arn:
        return error_response_json("NotFoundException", f"GraphQL API {api_id or arn} not found", 404)
    return None


def _select_api_region(api_id):
    """Select the unique stored region for an unsigned GraphQL data request."""
    if api_id in _apis:
        return True

    account_id = get_account_id()
    matches = [
        region
        for (stored_account, region, stored_api_id), _api in _apis.all_items()
        if stored_account == account_id and stored_api_id == api_id
    ]
    if len(matches) != 1:
        return False
    set_request_region(matches[0])
    return True


def _has_sigv4_credentials(headers, query_params):
    """Return whether the data request carries an explicit SigV4 region."""
    query_params = query_params or {}
    auth = headers.get("authorization") or headers.get("Authorization") or ""
    if auth.startswith("AWS4-HMAC-SHA256") and "Credential=" in auth:
        return True

    credential = (
        query_params.get("X-Amz-Credential")
        or query_params.get("x-amz-credential")
    )
    if isinstance(credential, (list, tuple)):
        credential = credential[0] if credential else ""
    return bool(credential)


def _json(status, body):
    return json_response(body, status)


# ---------------------------------------------------------------------------
# GraphQL APIs
# ---------------------------------------------------------------------------

def _create_graphql_api(body):
    api_id = new_uuid().replace("-", "")[:26]
    name = body.get("name", "")
    auth_type = body.get("authenticationType", "API_KEY")
    additional_auth = body.get("additionalAuthenticationProviders", [])
    log_config = body.get("logConfig")
    user_pool_config = body.get("userPoolConfig")
    openid_config = body.get("openIDConnectConfig")
    xray = body.get("xrayEnabled", False)
    tags = body.get("tags", {})
    lambda_auth = body.get("lambdaAuthorizerConfig")

    arn = _api_arn(api_id)
    now = _now()

    record = {
        "apiId": api_id,
        "name": name,
        "authenticationType": auth_type,
        "arn": arn,
        "uris": {
            "GRAPHQL": f"https://{api_id}.appsync-api.{get_region()}.amazonaws.com/graphql",
            "REALTIME": f"wss://{api_id}.appsync-realtime-api.{get_region()}.amazonaws.com/graphql",
        },
        "additionalAuthenticationProviders": additional_auth,
        "xrayEnabled": xray,
        # Fields with server-side defaults. Omitting them makes a Terraform plan
        # see api_type and visibility as newly set, and both force replacement —
        # so every plan wanted to recreate the API and all of its children.
        "apiType": body.get("apiType", "GRAPHQL"),
        "visibility": body.get("visibility", "GLOBAL"),
        "introspectionConfig": body.get("introspectionConfig", "ENABLED"),
        "queryDepthLimit": body.get("queryDepthLimit", 0),
        "resolverCountLimit": body.get("resolverCountLimit", 0),
        "wafWebAclArn": body.get("wafWebAclArn"),
        "createdAt": now,
        "lastUpdatedAt": now,
    }
    if log_config:
        record["logConfig"] = log_config
    if user_pool_config:
        record["userPoolConfig"] = user_pool_config
    if openid_config:
        record["openIDConnectConfig"] = openid_config
    if lambda_auth:
        record["lambdaAuthorizerConfig"] = lambda_auth

    _apis[api_id] = record
    _api_keys[api_id] = {}
    _data_sources[api_id] = {}
    _resolvers[api_id] = {}
    _types[api_id] = {}

    if tags:
        _tags[arn] = tags

    return _json(200, {"graphqlApi": _api_with_tags(record)})


def _api_with_tags(api):
    """AWS returns tags on the GraphqlApi itself, which is where the Terraform
    provider reads them. Merged in on read rather than copied onto the record so
    TagResource and UntagResource stay reflected without a second write."""
    return {**api, "tags": dict(_tags.get(api.get("arn", ""), {}))}


def _get_graphql_api(api_id):
    api = _apis.get(api_id)
    if not api:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)
    return _json(200, {"graphqlApi": _api_with_tags(api)})


def _list_graphql_apis(query_params):
    apis = [_api_with_tags(a) for a in _apis.values()]
    return _json(200, {"graphqlApis": apis})


def _update_graphql_api(api_id, body):
    api = _apis.get(api_id)
    if not api:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)

    if "name" in body:
        api["name"] = body["name"]
    if "authenticationType" in body:
        api["authenticationType"] = body["authenticationType"]
    if "additionalAuthenticationProviders" in body:
        api["additionalAuthenticationProviders"] = body["additionalAuthenticationProviders"]
    if "logConfig" in body:
        api["logConfig"] = body["logConfig"]
    if "userPoolConfig" in body:
        api["userPoolConfig"] = body["userPoolConfig"]
    if "openIDConnectConfig" in body:
        api["openIDConnectConfig"] = body["openIDConnectConfig"]
    if "xrayEnabled" in body:
        api["xrayEnabled"] = body["xrayEnabled"]
    if "lambdaAuthorizerConfig" in body:
        api["lambdaAuthorizerConfig"] = body["lambdaAuthorizerConfig"]

    api["lastUpdatedAt"] = _now()
    return _json(200, {"graphqlApi": api})


def _delete_graphql_api(api_id):
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)

    arn = _apis[api_id]["arn"]
    del _apis[api_id]
    _api_keys.pop(api_id, None)
    _data_sources.pop(api_id, None)
    _resolvers.pop(api_id, None)
    _types.pop(api_id, None)
    _functions.pop(api_id, None)
    _schemas.pop(api_id, None)
    _caches.pop(api_id, None)
    from ministack.core import appsync_graphql
    appsync_graphql.forget_schema(api_id)
    with _cache_entries_lock:
        _cache_entries.pop(api_id, None)
    _tags.pop(arn, None)

    return _json(200, {})


# ---------------------------------------------------------------------------
# API Keys
# ---------------------------------------------------------------------------

def _create_api_key(api_id, body):
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)

    key_id = "da2-" + new_uuid()[:26]
    now = _now()
    expires = body.get("expires", now + 604800)  # default 7 days
    description = body.get("description", "")

    record = {
        "id": key_id,
        "description": description,
        "expires": expires,
        "createdAt": now,
        "lastUpdatedAt": now,
        "deletes": expires + 5184000,  # 60 days after expiry
    }

    _api_keys.setdefault(api_id, {})[key_id] = record
    return _json(200, {"apiKey": record})


def _list_api_keys(api_id):
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)

    keys = list(_api_keys.get(api_id, {}).values())
    return _json(200, {"apiKeys": keys})


def _delete_api_key(api_id, key_id):
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)

    keys = _api_keys.get(api_id, {})
    if key_id not in keys:
        return error_response_json("NotFoundException", f"API key {key_id} not found", 404)

    del keys[key_id]
    return _json(200, {})


# ---------------------------------------------------------------------------
# Data Sources
# ---------------------------------------------------------------------------

def _create_data_source(api_id, body):
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)

    name = body.get("name", "")
    ds_type = body.get("type", "NONE")
    description = body.get("description", "")
    service_role_arn = body.get("serviceRoleArn", "")

    arn = f"{_apis[api_id]['arn']}/datasources/{name}"

    record = {
        "dataSourceArn": arn,
        "name": name,
        "type": ds_type,
        "description": description,
        "serviceRoleArn": service_role_arn,
        "createdAt": _now(),
        "lastUpdatedAt": _now(),
    }

    if ds_type == "AMAZON_DYNAMODB":
        record["dynamodbConfig"] = body.get("dynamodbConfig", {})
    elif ds_type == "AWS_LAMBDA":
        record["lambdaConfig"] = body.get("lambdaConfig", {})
    elif ds_type == "AMAZON_ELASTICSEARCH" or ds_type == "AMAZON_OPENSEARCH_SERVICE":
        record["elasticsearchConfig"] = body.get("elasticsearchConfig", {})
    elif ds_type == "HTTP":
        record["httpConfig"] = body.get("httpConfig", {})
    elif ds_type == "RELATIONAL_DATABASE":
        record["relationalDatabaseConfig"] = body.get("relationalDatabaseConfig", {})

    _data_sources.setdefault(api_id, {})[name] = record
    return _json(200, {"dataSource": record})


def _get_data_source(api_id, name):
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)

    ds = _data_sources.get(api_id, {}).get(name)
    if not ds:
        return error_response_json("NotFoundException", f"Data source {name} not found", 404)

    return _json(200, {"dataSource": ds})


def _list_data_sources(api_id):
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)

    sources = list(_data_sources.get(api_id, {}).values())
    return _json(200, {"dataSources": sources})


def _delete_data_source(api_id, name):
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)

    sources = _data_sources.get(api_id, {})
    if name not in sources:
        return error_response_json("NotFoundException", f"Data source {name} not found", 404)

    del sources[name]
    return _json(200, {})


# ---------------------------------------------------------------------------
# Resolvers
# ---------------------------------------------------------------------------

def _create_resolver(api_id, type_name, body):
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)

    field_name = body.get("fieldName", "")
    data_source_name = body.get("dataSourceName")
    request_template = body.get("requestMappingTemplate", "")
    response_template = body.get("responseMappingTemplate", "")
    kind = body.get("kind", "UNIT")
    pipeline_config = body.get("pipelineConfig")
    caching_config = body.get("cachingConfig")
    runtime = body.get("runtime")
    code = body.get("code")

    arn = f"{_apis[api_id]['arn']}/types/{type_name}/resolvers/{field_name}"

    record = {
        "typeName": type_name,
        "fieldName": field_name,
        "dataSourceName": data_source_name,
        "resolverArn": arn,
        "requestMappingTemplate": request_template,
        "responseMappingTemplate": response_template,
        "kind": kind,
        "createdAt": _now(),
        "lastUpdatedAt": _now(),
    }
    if pipeline_config:
        record["pipelineConfig"] = pipeline_config
    if caching_config:
        record["cachingConfig"] = caching_config
    if runtime:
        record["runtime"] = runtime
    if code:
        record["code"] = code

    _resolvers.setdefault(api_id, {}).setdefault(type_name, {})[field_name] = record
    return _json(200, {"resolver": record})


def _get_resolver(api_id, type_name, field_name):
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)

    resolver = _resolvers.get(api_id, {}).get(type_name, {}).get(field_name)
    if not resolver:
        return error_response_json("NotFoundException",
                                   f"Resolver {type_name}.{field_name} not found", 404)

    return _json(200, {"resolver": resolver})


def _list_resolvers(api_id, type_name):
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)

    resolvers = list(_resolvers.get(api_id, {}).get(type_name, {}).values())
    return _json(200, {"resolvers": resolvers})


def _delete_resolver(api_id, type_name, field_name):
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)

    type_resolvers = _resolvers.get(api_id, {}).get(type_name, {})
    if field_name not in type_resolvers:
        return error_response_json("NotFoundException",
                                   f"Resolver {type_name}.{field_name} not found", 404)

    del type_resolvers[field_name]
    return _json(200, {})


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

def _create_type(api_id, body):
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)

    definition = body.get("definition", "")
    fmt = body.get("format", "SDL")

    # Extract type name from SDL definition (e.g. "type Query { ... }" -> "Query")
    name_match = re.search(r"(?:type|input|enum|interface|union|scalar)\s+(\w+)", definition)
    type_name = name_match.group(1) if name_match else "Unknown"

    arn = f"{_apis[api_id]['arn']}/types/{type_name}"

    record = {
        "name": type_name,
        "description": body.get("description", ""),
        "arn": arn,
        "definition": definition,
        "format": fmt,
        "createdAt": _now(),
        "lastUpdatedAt": _now(),
    }

    _types.setdefault(api_id, {})[type_name] = record
    return _json(200, {"type": record})


def _get_type(api_id, type_name, query_params):
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)

    fmt = "SDL"
    if query_params.get("format"):
        fmt_val = query_params["format"]
        fmt = fmt_val[0] if isinstance(fmt_val, list) else fmt_val

    t = _types.get(api_id, {}).get(type_name)
    if not t:
        return error_response_json("NotFoundException", f"Type {type_name} not found", 404)

    return _json(200, {"type": t})


def _list_types(api_id, query_params):
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)

    types = list(_types.get(api_id, {}).values())
    return _json(200, {"types": types})


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

def _start_schema_creation(api_id, body):
    """StartSchemaCreation.

    AWS accepts the SDL, validates and compiles it asynchronously, and the caller
    polls GetSchemaCreationStatus until SUCCESS or FAILED. The definition arrives
    base64-encoded because it is a blob member.

    The SDL is stored verbatim rather than parsed: nothing here consumes a type
    graph — resolvers are addressed by type and field name, and _execute_graphql
    resolves fields against the registered resolvers — so parsing would add a
    dependency and a new failure mode without changing any behaviour. Compilation
    is therefore synchronous and always succeeds, and the status is reported the
    way a completed creation reports it.
    """
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)

    definition = body.get("definition", "")
    if not definition:
        return error_response_json("BadRequestException", "definition is required", 400)

    if isinstance(definition, str):
        try:
            definition = base64.b64decode(definition).decode("utf-8")
        except Exception:
            # Already-plain SDL: accept it rather than refusing a readable schema.
            pass
    elif isinstance(definition, (bytes, bytearray)):
        definition = definition.decode("utf-8", "replace")

    from ministack.core import appsync_graphql
    appsync_graphql.forget_schema(api_id)
    _schemas[api_id] = {
        "definition": definition,
        "status": "SUCCESS",
        "details": "Schema creation successful.",
    }
    logger.info("AppSync: schema created for %s (%d bytes)", api_id, len(definition))
    return _json(200, {"status": "PROCESSING"})


def _get_schema_creation_status(api_id):
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)
    schema = _schemas.get(api_id)
    if not schema:
        return _json(200, {"status": "NOT_APPLICABLE", "details": ""})
    return _json(200, {"status": schema["status"], "details": schema["details"]})


def _get_introspection_schema(api_id, query_params):
    """GetIntrospectionSchema — the response body is the schema blob itself.

    `format` is a required parameter with two values: SDL serves the stored
    definition verbatim, JSON serves the introspection query result built from
    it — the document `aws appsync get-introspection-schema --format JSON`
    hands to codegen tooling.
    """
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)
    schema = _schemas.get(api_id)
    if not schema:
        return error_response_json("GraphQLSchemaException", "No schema found for this API", 404)

    fmt = query_params.get("format")
    fmt = (fmt[0] if isinstance(fmt, list) else fmt or "").upper()
    if not fmt:
        return error_response_json("BadRequestException", "format is required", 400)
    if fmt not in ("SDL", "JSON"):
        return error_response_json("BadRequestException", f"Unsupported format: {fmt}", 400)

    if fmt == "JSON":
        from graphql.utilities import introspection_from_schema

        from ministack.core import appsync_graphql
        try:
            built = appsync_graphql.build_api_schema(api_id, schema["definition"])
        except appsync_graphql.SchemaUnavailable as exc:
            return error_response_json("GraphQLSchemaException", str(exc), 400)
        document = introspection_from_schema(built)
        return 200, {"Content-Type": "application/json"}, json.dumps(document).encode("utf-8")

    return 200, {"Content-Type": "application/octet-stream"}, schema["definition"].encode("utf-8")


def _is_ok(response):
    """True when a handler tuple carries a 2xx status."""
    return isinstance(response, tuple) and 200 <= response[0] < 300


def _update_data_source(api_id, name, body):
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)
    if name not in _data_sources.get(api_id, {}):
        return error_response_json("NotFoundException", f"Data source {name} not found", 404)
    created_at = _data_sources[api_id][name].get("createdAt")
    body = dict(body)
    body["name"] = name
    response = _create_data_source(api_id, body)
    if not _is_ok(response):
        return response
    # AWS keeps the original creation time across an update, so restore it on the
    # stored record and answer with that rather than the freshly stamped one.
    record = _data_sources[api_id][name]
    if created_at:
        record["createdAt"] = created_at
    return _json(200, {"dataSource": record})


def _update_resolver(api_id, type_name, field_name, body):
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)
    if field_name not in _resolvers.get(api_id, {}).get(type_name, {}):
        return error_response_json(
            "NotFoundException", f"Resolver {type_name}.{field_name} not found", 404)
    created_at = _resolvers[api_id][type_name][field_name].get("createdAt")
    body = dict(body)
    body["fieldName"] = field_name
    response = _create_resolver(api_id, type_name, body)
    if not _is_ok(response):
        return response
    record = _resolvers[api_id][type_name][field_name]
    if created_at:
        record["createdAt"] = created_at
    return _json(200, {"resolver": record})


def _update_type(api_id, type_name, body):
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)
    if type_name not in _types.get(api_id, {}):
        return error_response_json("NotFoundException", f"Type {type_name} not found", 404)
    if not body.get("format"):
        return error_response_json("BadRequestException", "format is required", 400)
    body = dict(body)
    body.setdefault("name", type_name)
    return _create_type(api_id, body)


def _update_api_key(api_id, key_id, body):
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)
    key = _api_keys.get(api_id, {}).get(key_id)
    if not key:
        return error_response_json("NotFoundException", f"API key {key_id} not found", 404)
    if body.get("description") is not None:
        key["description"] = body["description"]
    if body.get("expires") is not None:
        key["expires"] = int(body["expires"])
    return _json(200, {"apiKey": key})


def _put_environment_variables(api_id, body):
    """PutGraphqlApiEnvironmentVariables — replaces the whole map, as AWS does."""
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)
    env = body.get("environmentVariables")
    if env is None:
        return error_response_json("BadRequestException", "environmentVariables is required", 400)
    if not isinstance(env, dict):
        return error_response_json("BadRequestException", "environmentVariables must be a map", 400)
    _apis[api_id]["environmentVariables"] = env
    return _json(200, {"environmentVariables": env})


def _get_environment_variables(api_id):
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)
    return _json(200, {"environmentVariables": _apis[api_id].get("environmentVariables", {})})


# ---------------------------------------------------------------------------
# Pipeline functions
# ---------------------------------------------------------------------------

def _function_record(api_id, function_id, body):
    arn = f"{_apis[api_id]['arn']}/functions/{function_id}"
    record = {
        "functionId": function_id,
        "functionArn": arn,
        "name": body.get("name", ""),
        "description": body.get("description", ""),
        "dataSourceName": body.get("dataSourceName", ""),
        "requestMappingTemplate": body.get("requestMappingTemplate", ""),
        "responseMappingTemplate": body.get("responseMappingTemplate", ""),
        "functionVersion": body.get("functionVersion", "2018-05-29"),
    }
    for optional in ("syncConfig", "maxBatchSize", "runtime", "code"):
        if body.get(optional) is not None:
            record[optional] = body[optional]
    return record


def _create_function(api_id, body):
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)

    name = body.get("name")
    if not name:
        return error_response_json("BadRequestException", "name is required", 400)
    if not body.get("dataSourceName"):
        return error_response_json("BadRequestException", "dataSourceName is required", 400)

    function_id = new_uuid().replace("-", "")[:26]
    record = _function_record(api_id, function_id, body)
    _functions.setdefault(api_id, {})[function_id] = record
    return _json(200, {"functionConfiguration": record})


def _get_function(api_id, function_id):
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)
    record = _functions.get(api_id, {}).get(function_id)
    if not record:
        return error_response_json("NotFoundException", f"Function {function_id} not found", 404)
    return _json(200, {"functionConfiguration": record})


def _list_functions(api_id):
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)
    return _json(200, {"functions": list(_functions.get(api_id, {}).values())})


def _update_function(api_id, function_id, body):
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)
    if function_id not in _functions.get(api_id, {}):
        return error_response_json("NotFoundException", f"Function {function_id} not found", 404)
    if not body.get("name"):
        return error_response_json("BadRequestException", "name is required", 400)
    if not body.get("dataSourceName"):
        return error_response_json("BadRequestException", "dataSourceName is required", 400)
    record = _function_record(api_id, function_id, body)
    _functions[api_id][function_id] = record
    return _json(200, {"functionConfiguration": record})


def _delete_function(api_id, function_id):
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)
    if _functions.get(api_id, {}).pop(function_id, None) is None:
        return error_response_json("NotFoundException", f"Function {function_id} not found", 404)
    return _json(200, {})


def _tag_resource(body):
    arn = body.get("resourceArn", "")
    tags = body.get("tags", {})
    validation_error = _validate_tag_resource_arn(arn)
    if validation_error:
        return validation_error
    _tags.setdefault(arn, {}).update(tags)
    return _json(200, {})


def _untag_resource(arn, query_params):
    validation_error = _validate_tag_resource_arn(arn)
    if validation_error:
        return validation_error
    tag_keys = query_params.get("tagKeys", [])
    if isinstance(tag_keys, str):
        tag_keys = [tag_keys]
    existing = _tags.get(arn, {})
    for k in tag_keys:
        existing.pop(k, None)
    return _json(200, {})


def _list_tags_for_resource(arn):
    validation_error = _validate_tag_resource_arn(arn)
    if validation_error:
        return validation_error
    tags = _tags.get(arn, {})
    return _json(200, {"tags": tags})


# Enum values from the AppSync API reference (CreateApiCache).
_API_CACHING_BEHAVIORS = ("FULL_REQUEST_CACHING", "PER_RESOLVER_CACHING",
                          "OPERATION_LEVEL_CACHING")
_API_CACHE_TYPES = ("T2_SMALL", "T2_MEDIUM", "R4_LARGE", "R4_XLARGE", "R4_2XLARGE",
                    "R4_4XLARGE", "R4_8XLARGE", "SMALL", "MEDIUM", "LARGE", "XLARGE",
                    "LARGE_2X", "LARGE_4X", "LARGE_8X", "LARGE_12X")
_CACHE_HEALTH_METRICS = ("ENABLED", "DISABLED")


def _validate_cache_request(data):
    """ttl, apiCachingBehavior and type are required on create AND update, ttl
    within 1-3600, enums closed — AWS refuses rather than defaulting. Returns an
    error response, or None when the request is valid."""
    for member in ("ttl", "apiCachingBehavior", "type"):
        if data.get(member) is None:
            return error_response_json("BadRequestException", f"{member} is required", 400)
    try:
        ttl = int(data["ttl"])
    except (TypeError, ValueError):
        return error_response_json("BadRequestException", "ttl must be a number", 400)
    if not 1 <= ttl <= 3600:
        return error_response_json(
            "BadRequestException", "ttl must be between 1 and 3600 seconds", 400)
    if data["apiCachingBehavior"] not in _API_CACHING_BEHAVIORS:
        return error_response_json(
            "BadRequestException",
            f"Unknown apiCachingBehavior: {data['apiCachingBehavior']}", 400)
    if data["type"] not in _API_CACHE_TYPES:
        return error_response_json(
            "BadRequestException", f"Unknown cache type: {data['type']}", 400)
    if data.get("healthMetricsConfig") is not None \
            and data["healthMetricsConfig"] not in _CACHE_HEALTH_METRICS:
        return error_response_json(
            "BadRequestException",
            f"Unknown healthMetricsConfig: {data['healthMetricsConfig']}", 400)
    return None


def _cache_record(data, existing=None):
    """Build an ApiCache from a validated create or update request.

    atRestEncryptionEnabled and transitEncryptionEnabled are set at create time
    and cannot be changed afterwards, so an update carries them forward from the
    existing cache rather than defaulting them back to false.
    """
    base = existing or {}
    return {
        "ttl": int(data["ttl"]),
        "apiCachingBehavior": data["apiCachingBehavior"],
        "type": data["type"],
        "transitEncryptionEnabled": bool(
            base.get("transitEncryptionEnabled",
                     data.get("transitEncryptionEnabled", False))
        ),
        "atRestEncryptionEnabled": bool(
            base.get("atRestEncryptionEnabled",
                     data.get("atRestEncryptionEnabled", False))
        ),
        "healthMetricsConfig": data.get(
            "healthMetricsConfig", base.get("healthMetricsConfig", "DISABLED")),
        "status": "AVAILABLE",
    }


def _create_api_cache(api_id, data):
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)
    if _caches.get(api_id) is not None:
        return error_response_json(
            "BadRequestException", f"Cache already exists for API {api_id}", 400)
    invalid = _validate_cache_request(data)
    if invalid:
        return invalid
    record = _cache_record(data)
    _caches[api_id] = record
    return _json(200, {"apiCache": record})


def _get_api_cache(api_id):
    cache = _caches.get(api_id)
    if cache is None:
        return error_response_json(
            "NotFoundException", f"Cache not found for API {api_id}", 404)
    return _json(200, {"apiCache": cache})


def _update_api_cache(api_id, data):
    existing = _caches.get(api_id)
    if existing is None:
        return error_response_json(
            "NotFoundException", f"Cache not found for API {api_id}", 404)
    invalid = _validate_cache_request(data)
    if invalid:
        return invalid
    record = _cache_record(data, existing)
    _caches[api_id] = record
    return _json(200, {"apiCache": record})


def _delete_api_cache(api_id):
    if _caches.get(api_id) is None:
        return error_response_json(
            "NotFoundException", f"Cache not found for API {api_id}", 404)
    _caches.pop(api_id, None)
    with _cache_entries_lock:
        _cache_entries.pop(api_id, None)
    return _json(200, {})


def _flush_api_cache(api_id):
    if _caches.get(api_id) is None:
        return error_response_json(
            "NotFoundException", f"Cache not found for API {api_id}", 404)
    with _cache_entries_lock:
        _cache_entries.pop(api_id, None)
    return _json(200, {})


def _evaluate_code(body):
    """EvaluateCode — run a handler against a supplied context, without deploying.

    This is how AWS lets a resolver be tested before it exists on an API, and it
    is the operation a CI conformance check calls. Errors come back in the
    response rather than as a fault, because a resolver raising is a normal
    outcome of an evaluation.
    """
    runtime = body.get("runtime") or {}
    if runtime.get("name") != "APPSYNC_JS":
        return error_response_json(
            "BadRequestException",
            f"Unsupported runtime {runtime.get('name')!r}; only APPSYNC_JS is evaluated",
            400)
    code = body.get("code")
    if not code:
        return error_response_json("BadRequestException", "code is required", 400)

    raw_ctx = body.get("context")
    try:
        ctx = json.loads(raw_ctx) if isinstance(raw_ctx, str) else (raw_ctx or {})
    except (TypeError, ValueError):
        return error_response_json("BadRequestException", "context must be JSON", 400)
    if not isinstance(ctx, dict):
        return error_response_json("BadRequestException", "context must be an object", 400)
    # AppSync's ctx exposes arguments under both names.
    ctx.setdefault("arguments", ctx.get("args") or {})
    ctx.setdefault("args", ctx.get("arguments") or {})
    ctx.setdefault("stash", {})

    fn = body.get("function") or "request"
    from ministack.core import appsync_js
    try:
        status, value, appended, _stash, _skip = appsync_js.evaluate(code, fn, ctx)
    except appsync_js.AppSyncJsError as exc:
        return _json(200, {"error": {"message": str(exc),
                                     "codeErrors": []},
                           "logs": []})
    except appsync_js.AppSyncJsTimeout as exc:
        # A stuck evaluation is a result of the code under test, not a fault of
        # the service — report it in the response's error detail.
        return _json(200, {"error": {"message": str(exc), "codeErrors": []},
                           "logs": []})
    except RuntimeError as exc:
        return error_response_json("InternalFailureException", str(exc), 500)

    if status == "missing":
        return _json(200, {"error": {
            "message": f"code does not export {fn}()", "codeErrors": []}, "logs": []})
    return _json(200, {
        "evaluationResult": json.dumps(value),
        "logs": [f"appendError: {a.get('message')}" for a in appended],
    })


# ---------------------------------------------------------------------------
# Request router
# ---------------------------------------------------------------------------

# Path patterns for routing
_PATH_RE = re.compile(r"^/v1/apis(?:/([^/]+))?(?:/([^/]+))?(?:/([^/]+))?(?:/([^/]+))?(?:/([^/]+))?")
# /v1/apis                          -> groups: (None, None, None, None, None)
# /v1/apis/{apiId}                  -> groups: (apiId, None, None, None, None)
# /v1/apis/{apiId}/apikeys          -> groups: (apiId, "apikeys", None, None, None)
# /v1/apis/{apiId}/apikeys/{id}     -> groups: (apiId, "apikeys", id, None, None)
# /v1/apis/{apiId}/datasources      -> groups: (apiId, "datasources", None, None, None)
# /v1/apis/{apiId}/datasources/{n}  -> groups: (apiId, "datasources", name, None, None)
# /v1/apis/{apiId}/types            -> groups: (apiId, "types", None, None, None)
# /v1/apis/{apiId}/types/{t}/resolvers          -> (apiId, "types", t, "resolvers", None)
# /v1/apis/{apiId}/types/{t}/resolvers/{field}  -> (apiId, "types", t, "resolvers", field)


async def handle_request(method, path, headers, body, query_params):
    """Main entry point — route AppSync REST requests."""

    # AppSync Events Event APIs live under /v2/apis and share the
    # "appsync" credential scope with GraphQL, so delegate here instead of
    # teaching the central router to differentiate by credential scope.
    if path.startswith("/v2/apis") or path.startswith("/v2/tags"):
        from ministack.services import appsync_events
        return await appsync_events.handle_request(method, path, headers, body, query_params)

    # Tags endpoint: /v1/tags/{resourceArn}
    if path.startswith("/v1/tags/"):
        from urllib.parse import unquote
        arn = unquote(path[len("/v1/tags/"):])
        if method == "POST":
            data = json.loads(body) if body else {}
            data["resourceArn"] = arn
            return _tag_resource(data)
        elif method == "DELETE":
            return _untag_resource(arn, query_params)
        else:  # GET
            return _list_tags_for_resource(arn)

    if path == "/v1/dataplane-evaluatecode" and method == "POST":
        # Evaluation blocks on the Node worker; keep it off the event loop for
        # the same reason resolver execution runs on a thread.
        return await asyncio.to_thread(_evaluate_code, json.loads(body) if body else {})

    # GraphQL data plane: POST /graphql or POST /v1/apis/{apiId}/graphql
    if path == "/graphql" and method == "POST":
        api_key = headers.get("x-api-key", "")
        api_id = _resolve_api_by_key(
            api_key,
            allow_cross_region=not _has_sigv4_credentials(headers, query_params),
        )
        if not api_id:
            return error_response_json("UnauthorizedException", "Valid API key required", 401)
        data = json.loads(body) if body else {}
        # Resolver execution blocks — an HTTP or Lambda data source may call
        # back into ministack, and a nested request cannot be served while
        # the event loop waits on it. Run it on a worker thread.
        return await asyncio.to_thread(_execute_graphql, api_id, data, headers)

    if path.startswith("/v1/apis/") and path.endswith("/graphql") and method == "POST":
        parts = path.split("/")
        if len(parts) >= 5:
            api_id = parts[3]
            if not _has_sigv4_credentials(headers, query_params):
                _select_api_region(api_id)
            data = json.loads(body) if body else {}
            # Resolver execution blocks — an HTTP or Lambda data source may call
            # back into ministack, and a nested request cannot be served while
            # the event loop waits on it. Run it on a worker thread.
            return await asyncio.to_thread(_execute_graphql, api_id, data, headers)

    # Custom domain names: /v1/domainnames[/{domainName}[/apiassociation]]
    if path == "/v1/domainnames" or path.startswith("/v1/domainnames/"):
        return _route_domain_names(method, path, body)

    m = _PATH_RE.match(path)
    if not m:
        return error_response_json("NotFoundException", f"Unknown path: {path}", 404)

    api_id, sub1, sub2, sub3, sub4 = m.groups()

    data = {}
    if body:
        try:
            data = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError):
            data = {}

    # POST /v1/apis — CreateGraphQLApi
    if api_id is None and sub1 is None:
        if method == "POST":
            return _create_graphql_api(data)
        elif method == "GET":
            return _list_graphql_apis(query_params)

    # /v1/apis/{apiId}
    if api_id and sub1 is None:
        if method == "GET":
            return _get_graphql_api(api_id)
        elif method == "POST":
            return _update_graphql_api(api_id, data)
        elif method == "DELETE":
            return _delete_graphql_api(api_id)

    # /v1/apis/{apiId}/apikeys
    # ApiCaches: /v1/apis/{apiId}/ApiCaches[/update], and the separate
    # /FlushCache path. UpdateApiCache is a POST to .../ApiCaches/update, not a
    # PUT on the collection, so it has to be matched before the bare form.
    if sub1 == "ApiCaches":
        if sub2 == "update" and method == "POST":
            return _update_api_cache(api_id, data)
        if sub2 is None:
            if method == "POST":
                return _create_api_cache(api_id, data)
            if method == "GET":
                return _get_api_cache(api_id)
            if method == "DELETE":
                return _delete_api_cache(api_id)

    if sub1 == "FlushCache" and method == "DELETE":
        return _flush_api_cache(api_id)

    if sub1 == "apikeys":
        # Real SDKs route AppSync API key operations through the v1 path even
        # for Event APIs. If the id is not a GraphQL API but is an Event API,
        # hand the request to the Events service.
        if api_id and api_id not in _apis:
            from ministack.services import appsync_events
            if api_id in appsync_events._apis:
                if sub2 is None:
                    if method == "POST":
                        return appsync_events._create_api_key(api_id, body or b"{}")
                    elif method == "GET":
                        return appsync_events._list_api_keys(api_id, query_params)
                elif method == "DELETE":
                    return appsync_events._delete_api_key(api_id, sub2)
        if sub2 is None:
            if method == "POST":
                return _create_api_key(api_id, data)
            elif method == "GET":
                return _list_api_keys(api_id)
        else:
            # /v1/apis/{apiId}/apikeys/{keyId}
            if method == "POST":
                return _update_api_key(api_id, sub2, data)
            elif method == "DELETE":
                return _delete_api_key(api_id, sub2)

    # /v1/apis/{apiId}/environmentVariables
    if sub1 == "environmentVariables" and sub2 is None:
        if method == "PUT":
            return _put_environment_variables(api_id, data)
        elif method == "GET":
            return _get_environment_variables(api_id)

    # /v1/apis/{apiId}/schemacreation
    if sub1 == "schemacreation" and sub2 is None:
        if method == "POST":
            return _start_schema_creation(api_id, data)
        elif method == "GET":
            return _get_schema_creation_status(api_id)

    # /v1/apis/{apiId}/schema
    if sub1 == "schema" and sub2 is None and method == "GET":
        return _get_introspection_schema(api_id, query_params)

    # /v1/apis/{apiId}/functions
    if sub1 == "functions":
        if sub2 is None:
            if method == "POST":
                return _create_function(api_id, data)
            elif method == "GET":
                return _list_functions(api_id)
        else:
            # /v1/apis/{apiId}/functions/{functionId}
            if method == "GET":
                return _get_function(api_id, sub2)
            elif method == "POST":
                return _update_function(api_id, sub2, data)
            elif method == "DELETE":
                return _delete_function(api_id, sub2)

    # /v1/apis/{apiId}/datasources
    if sub1 == "datasources":
        if sub2 is None:
            if method == "POST":
                return _create_data_source(api_id, data)
            elif method == "GET":
                return _list_data_sources(api_id)
        else:
            # /v1/apis/{apiId}/datasources/{name}
            if method == "GET":
                return _get_data_source(api_id, sub2)
            elif method == "POST":
                return _update_data_source(api_id, sub2, data)
            elif method == "DELETE":
                return _delete_data_source(api_id, sub2)

    # /v1/apis/{apiId}/types
    if sub1 == "types":
        if sub2 is None:
            if method == "POST":
                return _create_type(api_id, data)
            elif method == "GET":
                return _list_types(api_id, query_params)
        elif sub3 == "resolvers":
            # /v1/apis/{apiId}/types/{typeName}/resolvers
            type_name = sub2
            if sub4 is None:
                if method == "POST":
                    return _create_resolver(api_id, type_name, data)
                elif method == "GET":
                    return _list_resolvers(api_id, type_name)
            else:
                # /v1/apis/{apiId}/types/{typeName}/resolvers/{fieldName}
                field_name = sub4
                if method == "GET":
                    return _get_resolver(api_id, type_name, field_name)
                elif method == "POST":
                    return _update_resolver(api_id, type_name, field_name, data)
                elif method == "DELETE":
                    return _delete_resolver(api_id, type_name, field_name)
        else:
            # /v1/apis/{apiId}/types/{typeName}
            if sub3 is None and method == "GET":
                return _get_type(api_id, sub2, query_params)
            if sub3 is None and method == "POST":
                return _update_type(api_id, sub2, data)

    return error_response_json("BadRequestException", f"Unsupported route: {method} {path}")


# ---------------------------------------------------------------------------
# Domain names and API associations
#
# A custom domain is a record: the certificate is not checked and nothing
# answers on the name, but the CloudFront-shaped appsyncDomainName and the
# hosted zone id are what a template aliases a Route 53 record at, and the
# association is what a client would resolve the domain to.
# ---------------------------------------------------------------------------

# CloudFront's hosted zone id: every AppSync custom domain is an alias into
# the distribution AppSync fronts it with.
_CLOUDFRONT_HOSTED_ZONE_ID = "Z2FDTNDATAQYW2"


def _domain_name_arn(domain_name):
    return f"arn:aws:appsync:{get_region()}:{get_account_id()}:domainnames/{domain_name}"


def _domain_name_config(record):
    out = dict(record)
    tags = _tags.get(record["domainNameArn"])
    if tags:
        out["tags"] = dict(tags)
    return out


def _create_domain_name(body):
    domain = body.get("domainName", "")
    certificate_arn = body.get("certificateArn", "")
    if not domain or not certificate_arn:
        return error_response_json("BadRequestException",
                                   "domainName and certificateArn are required", 400)
    if domain in _domain_names:
        return error_response_json("BadRequestException",
                                   f"Domain name {domain} already exists", 400)
    record = {
        "domainName": domain,
        "description": body.get("description", ""),
        "certificateArn": certificate_arn,
        # The fronting distribution's name, in CloudFront's shape.
        "appsyncDomainName": "d" + new_uuid().replace("-", "")[:13] + ".cloudfront.net",
        "hostedZoneId": _CLOUDFRONT_HOSTED_ZONE_ID,
        "domainNameArn": _domain_name_arn(domain),
    }
    _domain_names[domain] = record
    tags = body.get("tags") or {}
    if tags:
        _tags[record["domainNameArn"]] = dict(tags)
    return _json(200, {"domainNameConfig": _domain_name_config(record)})


def _get_domain_name(domain):
    record = _domain_names.get(domain)
    if record is None:
        return error_response_json("NotFoundException", f"Domain name {domain} not found", 404)
    return _json(200, {"domainNameConfig": _domain_name_config(record)})


def _list_domain_names():
    return _json(200, {"domainNameConfigs": [_domain_name_config(r) for r in _domain_names.values()]})


def _update_domain_name(domain, body):
    record = _domain_names.get(domain)
    if record is None:
        return error_response_json("NotFoundException", f"Domain name {domain} not found", 404)
    if "description" in body:
        record["description"] = body["description"]
    return _json(200, {"domainNameConfig": _domain_name_config(record)})


def _delete_domain_name(domain):
    record = _domain_names.get(domain)
    if record is None:
        return error_response_json("NotFoundException", f"Domain name {domain} not found", 404)
    if domain in _api_associations:
        return error_response_json("BadRequestException",
                                   f"Domain name {domain} is associated with an API", 400)
    del _domain_names[domain]
    _tags.pop(record["domainNameArn"], None)
    return _json(200, {})


def _associate_api(domain, body):
    if domain not in _domain_names:
        return error_response_json("NotFoundException", f"Domain name {domain} not found", 404)
    api_id = body.get("apiId", "")
    if api_id not in _apis:
        return error_response_json("NotFoundException", f"GraphQL API {api_id} not found", 404)
    association = {"domainName": domain, "apiId": api_id, "associationStatus": "SUCCESS"}
    _api_associations[domain] = association
    return _json(200, {"apiAssociation": dict(association)})


def _get_api_association(domain):
    association = _api_associations.get(domain)
    if association is None:
        return error_response_json("NotFoundException",
                                   f"Domain name {domain} has no API association", 404)
    return _json(200, {"apiAssociation": dict(association)})


def _disassociate_api(domain):
    if domain not in _domain_names:
        return error_response_json("NotFoundException", f"Domain name {domain} not found", 404)
    _api_associations.pop(domain, None)
    return _json(200, {})


def _route_domain_names(method, path, body):
    from urllib.parse import unquote
    parts = [unquote(p) for p in path[len("/v1/domainnames"):].split("/") if p]
    data = {}
    if body:
        try:
            data = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError):
            data = {}
    if not parts:
        if method == "POST":
            return _create_domain_name(data)
        if method == "GET":
            return _list_domain_names()
    elif len(parts) == 1:
        if method == "GET":
            return _get_domain_name(parts[0])
        if method == "POST":
            return _update_domain_name(parts[0], data)
        if method == "DELETE":
            return _delete_domain_name(parts[0])
    elif len(parts) == 2 and parts[1] == "apiassociation":
        if method == "POST":
            return _associate_api(parts[0], data)
        if method == "GET":
            return _get_api_association(parts[0])
        if method == "DELETE":
            return _disassociate_api(parts[0])
    return error_response_json("BadRequestException", f"Unsupported route: {method} {path}")


# ---------------------------------------------------------------------------
# State management
# ---------------------------------------------------------------------------

def reset():
    """Clear all in-memory state."""
    _apis.clear()
    _api_keys.clear()
    _data_sources.clear()
    _resolvers.clear()
    _types.clear()
    _functions.clear()
    _schemas.clear()
    _caches.clear()
    _domain_names.clear()
    _api_associations.clear()
    with _cache_entries_lock:
        _cache_entries.clear()
    # Drop the JS workers so their compiled-module cache does not outlive a
    # reset, and the built-schema cache with them.
    from ministack.core import appsync_graphql, appsync_js
    appsync_js.reset()
    appsync_graphql.forget_schema()
    _tags.clear()


def get_state():
    """Return a deep copy of all state for persistence."""
    return copy.deepcopy({
        "apis": _apis,
        "api_keys": _api_keys,
        "data_sources": _data_sources,
        "resolvers": _resolvers,
        "types": _types,
        "functions": _functions,
        "schemas": _schemas,
        "caches": _caches,
        "domain_names": _domain_names,
        "api_associations": _api_associations,
        "tags": _tags,
    })


def load_persisted_state(data):
    return _restore_state(data)


def _restore_state(data):
    """Restore state from persisted data."""
    reset()
    _apis.update(data.get("apis", {}))
    api_regions = {
        (account_id, api_id): region
        for (account_id, region, api_id), _api in _apis.all_items()
    }
    for store, key in (
        (_api_keys, "api_keys"),
        (_data_sources, "data_sources"),
        (_resolvers, "resolvers"),
        (_types, "types"),
        (_functions, "functions"),
        (_schemas, "schemas"),
        (_caches, "caches"),
    ):
        _restore_api_child_store(store, data.get(key, {}), api_regions)
    _domain_names.update(data.get("domain_names", {}))
    _api_associations.update(data.get("api_associations", {}))
    _tags.update(data.get("tags", {}))


def _restore_api_child_store(store, restored, api_regions):
    """Adopt legacy API children into their parent GraphQL API's region."""
    if isinstance(restored, AccountRegionScopedDict):
        store.update(restored)
        return

    if isinstance(restored, AccountScopedDict):
        items = restored._data.items()
    else:
        account_id = get_account_id()
        items = (((account_id, api_id), value) for api_id, value in restored.items())

    for (account_id, api_id), value in items:
        region = api_regions.get(
            (account_id, api_id),
            store._region_for_legacy_value(api_id, value),
        )
        store.set_scoped(account_id, region, api_id, value)


# ---------------------------------------------------------------------------
# GraphQL Data Plane — parse and execute queries against DynamoDB
# ---------------------------------------------------------------------------

import re as _re

# Simple GraphQL parser — handles queries/mutations that Amplify generates
# An operation may be anonymous and still declare variables, in which case the
# parenthesis follows the keyword with no space — `mutation($x: T!) { ... }`.
# That is what every SDK and generated client sends, so the whitespace after the
# keyword has to be optional; requiring it made the pattern miss, and the bare
# field fallback then read the whole document as one field named "mutation".
_GQL_OP_RE = _re.compile(
    r'\b(?:query|mutation|subscription)\b\s*(\w+)?\s*(?:\(([^)]*)\))?\s*\{(.*)\}',
    _re.DOTALL,
)
_GQL_FIELD_RE = _re.compile(r'(\w+)\s*(?:\(([^)]*)\))?\s*(?:\{([^}]*)\})?')


def _resolve_api_by_key(api_key_value, allow_cross_region=True):
    """Find the API ID that owns this API key."""
    for api_id, keys in _api_keys.items():
        for kid, key in keys.items():
            if kid == api_key_value or key.get("id") == api_key_value:
                return api_id

    if not allow_cross_region:
        # Signed requests may use the sole API in their credential region,
        # but must not discover an API stored in another region.
        if len(_apis) == 1:
            return next(iter(_apis))
        return None

    account_id = get_account_id()
    for (stored_account, region, api_id), keys in _api_keys.all_items():
        if stored_account != account_id:
            continue
        for kid, key in keys.items():
            if kid == api_key_value or key.get("id") == api_key_value:
                set_request_region(region)
                return api_id

    # Fallback: if only one API exists in this account, use its region.
    matches = [
        (region, api_id)
        for (stored_account, region, api_id), _api in _apis.all_items()
        if stored_account == account_id
    ]
    if len(matches) == 1:
        region, api_id = matches[0]
        set_request_region(region)
        return api_id
    return None


class _AuthorizerRejected(Exception):
    """Sentinel raised when the Lambda authorizer denies a request.

    AWS docs are explicit: an authorizer returning `isAuthorized:false`, an
    authorizer Lambda that's unreachable, or an authorizer that raises must all
    surface to the client as `UnauthorizedException` (HTTP 401). Callers catch
    this and emit the standard AppSync error envelope.
    """


def _invoke_lambda_authorizer(
    api_id, authorizer_config, request_headers,
    *,
    query: str = "",
    variables: dict | None = None,
    operation_name: str | None = None,
):
    """Invoke the Lambda authorizer.

    Returns the identity dict (`{}` when authorized with no `resolverContext`,
    or `{"resolverContext": {...}}` when present). Raises ``_AuthorizerRejected``
    for any failure mode AWS treats as unauthorized: missing authorizer Lambda,
    invocation error, malformed response, or ``isAuthorized:false``.

    AWS stores the authorizer Lambda under ``lambdaAuthorizerConfig.authorizerUri``
    (which ``_create_graphql_api`` persists verbatim). ministack's AppSync Events
    authorizer reads the same key.
    """
    func_arn = authorizer_config.get("authorizerUri") or authorizer_config.get("authorizer_uri")
    if not func_arn:
        # Misconfigured API (lambdaAuthorizerConfig present but no Uri). Treat
        # as authorized — this is a config error surface, not a per-request
        # rejection signal.
        return {}

    import ministack.services.lambda_svc as _lambda_svc

    func, func_config, func_name = _lambda_svc._get_func_record_for_ref(func_arn)
    if not func or not func_config:
        logger.warning("Lambda authorizer %s not found in ministack", func_arn)
        raise _AuthorizerRejected("authorizer Lambda not found")

    # AWS-verified authorizer event shape — apiId / accountId / requestId /
    # queryString / operationName / variables / requestHeaders all present per
    # the AppSync Developer Guide AWS_LAMBDA authorization section.
    authorizer_event = {
        "authorizationToken": request_headers.get("authorization", ""),
        "requestContext": {
            "apiId": api_id,
            "accountId": get_account_id(),
            "requestId": new_uuid(),
            "queryString": query,
            "operationName": operation_name or "unknown",
            "variables": variables or {},
        },
        "requestHeaders": request_headers,
    }

    try:
        exec_record = _lambda_svc._execution_record_for_config(func, func_config)
        result = _lambda_svc._execute_function_with_config_scope(exec_record, authorizer_event)
    except Exception as e:
        logger.warning("Lambda authorizer invocation failed: %s", e)
        raise _AuthorizerRejected("authorizer invocation failed") from e

    if not isinstance(result, dict) or result.get("error"):
        logger.warning("Lambda authorizer execution error")
        raise _AuthorizerRejected("authorizer execution error")

    body = result.get("body")
    if isinstance(body, str):
        try:
            body = json.loads(body)
        except Exception:
            raise _AuthorizerRejected("authorizer returned non-JSON body")
    if not isinstance(body, dict):
        raise _AuthorizerRejected("authorizer returned non-dict body")

    if not body.get("isAuthorized", False):
        logger.warning("Lambda authorizer rejected request")
        raise _AuthorizerRejected("isAuthorized=false")

    resolver_context = body.get("resolverContext")
    if resolver_context:
        return {"resolverContext": resolver_context}
    return {}


def _unauthorized_response():
    """AppSync's wire-shape for an authorizer rejection: HTTP 401 with a
    GraphQL `errors` envelope carrying `UnauthorizedException`."""
    return _json(401, {
        "errors": [{
            "errorType": "UnauthorizedException",
            "message": "You are not authorized to make this call.",
        }],
    })


def _auth_modes(api):
    """Every authentication type the API accepts."""
    modes = {api.get("authenticationType") or "API_KEY"}
    for extra in api.get("additionalAuthenticationProviders") or []:
        t = extra.get("authenticationType") if isinstance(extra, dict) else extra
        if t:
            modes.add(t)
    return modes


def _request_is_authenticated(api_id, api, request_headers):
    """Whether the request satisfies any of the API's configured providers.

    AppSync accepts a request if any one provider accepts it. Credentials are
    not verified here — ministack issued these tokens and does not check
    signatures — but a request carrying nothing at all matches no provider, and
    that is the case worth refusing: it is the one that makes an authorization
    test pass no matter what the resolvers do.
    """
    headers = {str(k).lower(): v for k, v in (request_headers or {}).items()}
    modes = _auth_modes(api)
    auth = str(headers.get("authorization") or "")

    if "API_KEY" in modes:
        supplied = headers.get("x-api-key")
        if supplied and supplied in (_api_keys.get(api_id) or {}):
            return True
    if "AMAZON_COGNITO_USER_POOLS" in modes:
        # A bearer token, as distinct from a SigV4 credential.
        if auth and not auth.startswith("AWS4-HMAC-SHA256"):
            return True
    if "OPENID_CONNECT" in modes and auth and not auth.startswith("AWS4-HMAC-SHA256"):
        # Under AUTH, only a token one of the API's issuers verifies; without
        # it, any bearer, as before.
        if not _verifying() or _oidc_identity(api, request_headers) is not None:
            return True
    if "AWS_IAM" in modes and auth.startswith("AWS4-HMAC-SHA256"):
        return True
    if "AWS_LAMBDA" in modes and api.get("lambdaAuthorizerConfig"):
        # The authorizer runs below and may still reject.
        return True
    return False


def _execute_with_schema(api_id, sdl, query, variables, operation_name, request_headers):
    """Execute against the API's schema with the real GraphQL algorithm."""
    from ministack.core import appsync_graphql

    api = _apis.get(api_id, {})
    identity = None
    if api.get("userPoolConfig"):
        identity = _cognito_identity(api, request_headers)
    if identity is None:
        identity = _oidc_identity(api, request_headers)
    if api.get("lambdaAuthorizerConfig"):
        try:
            identity = _invoke_lambda_authorizer(
                api_id, api["lambdaAuthorizerConfig"], request_headers,
                query=query, variables=variables, operation_name=operation_name)
        except _AuthorizerRejected:
            return _unauthorized_response()

    appended = []

    def field_resolver(source, info, **args):
        """Called for every field graphql-core resolves."""
        type_name = info.parent_type.name
        field_name = info.field_name

        resolver = (_resolvers.get(api_id) or {}).get(type_name, {}).get(field_name)
        if resolver is None:
            # No resolver on this field: read it off the parent, which is how
            # AppSync resolves a plain attribute of a returned object.
            if isinstance(source, dict):
                return source.get(field_name)
            return getattr(source, field_name, None)

        return _resolve_field(
            api_id, resolver, args, [], variables,
            field_name=field_name,
            identity=identity,
            request_headers=request_headers,
            source=source if isinstance(source, dict) else {},
            appended_errors=appended,
            type_name=type_name,
        )

    try:
        data, errors = appsync_graphql.execute(
            api_id, sdl, query, variables, operation_name, field_resolver, None)
    except appsync_graphql.SchemaUnavailable as exc:
        return _json(200, {"data": None, "errors": [{"message": str(exc)}]})

    # util.appendError is non-fatal and rides alongside the data.
    for a in appended:
        errors.append({"message": a.get("message"), "errorType": a.get("errorType")})

    body = {"data": data}
    if errors:
        body["errors"] = errors
    return _json(200, body)


def _execute_graphql(api_id, data, request_headers=None):
    """Execute a GraphQL query/mutation against the configured resolvers."""
    query = data.get("query", "")
    variables = data.get("variables", {})
    operation_name = data.get("operationName")

    if not query.strip():
        return _json(400, {"errors": [{"message": "Query is required"}]})

    if api_id not in _apis:
        return _json(404, {"errors": [{"message": f"API {api_id} not found"}]})

    # Applied before either execution path, so it does not depend on whether
    # the API has a schema. AWS refuses a request that satisfies none of the
    # API's configured auth providers; matching that is what makes an
    # authorization test against ministack meaningful.
    if not _request_is_authenticated(
            api_id, _apis.get(api_id, {}), request_headers or {}):
        return _unauthorized_response()

    # A schema means the real engine: parse, validate, execute. The regex path
    # below is kept only for an API that has no schema yet, where there is
    # nothing to validate against.
    sdl = (_schemas.get(api_id) or {}).get("definition")
    if sdl:
        return _execute_with_schema(
            api_id, sdl, query, variables, operation_name, request_headers or {})

    # Parse the top-level operation
    # Strip __typename fields — Amplify adds these everywhere
    query_clean = _re.sub(r'__typename\s*', '', query)

    m = _GQL_OP_RE.search(query_clean)
    if not m:
        # Try bare field query: { getUser(id: "1") { name } }
        inner = query_clean.strip().strip("{}")
        fields = _parse_fields(inner, variables)
    else:
        op_name, op_args, body = m.groups()
        fields = _parse_fields(body, variables)

    # Determine operation type
    is_mutation = query_clean.strip().startswith("mutation")

    # Determine identity from the Lambda authorizer (AWS_LAMBDA auth mode).
    # AWS contract: a rejected authorizer must surface as UnauthorizedException
    # (HTTP 401), not as a HTTP 200 with identity=null.
    identity = None
    api = _apis.get(api_id, {})
    # A Cognito-authenticated API populates identity from the caller's token.
    # Resolvers read identity.sub to decide what the caller may see, so leaving
    # it null makes every permission check fail against an API that works on AWS.
    if api.get("userPoolConfig"):
        identity = _cognito_identity(api, request_headers or {})
    if identity is None:
        identity = _oidc_identity(api, request_headers or {})
    if api.get("lambdaAuthorizerConfig"):
        try:
            identity = _invoke_lambda_authorizer(
                api_id, api["lambdaAuthorizerConfig"], request_headers or {},
                query=query,
                variables=variables,
                operation_name=operation_name,
            )
        except _AuthorizerRejected:
            return _unauthorized_response()

    results = {}
    errors = []
    for field_name, args, sub_fields in fields:
        resolver = _find_resolver(api_id, "Mutation" if is_mutation else "Query", field_name)
        if resolver:
            # A mutation is never served from cache, and never populates it.
            cache_hit = None if is_mutation else _cache_key_for(
                api_id, resolver, field_name, args, identity, {})
            if cache_hit:
                cached = _cache_get(api_id, cache_hit[0])
                if cached is not None:
                    results[field_name] = cached
                    continue
            try:
                appended = []
                result = _resolve_field(
                    api_id, resolver, args, sub_fields, variables,
                    field_name=field_name,
                    identity=identity,
                    request_headers=request_headers or {},
                    source={},
                    appended_errors=appended,
                )
                # Resolve the fields of whatever this field returned — the
                # resolvers attached to its own type, with it as ctx.source.
                field_types = _schema_field_types(api_id)
                child_type = field_types.get(
                    f"{'Mutation' if is_mutation else 'Query'}.{field_name}")
                if child_type and sub_fields:
                    result = _resolve_selection(
                        api_id, child_type, result, sub_fields, variables,
                        identity, request_headers or {}, errors)
                results[field_name] = result
                # util.appendError is non-fatal: the field still resolves and
                # the errors ride alongside the data, as AppSync does.
                for a in appended:
                    errors.append({"message": a.get("message"),
                                   "errorType": a.get("errorType"),
                                   "path": [field_name]})
                # Only a successful result is cached; caching an error would
                # make a transient failure stick for the whole ttl.
                if cache_hit and result is not None:
                    _cache_put(api_id, cache_hit[0], cache_hit[1], result)
            except _AppSyncResolverError as e:
                # util.error and the executor's own refusals carry an errorType,
                # which clients switch on.
                errors.append({"message": str(e), "errorType": e.error_type,
                               "path": [field_name],
                               **({"data": e.data} if e.data is not None else {})})
                results[field_name] = None
            except Exception as e:
                errors.append({"message": str(e), "path": [field_name]})
                results[field_name] = None
        else:
            # No resolver — return mock empty result
            results[field_name] = None

    response = {"data": results}
    if errors:
        response["errors"] = errors
    return _json(200, response)


def _parse_fields(body, variables):
    """Parse GraphQL field selections into (name, args_dict, sub_fields) tuples."""
    fields = []
    for m in _GQL_FIELD_RE.finditer(body.strip()):
        name = m.group(1)
        args_str = m.group(2) or ""
        sub = m.group(3) or ""
        args = _parse_args(args_str, variables)
        sub_fields = [s.strip() for s in sub.split() if s.strip() and s.strip() != "__typename"]
        fields.append((name, args, sub_fields))
    return fields


def _parse_args(args_str, variables):
    """Parse GraphQL arguments like (id: "1") or (id: $id) into a dict."""
    args = {}
    if not args_str.strip():
        return args
    # Match key: value pairs
    for pair in _re.finditer(r'(\w+)\s*:\s*("(?:[^"\\]|\\.)*"|\$\w+|\d+(?:\.\d+)?|true|false|null|\{[^}]*\}|\[[^\]]*\])', args_str):
        key = pair.group(1)
        val = pair.group(2)
        if val.startswith("$"):
            val = variables.get(val[1:], val)
        elif val.startswith('"') and val.endswith('"'):
            val = val[1:-1]
        elif val == "true":
            val = True
        elif val == "false":
            val = False
        elif val == "null":
            val = None
        elif val.startswith("{") and val.endswith("}"):
            val = _parse_args(val[1:-1], variables)
        elif val.startswith("[") and val.endswith("]"):
            val = val  # Keep as string for now
        elif val.replace(".", "").isdigit():
            val = float(val) if "." in val else int(val)
        args[key] = val
    return args


def _cache_key_for(api_id, resolver, field_name, args, identity, source):
    """Build a cache key for this field call, or None when it must not be cached.

    Mirrors AppSync: under PER_RESOLVER_CACHING only a resolver carrying a
    cachingConfig ttl is cached; under FULL_REQUEST_CACHING every resolver is,
    using the cache's own ttl. A caching key that cannot be resolved disables
    caching for the call rather than collapsing to a shared entry — two callers
    sharing an entry they should not is worse than not caching.
    """
    cache_cfg = _caches.get(api_id)
    if not cache_cfg:
        return None
    behavior = cache_cfg.get("apiCachingBehavior", "FULL_REQUEST_CACHING")
    resolver_cfg = (resolver or {}).get("cachingConfig") or {}

    if behavior == "PER_RESOLVER_CACHING":
        ttl = resolver_cfg.get("ttl")
        if not ttl:
            return None
    else:
        ttl = cache_cfg.get("ttl")
        if not ttl:
            return None

    ctx = {"arguments": args or {}, "args": args or {},
           "identity": identity or {}, "source": source or {}}
    parts = []
    for expr in resolver_cfg.get("cachingKeys") or []:
        path = expr.split(".")
        if path and path[0].lstrip("$") in ("context", "ctx"):
            path = path[1:]
        cur = ctx
        for seg in path:
            if not isinstance(cur, dict) or seg not in cur:
                return None
            cur = cur[seg]
        parts.append(f"{expr}={json.dumps(cur, sort_keys=True, default=str)}")
    return f"{field_name}|" + "&".join(parts), int(ttl)


def _cache_get(api_id, key):
    with _cache_entries_lock:
        entry = _cache_entries.get(api_id, {}).get(key)
        if not entry:
            return None
        expires_at, value = entry
        if expires_at <= time.time():
            _cache_entries.get(api_id, {}).pop(key, None)
            return None
        return copy.deepcopy(value)


def _cache_put(api_id, key, ttl, value):
    with _cache_entries_lock:
        _cache_entries.setdefault(api_id, {})[key] = (time.time() + ttl, copy.deepcopy(value))


def _find_resolver(api_id, type_name, field_name):
    """Find a resolver for Query.fieldName or Mutation.fieldName."""
    resolvers = _resolvers.get(api_id, {})
    # Try exact match
    if type_name in resolvers and field_name in resolvers[type_name]:
        return resolvers[type_name][field_name]
    # Try generic match (some setups use "Query" or "Mutation" type)
    for tn in resolvers:
        if field_name in resolvers[tn]:
            return resolvers[tn][field_name]
    return None


# ---------------------------------------------------------------------------
# Resolver execution
#
# Two registries rather than an elif chain, so a new runtime or data source type
# is a new entry rather than a new branch in the middle of the executor.
# ---------------------------------------------------------------------------


def _ds_none(api_id, data_source, request_obj, ctx):
    """A NONE data source echoes the request's payload — this is how AppSync
    supports resolvers that compute their whole answer locally."""
    if isinstance(request_obj, dict):
        return request_obj.get("payload")
    return None


def _ds_http(api_id, data_source, request_obj, ctx):
    """An HTTP data source performs the request the resolver described.

    AppSync puts {statusCode, headers, body} into ctx.result, with body as a
    string — a resolver parses it itself, so it must not be decoded here.
    """
    import urllib.error
    import urllib.request

    cfg = data_source.get("httpConfig", {})
    endpoint = (cfg.get("endpoint") or "").rstrip("/")
    req = request_obj if isinstance(request_obj, dict) else {}
    params = req.get("params") or {}
    url = f"{endpoint}{req.get('resourcePath', '/')}"
    body = params.get("body")
    if isinstance(body, str):
        body = body.encode()

    http_req = urllib.request.Request(
        url, data=body, method=req.get("method", "POST"),
        headers={k: str(v) for k, v in (params.get("headers") or {}).items()})
    try:
        with urllib.request.urlopen(http_req, timeout=15) as resp:
            return {"statusCode": resp.status,
                    "headers": dict(resp.headers.items()),
                    "body": resp.read().decode("utf-8", errors="replace")}
    except urllib.error.HTTPError as exc:
        # A non-2xx is a result the resolver inspects, not a failure — AppSync
        # hands the status back rather than raising.
        return {"statusCode": exc.code, "headers": dict(exc.headers.items()),
                "body": exc.read().decode("utf-8", errors="replace")}
    except Exception as exc:
        raise _AppSyncResolverError(f"HTTP data source request failed: {exc}",
                                    "HttpDataSourceError") from exc


def _ds_dynamodb(api_id, data_source, request_obj, ctx):
    """Run the operation the resolver asked for.

    The non-JS path infers an operation from the field name because it has no
    request object to read; here the resolver said what it wanted, so honour it.
    Runs in the data source's declared region, as the non-JS path does — the
    table lives where dynamodbConfig.awsRegion says, not where the request came
    in.
    """
    cfg = data_source.get("dynamodbConfig", {})
    request_region = get_region()
    data_source_region = cfg.get("awsRegion") or request_region
    set_request_region(data_source_region)
    try:
        return _ds_dynamodb_in_region(api_id, data_source, request_obj, ctx)
    finally:
        set_request_region(request_region)


def _ds_dynamodb_in_region(api_id, data_source, request_obj, ctx):
    """A resolver's DynamoDB request as AppSync runs it.

    Each operation of AppSync's DynamoDB request reference is translated to the
    DynamoDB API call it names and run by this emulator's own DynamoDB, so a
    key condition, an index, a filter, a limit, an update expression, a
    condition and a page token mean what they mean there; the answer is put in
    AppSync's shape, plain values rather than AttributeValues.
    """
    cfg = data_source.get("dynamodbConfig", {})
    table_name = cfg.get("tableName", "")
    req = request_obj if isinstance(request_obj, dict) else {}
    op = req.get("operation", "GetItem")
    run = _APPSYNC_DDB_OPERATIONS.get(op)
    if run is None:
        raise _AppSyncResolverError(
            f"DynamoDB operation {op} is not supported yet", "NotImplemented")
    return run(table_name, req)


def _ddb_call(action, data):
    """One call of this emulator's DynamoDB API: (answer, None) or (None, (code, message, answer))."""
    import ministack.services.dynamodb as _ddb
    handler = {
        "GetItem": _ddb._get_item,
        "PutItem": _ddb._put_item,
        "UpdateItem": _ddb._update_item,
        "DeleteItem": _ddb._delete_item,
        "Query": _ddb._query,
        "Scan": _ddb._scan,
        "BatchGetItem": _ddb._batch_get_item,
        "TransactWriteItems": _ddb._transact_write_items,
    }[action]
    status, _headers, body = handler(data)
    text = body.decode("utf-8") if isinstance(body, bytes) else body
    answer = json.loads(text) if text else {}
    if status >= 400:
        code = str(answer.get("__type") or "DynamoDbException").split("#")[-1]
        return None, (code, answer.get("message") or answer.get("Message") or code, answer)
    return answer, None


def _ddb_refusal(failure, result=None):
    """A DynamoDB refusal as AppSync reports it: its error type is the
    exception's name under `DynamoDB:`."""
    code, message, _answer = failure
    return _AppSyncResolverError(message, f"DynamoDB:{code}", result=result)


def _ddb_expression(data, expression, field):
    """An AppSync expression block — expression, expressionNames,
    expressionValues — as the request's `<field>` beside the names and values
    every expression of one request shares."""
    if not isinstance(expression, dict) or not expression.get("expression"):
        return
    data[field] = expression["expression"]
    if expression.get("expressionNames"):
        data.setdefault("ExpressionAttributeNames", {}).update(expression["expressionNames"])
    if expression.get("expressionValues"):
        data.setdefault("ExpressionAttributeValues", {}).update(expression["expressionValues"])


def _ddb_page_token(token):
    """AppSync's opaque page token, which carries DynamoDB's last evaluated key."""
    if not token:
        return None
    try:
        return json.loads(base64.urlsafe_b64decode(token.encode() + b"==").decode())
    except Exception as exc:
        raise _AppSyncResolverError(
            f"Invalid pagination token: {token}", "DynamoDB:ValidationException") from exc


def _ddb_next_token(last_key):
    if not last_key:
        return None
    return base64.urlsafe_b64encode(json.dumps(last_key, sort_keys=True).encode()).decode().rstrip("=")


def _ddb_get(table_name, key, consistent=True):
    answer, _failure = _ddb_call(
        "GetItem", {"TableName": table_name, "Key": key, "ConsistentRead": consistent})
    item = (answer or {}).get("Item")
    return _ddb_plain(item) if item else None


def _ddb_op_get_item(table_name, req):
    data = {"TableName": table_name, "Key": req.get("key") or {}}
    if req.get("consistentRead") is not None:
        data["ConsistentRead"] = bool(req["consistentRead"])
    _ddb_projection(data, req.get("projection"))
    answer, failure = _ddb_call("GetItem", data)
    if failure:
        raise _ddb_refusal(failure)
    item = answer.get("Item")
    return _ddb_plain(item) if item else None


def _ddb_projection(data, projection):
    if isinstance(projection, dict) and projection.get("expression"):
        data["ProjectionExpression"] = projection["expression"]
        if projection.get("expressionNames"):
            data.setdefault("ExpressionAttributeNames", {}).update(projection["expressionNames"])


def _ddb_condition_failed(table_name, key, condition, wanted):
    """A failed condition as AppSync handles it: the item read again, and the
    write answered as done where what stands is what it wanted to leave —
    the item it would have put, apart from `equalsIgnore`, or no item for a
    delete — and refused otherwise, under the Reject strategy, with the item
    read handed to response() as ctx.result beside the error. An update is
    never answered as done: AppSync cannot tell what it wanted to leave."""
    current = _ddb_get(table_name, key, (condition or {}).get("consistentRead", True))
    ignore = set((condition or {}).get("equalsIgnore") or [])
    if wanted == "absent":
        if current is None:
            return True, None
    elif wanted is not None and current is not None:
        strip = lambda item: {k: v for k, v in item.items() if k not in ignore}
        if strip(current) == strip(_ddb_plain(wanted)):
            return True, current
    return False, current


def _ddb_op_put_item(table_name, req):
    key = req.get("key") or {}
    item = {**(req.get("attributeValues") or {}), **key}
    data = {"TableName": table_name, "Item": item}
    _ddb_expression(data, req.get("condition"), "ConditionExpression")
    answer, failure = _ddb_call("PutItem", data)
    if failure:
        if failure[0] == "ConditionalCheckFailedException":
            done, current = _ddb_condition_failed(table_name, key, req.get("condition"), item)
            if done:
                return current
            raise _ddb_refusal(failure, result=current)
        raise _ddb_refusal(failure)
    return _ddb_plain(item)


def _ddb_op_update_item(table_name, req):
    key = req.get("key") or {}
    data = {"TableName": table_name, "Key": key, "ReturnValues": "ALL_NEW"}
    _ddb_expression(data, req.get("update"), "UpdateExpression")
    _ddb_expression(data, req.get("condition"), "ConditionExpression")
    answer, failure = _ddb_call("UpdateItem", data)
    if failure:
        if failure[0] == "ConditionalCheckFailedException":
            _done, current = _ddb_condition_failed(table_name, key, req.get("condition"), None)
            raise _ddb_refusal(failure, result=current)
        raise _ddb_refusal(failure)
    attributes = answer.get("Attributes")
    return _ddb_plain(attributes) if attributes else None


def _ddb_op_delete_item(table_name, req):
    key = req.get("key") or {}
    data = {"TableName": table_name, "Key": key, "ReturnValues": "ALL_OLD"}
    _ddb_expression(data, req.get("condition"), "ConditionExpression")
    answer, failure = _ddb_call("DeleteItem", data)
    if failure:
        if failure[0] == "ConditionalCheckFailedException":
            done, current = _ddb_condition_failed(table_name, key, req.get("condition"), "absent")
            if done:
                return None
            raise _ddb_refusal(failure, result=current)
        raise _ddb_refusal(failure)
    attributes = answer.get("Attributes")
    return _ddb_plain(attributes) if attributes else None


def _ddb_op_read_many(action):
    def run(table_name, req):
        data = {"TableName": table_name}
        if req.get("index"):
            data["IndexName"] = req["index"]
        if action == "Query":
            _ddb_expression(data, req.get("query"), "KeyConditionExpression")
            if req.get("scanIndexForward") is not None:
                data["ScanIndexForward"] = bool(req["scanIndexForward"])
        else:
            if req.get("segment") is not None:
                data["Segment"] = req["segment"]
            if req.get("totalSegments") is not None:
                data["TotalSegments"] = req["totalSegments"]
        _ddb_expression(data, req.get("filter"), "FilterExpression")
        _ddb_projection(data, req.get("projection"))
        if req.get("limit") is not None:
            data["Limit"] = int(req["limit"])
        if req.get("consistentRead") is not None:
            data["ConsistentRead"] = bool(req["consistentRead"])
        if req.get("select"):
            data["Select"] = req["select"]
        start = _ddb_page_token(req.get("nextToken"))
        if start:
            data["ExclusiveStartKey"] = start
        answer, failure = _ddb_call(action, data)
        if failure:
            raise _ddb_refusal(failure)
        return {
            "items": [_ddb_plain(item) for item in answer.get("Items") or []],
            "nextToken": _ddb_next_token(answer.get("LastEvaluatedKey")),
            "scannedCount": answer.get("ScannedCount", 0),
        }
    return run


def _ddb_op_batch_get_item(_table_name, req):
    """Each table's items in the order its keys were asked, null where a key
    holds none, and the keys DynamoDB left unprocessed."""
    asked = {}
    request_items = {}
    for table, spec in (req.get("tables") or {}).items():
        spec = {"keys": spec} if isinstance(spec, list) else (spec or {})
        keys = spec.get("keys") or []
        asked[table] = keys
        entry = {"Keys": keys}
        if spec.get("consistentRead") is not None:
            entry["ConsistentRead"] = bool(spec["consistentRead"])
        _ddb_projection(entry, spec.get("projection"))
        if keys:
            request_items[table] = entry
    answer = {}
    if request_items:
        answer, failure = _ddb_call("BatchGetItem", {"RequestItems": request_items})
        if failure:
            raise _ddb_refusal(failure)
    data, unprocessed = {}, {}
    for table, keys in asked.items():
        found = [_ddb_plain(item) for item in (answer.get("Responses") or {}).get(table) or []]
        rows = []
        for key in keys:
            plain_key = _ddb_plain(key)
            rows.append(next((item for item in found
                              if all(item.get(k) == v for k, v in plain_key.items())), None))
        data[table] = rows
        left = ((answer.get("UnprocessedKeys") or {}).get(table) or {}).get("Keys") or []
        unprocessed[table] = [_ddb_plain(key) for key in left]
    return {"data": data, "unprocessedKeys": unprocessed}


_TRANSACT_SHAPES = {
    "PutItem": "Put",
    "UpdateItem": "Update",
    "DeleteItem": "Delete",
    "ConditionCheck": "ConditionCheck",
}


def _ddb_op_transact_write_items(_table_name, req):
    """Every item written or none: their keys in order, or AppSync's
    cancellation reasons in ctx.result beside the error."""
    items = req.get("transactItems") or []
    transact = []
    for entry in items:
        shape = _TRANSACT_SHAPES.get(entry.get("operation"))
        if shape is None:
            raise _AppSyncResolverError(
                f"TransactWriteItems operation {entry.get('operation')} is not supported",
                "DynamoDB:ValidationException")
        key = entry.get("key") or {}
        body = {"TableName": entry.get("table")}
        if shape == "Put":
            body["Item"] = {**(entry.get("attributeValues") or {}), **key}
        else:
            body["Key"] = key
        if shape == "Update":
            _ddb_expression(body, entry.get("update"), "UpdateExpression")
        _ddb_expression(body, entry.get("condition"), "ConditionExpression")
        condition = entry.get("condition") or {}
        if condition.get("returnValuesOnConditionCheckFailure", True) is not False:
            body["ReturnValuesOnConditionCheckFailure"] = "ALL_OLD"
        transact.append({shape: body})
    answer, failure = _ddb_call("TransactWriteItems", {"TransactItems": transact})
    if failure:
        reasons = [
            {
                "type": reason.get("Code") or "None",
                "message": reason.get("Message") or "None",
                **({"item": _ddb_plain(reason["Item"])} if reason.get("Item") else {}),
            }
            for reason in failure[2].get("CancellationReasons") or []
        ]
        raise _ddb_refusal(failure, result={"keys": None, "cancellationReasons": reasons})
    return {"keys": [_ddb_plain(entry.get("key") or {}) for entry in items],
            "cancellationReasons": None}


_APPSYNC_DDB_OPERATIONS = {
    "GetItem": _ddb_op_get_item,
    "PutItem": _ddb_op_put_item,
    "UpdateItem": _ddb_op_update_item,
    "DeleteItem": _ddb_op_delete_item,
    "Query": _ddb_op_read_many("Query"),
    "Scan": _ddb_op_read_many("Scan"),
    "BatchGetItem": _ddb_op_batch_get_item,
    "TransactWriteItems": _ddb_op_transact_write_items,
}


def _ddb_plain(value):
    """Unwrap AttributeValue shapes ({"S": "x"}) into plain JSON.

    A resolver may hand back either form — the @aws-appsync/utils dynamodb
    helpers produce the wrapped one, hand-written request objects usually do
    not — so accept both.
    """
    if isinstance(value, dict):
        if len(value) == 1:
            (tag, inner), = value.items()
            if tag in ("S", "BOOL", "B"):
                return inner
            if tag == "N":
                return _ddb_number(inner)
            if tag == "NULL":
                return None
            if tag == "M":
                return {k: _ddb_plain(v) for k, v in inner.items()}
            if tag == "L":
                return [_ddb_plain(v) for v in inner]
            if tag in ("SS", "BS"):
                return list(inner or [])
            if tag == "NS":
                return [_ddb_number(v) for v in (inner or [])]
        return {k: _ddb_plain(v) for k, v in value.items()}
    return value


def _ddb_number(inner):
    """DynamoDB's N is a string; floats and negatives are numbers too."""
    text = str(inner)
    try:
        return int(text)
    except (TypeError, ValueError):
        try:
            return float(text)
        except (TypeError, ValueError):
            return inner


def _ds_lambda(api_id, data_source, request_obj, ctx):
    """Invoke the Lambda with what the resolver's request() returned.

    A JS resolver returning {operation: "Invoke", payload} sends that payload as
    the event, verbatim — AppSync does not wrap it. Wrapping it in the standard
    resolver event means a function expecting its own shape receives something
    else entirely, and the failure surfaces back in the resolver rather than
    where it was caused.

    Without an explicit payload there is nothing the resolver has shaped, so the
    standard resolver event is the right thing to send, which is what a data
    source with no JS code has always received.
    """
    req = request_obj if isinstance(request_obj, dict) else {}
    if isinstance(req, dict) and "payload" in req:
        return _invoke_lambda_with_event(data_source, req["payload"])
    return _resolve_lambda(
        api_id=api_id,
        resolver={"fieldName": (ctx.get("info") or {}).get("fieldName", "")},
        data_source=data_source,
        args=(ctx.get("args") or {}),
        field_name=(ctx.get("info") or {}).get("fieldName", ""),
        identity=ctx.get("identity"),
        request_headers=(ctx.get("request") or {}).get("headers") or {},
        source=ctx.get("source") or {},
        variables=(ctx.get("info") or {}).get("variables") or {},
    )


def _invoke_lambda_with_event(data_source, event):
    """Invoke a data source's Lambda with an exact event, and unwrap its body."""
    config = data_source.get("lambdaConfig", {})
    func_arn = config.get("lambdaFunctionArn", "")
    if not func_arn:
        raise _AppSyncResolverError(
            "Lambda data source has no lambdaFunctionArn", "InvalidDataSource")

    import ministack.services.lambda_svc as _lambda_svc
    func, func_config, func_name = _lambda_svc._get_func_record_for_ref(func_arn)
    if not func or not func_config:
        raise _AppSyncResolverError(
            f"Lambda function {func_arn} not found", "FunctionNotFound")

    try:
        exec_record = _lambda_svc._execution_record_for_config(func, func_config)
        result = _lambda_svc._execute_function_with_config_scope(exec_record, event)
    except Exception as exc:
        raise _AppSyncResolverError(
            f"Lambda invocation error: {exc}", "LambdaExecutionError") from exc

    if not isinstance(result, dict) or result.get("error"):
        body = result.get("body") if isinstance(result, dict) else None
        msg = body.get("errorMessage") if isinstance(body, dict) else "Lambda execution error"
        raise _AppSyncResolverError(msg or "Lambda execution error", "LambdaExecutionError")

    body = result.get("body")
    if isinstance(body, (str, bytes)):
        try:
            return json.loads(body)
        except (json.JSONDecodeError, ValueError):
            return body
    return body


# type -> handler. RELATIONAL_DATABASE and AMAZON_OPENSEARCH_SERVICE are
# accepted by the control plane and have no handler yet; a resolver over one
# refuses clearly rather than silently answering a mock.
_DATA_SOURCE_HANDLERS = {
    "NONE": _ds_none,
    "HTTP": _ds_http,
    "AMAZON_DYNAMODB": _ds_dynamodb,
    "AWS_LAMBDA": _ds_lambda,
}


class _AppSyncResolverError(Exception):
    def __init__(self, message, error_type="UnknownError", data=None, error_info=None,
                 result=None):
        super().__init__(message)
        self.error_type = error_type
        self.data = data
        self.error_info = error_info
        # What a data source answers beside its error, which AppSync still
        # hands response() as ctx.result: a cancelled transaction's reasons,
        # or the item a refused condition read.
        self.result = result


_SDL_TYPE_RE = _re.compile(r"\btype\s+(\w+)\s*(?:implements[^{]*)?\{([^}]*)\}", _re.S)
_SDL_FIELD_RE = _re.compile(r"^\s*(\w+)\s*(?:\([^)]*\))?\s*:\s*([\[\]\w!]+)", _re.M)


def _schema_field_types(api_id):
    """Map "Type.field" -> the field's type name, parsed from the API's SDL.

    Nested resolution needs to know what type a field returns so it can look up
    the resolvers attached to that type. Parsed from the stored schema rather
    than inferred, and cached per API — the alternative is a GraphQL parser,
    which is a dependency for something this small.
    """
    schema = _schemas.get(api_id) or {}
    sdl = schema.get("definition") or ""
    if not sdl:
        return {}
    cached = schema.get("_field_types")
    if cached is not None and schema.get("_field_types_for") == len(sdl):
        return cached

    out = {}
    for type_name, body in _SDL_TYPE_RE.findall(sdl):
        for field, ftype in _SDL_FIELD_RE.findall(body):
            out[f"{type_name}.{field}"] = ftype.strip("[]!")
    schema["_field_types"] = out
    schema["_field_types_for"] = len(sdl)
    return out


def _resolve_selection(api_id, type_name, value, sub_fields, variables,
                       identity, request_headers, errors, depth=0):
    """Run the resolvers attached to a resolved value's own type.

    AppSync resolves a field, then resolves the fields of whatever that field
    returned, passing the parent as ctx.source. Without this only the top-level
    Query/Mutation fields ever run.
    """
    if depth > 10 or not sub_fields or not isinstance(value, dict):
        return value
    field_types = _schema_field_types(api_id)
    type_resolvers = (_resolvers.get(api_id) or {}).get(type_name) or {}
    if not type_resolvers:
        return value

    for sub in sub_fields:
        name = sub if isinstance(sub, str) else sub.get("name")
        nested = None if isinstance(sub, str) else sub.get("sub_fields")
        resolver = type_resolvers.get(name)
        if not resolver:
            continue
        try:
            child = _resolve_field(
                api_id, resolver, {}, nested or [], variables,
                field_name=name, identity=identity,
                request_headers=request_headers, source=value,
                appended_errors=errors, type_name=type_name)
        except _AppSyncResolverError as exc:
            errors.append({"message": str(exc), "errorType": exc.error_type,
                           "path": [name]})
            child = None
        child_type = field_types.get(f"{type_name}.{name}")
        if child_type and nested:
            child = _resolve_selection(
                api_id, child_type, child, nested, variables, identity,
                request_headers, errors, depth + 1)
        value[name] = child
    return value


def _cognito_identity(api, request_headers):
    """Build ctx.identity from a Cognito token on the request.

    The token's claims are read, not verified — ministack has no auth by design
    and its own Cognito issues these tokens. What matters for a resolver is that
    sub, username, groups and the claim set are present and consistent with the
    pool that issued them.
    """
    auth = request_headers.get("authorization") or request_headers.get("Authorization") or ""
    token = auth[7:] if auth[:7].lower() == "bearer " else auth
    parts = token.split(".")
    if len(parts) != 3:
        return None
    try:
        padded = parts[1] + "=" * (-len(parts[1]) % 4)
        claims = json.loads(base64.urlsafe_b64decode(padded))
    except Exception:
        return None
    if not isinstance(claims, dict) or not claims.get("sub"):
        return None
    return {
        "sub": claims.get("sub"),
        "username": claims.get("cognito:username") or claims.get("username") or claims.get("sub"),
        "claims": claims,
        "sourceIp": ["127.0.0.1"],
        "defaultAuthStrategy": (api.get("userPoolConfig") or {}).get("defaultAction", "ALLOW"),
        "groups": claims.get("cognito:groups"),
        "issuer": claims.get("iss"),
    }


def _verifying():
    """Whether tokens are verified: the AUTH switch that turns IAM on."""
    from ministack import app as _app
    return bool(getattr(_app, "AUTH", False))


def _bearer(request_headers):
    """The bearer token a request carries, or "" for none or a SigV4 credential."""
    auth = request_headers.get("authorization") or request_headers.get("Authorization") or ""
    if not auth or auth.startswith("AWS4-HMAC-SHA256"):
        return ""
    return auth[7:] if auth[:7].lower() == "bearer " else auth


def _oidc_configs(api):
    """Every OPENID_CONNECT provider's configuration on the API, the default first."""
    configs = []
    if api.get("authenticationType") == "OPENID_CONNECT" and api.get("openIDConnectConfig"):
        configs.append(api["openIDConnectConfig"])
    for extra in api.get("additionalAuthenticationProviders") or []:
        if (isinstance(extra, dict) and extra.get("authenticationType") == "OPENID_CONNECT"
                and extra.get("openIDConnectConfig")):
            configs.append(extra["openIDConnectConfig"])
    return configs


_OIDC_KEYS = {}  # issuer -> (fetched at, {kid: jwk})
_OIDC_KEYS_TTL = 300


def _fetch_json(url):
    """A JSON document over HTTP(S), as the issuer publishes it."""
    import urllib.request
    with urllib.request.urlopen(url, timeout=10) as response:  # noqa: S310 - the issuer's own URL
        return json.loads(response.read().decode("utf-8"))


def _oidc_keys(issuer):
    """The issuer's signing keys by kid: its discovery document's jwks_uri, read
    and kept for five minutes."""
    held = _OIDC_KEYS.get(issuer)
    if held and time.time() - held[0] < _OIDC_KEYS_TTL:
        return held[1]
    discovery = _fetch_json(issuer.rstrip("/") + "/.well-known/openid-configuration")
    jwks = _fetch_json(discovery["jwks_uri"])
    keys = {key.get("kid", ""): key for key in jwks.get("keys", []) if isinstance(key, dict)}
    _OIDC_KEYS[issuer] = (time.time(), keys)
    return keys


class _OidcRefused(Exception):
    pass


def _b64url(part):
    return base64.urlsafe_b64decode(part + "=" * (-len(part) % 4))


def _verified_oidc_claims(token, config, now=None):
    """The token's claims, once verified against one OPENID_CONNECT provider the
    way AppSync verifies them: the issuer's JWKS signature, `iss`, the expiry,
    `iat` and `auth_time` against the configured TTLs (milliseconds), and the
    clientId as a regular expression matched against `aud` or `azp`."""
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import padding, rsa

    now = time.time() if now is None else now
    parts = token.split(".")
    if len(parts) != 3:
        raise _OidcRefused("the token is not a JWT")
    try:
        header = json.loads(_b64url(parts[0]))
        claims = json.loads(_b64url(parts[1]))
        signature = _b64url(parts[2])
    except Exception:
        raise _OidcRefused("the token cannot be read")
    if header.get("alg") != "RS256":
        raise _OidcRefused(f"the token is signed {header.get('alg')}, not RS256")
    issuer = str(config.get("issuer") or "")
    if str(claims.get("iss") or "").rstrip("/") != issuer.rstrip("/"):
        raise _OidcRefused(f"the token's issuer is not {issuer}")
    try:
        jwk = _oidc_keys(issuer).get(header.get("kid", ""))
    except Exception as exc:
        raise _OidcRefused(f"the issuer's keys cannot be read: {exc}")
    if not jwk or jwk.get("kty") != "RSA":
        raise _OidcRefused("the issuer publishes no RSA key by the token's kid")
    public = rsa.RSAPublicNumbers(
        int.from_bytes(_b64url(jwk["e"]), "big"),
        int.from_bytes(_b64url(jwk["n"]), "big"),
    ).public_key()
    try:
        public.verify(signature, f"{parts[0]}.{parts[1]}".encode("ascii"),
                      padding.PKCS1v15(), hashes.SHA256())
    except Exception:
        raise _OidcRefused("the token's signature does not verify")
    if "exp" in claims and float(claims["exp"]) <= now:
        raise _OidcRefused("the token has expired")
    iat_ttl = config.get("iatTTL")
    if iat_ttl and "iat" in claims and now - float(claims["iat"]) > float(iat_ttl) / 1000:
        raise _OidcRefused("the token was issued longer ago than iatTTL")
    auth_ttl = config.get("authTTL")
    if auth_ttl and "auth_time" in claims and now - float(claims["auth_time"]) > float(auth_ttl) / 1000:
        raise _OidcRefused("the token's authentication is older than authTTL")
    client_id = config.get("clientId")
    if client_id:
        audiences = claims.get("aud")
        named = (audiences if isinstance(audiences, list) else [audiences]) + [claims.get("azp")]
        if not any(isinstance(value, str) and re.fullmatch(client_id, value) for value in named):
            raise _OidcRefused(f"the token names no audience {client_id} matches")
    return claims


def _oidc_identity(api, request_headers):
    """ctx.identity for a bearer token one of the API's OPENID_CONNECT providers
    verifies — the token's claims, its issuer and its subject — or None.

    Verified under the AUTH switch, as the IAM evaluation is; without it the
    claims are read unverified, as a Cognito token's are.
    """
    token = _bearer(request_headers)
    if not token:
        return None
    configs = _oidc_configs(api)
    if not configs:
        return None
    if _verifying():
        for config in configs:
            try:
                claims = _verified_oidc_claims(token, config)
            except _OidcRefused as refused:
                logger.debug("AppSync OIDC: %s (%s)", refused, config.get("issuer"))
                continue
            break
        else:
            return None
    else:
        parts = token.split(".")
        try:
            claims = json.loads(_b64url(parts[1])) if len(parts) == 3 else None
        except Exception:
            claims = None
        if not isinstance(claims, dict):
            return None
    return {
        "claims": claims,
        "issuer": claims.get("iss"),
        "sub": claims.get("sub"),
        "sourceIp": ["127.0.0.1"],
    }


def _api_env(api_id):
    """The API's environment variables, which resolvers read as ctx.env."""
    return (_apis.get(api_id) or {}).get("environmentVariables") or {}


def _js_ctx(args, identity, source, request_headers, variables, field_name,
            type_name, stash, prev, result=None, api_env=None, error=None):
    """The ctx an APPSYNC_JS resolver sees."""
    ctx = {
        "arguments": args or {},
        "args": args or {},
        "identity": identity,
        "source": source or {},
        "stash": stash if stash is not None else {},
        "prev": {"result": prev},
        "request": {"headers": request_headers or {}},
        "info": {
            "fieldName": field_name,
            "parentTypeName": type_name,
            "variables": variables or {},
            "selectionSetList": [],
        },
    }
    if result is not None:
        ctx["result"] = result
    if api_env:
        ctx["env"] = api_env
    if error is not None:
        # AppSync sets ctx.error when the data source failed, and still runs
        # response() so the resolver can turn it into its own error or a value.
        ctx["error"] = error
    return ctx


def _js_evaluate(code, fn, ctx, errors):
    """Evaluate one handler, translating a resolver error into the executor's."""
    from ministack.core import appsync_js
    try:
        status, value, appended, stash, skip_to = appsync_js.evaluate(code, fn, ctx)
    except appsync_js.AppSyncJsError as exc:
        raise _AppSyncResolverError(str(exc), exc.error_type, exc.data,
                                    exc.error_info) from exc
    errors.extend(appended)
    # Carry the mutated stash back into the ctx the caller holds, so the next
    # stage of a pipeline sees what this one stashed.
    if isinstance(ctx.get("stash"), dict) and isinstance(stash, dict):
        ctx["stash"].clear()
        ctx["stash"].update(stash)
    return status, value, skip_to


def _run_js_stage(api_id, unit, args, identity, source, request_headers,
                  variables, field_name, type_name, stash, prev, errors):
    """Run one APPSYNC_JS unit — a resolver or a pipeline function.

    Returns (status, value): "ok" with the response value, or "earlyReturn"
    with the value the caller must return immediately.
    """
    code = unit.get("code") or ""
    ds_name = unit.get("dataSourceName", "")
    data_source = _data_sources.get(api_id, {}).get(ds_name)

    ctx = _js_ctx(args, identity, source, request_headers, variables,
                  field_name, type_name, stash, prev, api_env=_api_env(api_id))
    status, request_obj, skip_to = _js_evaluate(code, "request", ctx, errors)
    if status == "earlyReturn":
        # AWS: the data source and this handler's response are skipped, and the
        # value becomes the result the next stage sees. skipTo "END" ends the
        # pipeline instead; anything else continues.
        return ("earlyReturnEnd" if skip_to == "END" else "earlyReturn"), request_obj
    if status == "missing":
        raise _AppSyncResolverError(
            f"{type_name}.{field_name}: APPSYNC_JS code exports no request()",
            "InvalidResolver")

    ds_error = None
    if data_source is None:
        result = None
    else:
        ds_type = data_source.get("type", "NONE")
        handler = _DATA_SOURCE_HANDLERS.get(ds_type)
        if handler is None:
            raise _AppSyncResolverError(
                f"Data source type {ds_type} is not executable yet", "NotImplemented")
        try:
            result = handler(api_id, data_source, request_obj, ctx)
        except _AppSyncResolverError as exc:
            # AppSync hands a data source failure to response() as ctx.error
            # rather than skipping it, so the resolver can map it to its own.
            result = exc.result
            ds_error = {"message": str(exc), "type": exc.error_type}

    ctx = _js_ctx(args, identity, source, request_headers, variables,
                  field_name, type_name, stash, prev, result=result,
                  api_env=_api_env(api_id), error=ds_error)
    status, value, skip_to = _js_evaluate(code, "response", ctx, errors)
    if status == "earlyReturn":
        return ("earlyReturnEnd" if skip_to == "END" else "earlyReturn"), value
    if status == "missing":
        # AppSync requires response() on a JS resolver, but returning the raw
        # result is more useful than failing the field outright.
        return "ok", result
    return "ok", value


def _resolve_appsync_js(api_id, resolver, args, variables, field_name,
                        type_name, identity, request_headers, source, errors):
    """Execute an APPSYNC_JS resolver — unit, or pipeline with its functions."""
    stash = {}
    if resolver.get("kind") == "PIPELINE":
        # The resolver's own request() is the "before" step; it may stash values
        # or end the whole pipeline.
        ctx = _js_ctx(args, identity, source, request_headers, variables,
                      field_name, type_name, stash, None)
        status, _before, _skip = _js_evaluate(
            resolver.get("code") or "", "request", ctx, errors)
        stash = ctx["stash"]
        prev = None
        if status == "earlyReturn":
            # AWS: the pipeline is skipped and the resolver's response handler
            # runs immediately — it is not bypassed, so a resolver that shapes
            # its answer there still gets to.
            prev = _before
        else:
            for fn_id in (resolver.get("pipelineConfig") or {}).get("functions") or []:
                fn = (_functions.get(api_id) or {}).get(fn_id)
                if not fn:
                    raise _AppSyncResolverError(
                        f"Pipeline function {fn_id} not found", "InvalidResolver")
                # AWS: every function of a pipeline resolves the resolver's field,
                # and ctx.info.fieldName names that field, not the function.
                status, value = _run_js_stage(
                    api_id, fn, args, identity, source, request_headers, variables,
                    field_name, type_name, stash, prev, errors)
                prev = value
                if status == "earlyReturnEnd":
                    break

        # The resolver's response handler sees both: ctx.prev.result is the last
        # function's result, and ctx.result is the resolver's own result, which
        # for a pipeline is the same value. AWS provides both, and resolvers use
        # either — a template returning ctx.result got null without this.
        ctx = _js_ctx(args, identity, source, request_headers, variables,
                      field_name, type_name, stash, prev, result=prev)
        status, value, _ = _js_evaluate(
            resolver.get("code") or "", "response", ctx, errors)
        return prev if status == "missing" else value

    status, value = _run_js_stage(
        api_id, resolver, args, identity, source, request_headers, variables,
        field_name, type_name, stash, None, errors)
    return value


def _resolve_field(api_id, resolver, args, sub_fields, variables,
                   field_name=None, identity=None, request_headers=None, source=None,
                   appended_errors=None, type_name=None):
    """Execute a resolver against its data source.

    An APPSYNC_JS resolver runs its own code; anything else keeps the previous
    behaviour of dispatching on the data source type and inferring an operation
    from the field arguments.
    """
    runtime_name = (resolver.get("runtime") or {}).get("name")
    if runtime_name == "APPSYNC_JS" and resolver.get("code"):
        return _resolve_appsync_js(
            api_id, resolver, args or {}, variables or {},
            field_name or resolver.get("fieldName", ""),
            type_name or resolver.get("typeName", "Query"),
            identity, request_headers or {}, source or {},
            appended_errors if appended_errors is not None else [])

    ds_name = resolver.get("dataSourceName", "")
    data_source = _data_sources.get(api_id, {}).get(ds_name)

    if not data_source:
        # No data source — return args as mock
        return args or {}

    ds_type = data_source.get("type", "NONE")

    if ds_type == "AMAZON_DYNAMODB":
        return _resolve_dynamodb(data_source, resolver, args, sub_fields)
    elif ds_type == "AWS_LAMBDA":
        return _resolve_lambda(
            api_id=api_id,
            resolver=resolver,
            data_source=data_source,
            args=args,
            field_name=field_name or resolver.get("fieldName", ""),
            identity=identity,
            request_headers=request_headers or {},
            source=source or {},
            variables=variables or {},
        )
    else:
        return args or {}


def _resolve_dynamodb(data_source, resolver, args, sub_fields):
    """Execute a DynamoDB resolver in the data source's configured region."""
    config = data_source.get("dynamodbConfig", {})
    request_region = get_region()
    data_source_region = config.get("awsRegion") or request_region
    set_request_region(data_source_region)
    try:
        return _resolve_dynamodb_in_region(data_source, resolver, args, sub_fields)
    finally:
        set_request_region(request_region)


def _resolve_dynamodb_in_region(data_source, resolver, args, sub_fields):
    """Execute a DynamoDB resolver — auto-detect operation from field name and args."""
    import ministack.services.dynamodb as _ddb

    config = data_source.get("dynamodbConfig", {})
    table_name = config.get("tableName", "")
    if not table_name:
        return None

    table = _ddb._tables.get(table_name)
    if not table:
        return None

    field_name = resolver.get("fieldName", "")

    # Auto-detect: get* → GetItem, list* → Scan, create*/update*/put* → PutItem, delete* ��� DeleteItem
    if field_name.startswith("get") or "id" in args:
        return _ddb_get_item(table, table_name, args, sub_fields)
    elif field_name.startswith("list"):
        return _ddb_scan(table, table_name, args, sub_fields)
    elif field_name.startswith("create") or field_name.startswith("put"):
        return _ddb_put_item(table, table_name, args)
    elif field_name.startswith("update"):
        return _ddb_update_item(table, table_name, args)
    elif field_name.startswith("delete"):
        return _ddb_delete_item(table, table_name, args)
    else:
        # Default: try scan
        return _ddb_scan(table, table_name, args, sub_fields)


def _ddb_get_item(table, table_name, args, sub_fields):
    """Get a single item by primary key."""
    pk_name = table["pk_name"]
    sk_name = table.get("sk_name")

    pk_val = args.get("id") or args.get(pk_name) or next(iter(args.values()), None)
    if pk_val is None:
        return None

    items = table["items"]
    pk_bucket = items.get(str(pk_val), {})

    if sk_name:
        sk_val = args.get(sk_name, "")
        item = pk_bucket.get(str(sk_val))
    else:
        # No sort key — get the single item
        item = next(iter(pk_bucket.values()), None) if pk_bucket else None

    if not item:
        return None

    return _strip_ddb_types(item, sub_fields)


def _ddb_scan(table, table_name, args, sub_fields):
    """Scan/list items, optionally with filters and pagination."""
    items = []
    limit = args.get("limit", 100)
    next_token = args.get("nextToken")

    count = 0
    for pk in sorted(table["items"].keys()):
        for sk in sorted(table["items"][pk].keys()):
            if count >= limit:
                break
            items.append(_strip_ddb_types(table["items"][pk][sk], sub_fields))
            count += 1

    # Filter if filter arg provided
    filter_arg = args.get("filter", {})
    if filter_arg and isinstance(filter_arg, dict):
        filtered = []
        for item in items:
            match = True
            for fk, fv in filter_arg.items():
                if isinstance(fv, dict) and "eq" in fv:
                    if item.get(fk) != fv["eq"]:
                        match = False
                elif item.get(fk) != fv:
                    match = False
            if match:
                filtered.append(item)
        items = filtered

    return {"items": items}


def _ddb_put_item(table, table_name, args):
    """Create/put an item."""
    from collections import defaultdict

    import ministack.services.dynamodb as _ddb

    input_data = args.get("input", args)
    pk_name = table["pk_name"]
    sk_name = table.get("sk_name")

    # Build DynamoDB-typed item
    ddb_item = {}
    for k, v in input_data.items():
        if isinstance(v, str):
            ddb_item[k] = {"S": v}
        elif isinstance(v, (int, float)):
            ddb_item[k] = {"N": str(v)}
        elif isinstance(v, bool):
            ddb_item[k] = {"BOOL": v}
        elif isinstance(v, list):
            ddb_item[k] = {"L": [{"S": str(i)} for i in v]}
        elif v is None:
            ddb_item[k] = {"NULL": True}
        else:
            ddb_item[k] = {"S": str(v)}

    # Auto-generate ID if not provided
    if pk_name not in ddb_item and "id" not in ddb_item:
        ddb_item["id" if pk_name == "id" else pk_name] = {"S": new_uuid()}

    pk_val = _ddb._extract_key_val(ddb_item.get(pk_name, {}))
    sk_val = _ddb._extract_key_val(ddb_item.get(sk_name, {})) if sk_name else ""

    if not isinstance(table["items"], defaultdict):
        table["items"] = defaultdict(dict, table["items"])

    table["items"][pk_val][sk_val] = ddb_item
    table["ItemCount"] = sum(len(v) for v in table["items"].values())

    return _strip_ddb_types(ddb_item, [])


def _ddb_update_item(table, table_name, args):
    """Update an existing item — merge input fields."""
    input_data = args.get("input", args)
    pk_name = table["pk_name"]
    pk_val = str(input_data.get("id") or input_data.get(pk_name, ""))

    if pk_val in table["items"]:
        sk = next(iter(table["items"][pk_val]), "")
        existing = table["items"][pk_val].get(sk, {})
        for k, v in input_data.items():
            if isinstance(v, str):
                existing[k] = {"S": v}
            elif isinstance(v, (int, float)):
                existing[k] = {"N": str(v)}
            elif isinstance(v, bool):
                existing[k] = {"BOOL": v}
        return _strip_ddb_types(existing, [])
    return None


def _ddb_delete_item(table, table_name, args):
    """Delete an item and return it."""
    input_data = args.get("input", args)
    pk_name = table["pk_name"]
    pk_val = str(input_data.get("id") or input_data.get(pk_name, ""))

    if pk_val in table["items"]:
        sk = next(iter(table["items"][pk_val]), "")
        item = table["items"][pk_val].pop(sk, None)
        if not table["items"][pk_val]:
            table["items"].pop(pk_val, None)
        if item:
            return _strip_ddb_types(item, [])
    return None


def _strip_ddb_types(item, sub_fields):
    """Convert DynamoDB typed attributes to plain values for GraphQL response."""
    if not item:
        return None
    result = {}
    for k, v in item.items():
        if isinstance(v, dict):
            if "S" in v:
                result[k] = v["S"]
            elif "N" in v:
                val = v["N"]
                result[k] = int(val) if "." not in val else float(val)
            elif "BOOL" in v:
                result[k] = v["BOOL"]
            elif "NULL" in v:
                result[k] = None
            elif "L" in v:
                result[k] = [_strip_ddb_types(i, []) if isinstance(i, dict) and not any(t in i for t in ("S", "N", "BOOL")) else (i.get("S") or i.get("N") or i.get("BOOL")) for i in v["L"]]
            elif "M" in v:
                result[k] = _strip_ddb_types(v["M"], [])
            else:
                result[k] = v
        else:
            result[k] = v
    if sub_fields:
        result = {k: v for k, v in result.items() if k in sub_fields or k == "id" or k == "__typename"}
    return result


def _resolve_lambda(api_id, resolver, data_source, args,
                    field_name, identity, request_headers, source, variables=None):
    """Execute a Lambda resolver by building the standard AWS AppSync resolver event."""
    config = data_source.get("lambdaConfig", {})
    func_arn = config.get("lambdaFunctionArn", "")
    if not func_arn:
        logger.warning("No lambdaFunctionArn in data source")
        return args or {}

    import ministack.services.lambda_svc as _lambda_svc
    func, func_config, func_name = _lambda_svc._get_func_record_for_ref(func_arn)
    if not func or not func_config:
        logger.warning("Lambda function %s not found", func_arn)
        return args or {}

    # AWS-standard AppSync resolver event — fieldName lives only under info.
    event = {
        "arguments": args,
        "source": source,
        "request": {"headers": request_headers},
        "prev": None,
        "stash": {},
        "info": {
            "fieldName": field_name,
            "parentTypeName": resolver.get("typeName", "Query"),
            "variables": variables or {},
        },
    }
    if identity is not None:
        # Generic pass-through of the authorizer's resolverContext. Omitted for
        # API_KEY auth; a consumer detects API-key auth via request.headers.
        event["identity"] = identity

    try:
        exec_record = _lambda_svc._execution_record_for_config(func, func_config)
        result = _lambda_svc._execute_function_with_config_scope(exec_record, event)
    except Exception as e:
        logger.error("Lambda %s invocation failed: %s", func_name, e)
        return {"errors": [f"Lambda invocation error: {str(e)}"]}

    # RIE catches an unhandled Lambda exception and returns a normal dict with
    # error=True (no Python exception bubbles up), so the try/except above does
    # not cover it. Surface execution errors as GraphQL errors, not as data.
    if not isinstance(result, dict) or result.get("error"):
        err_body = result.get("body") if isinstance(result, dict) else None
        msg = err_body.get("errorMessage") if isinstance(err_body, dict) else "Lambda execution error"
        logger.error("Lambda %s returned error: %s", func_name, msg)
        return {"errors": [msg or "Lambda execution error"]}

    body = result.get("body")
    if body is None:
        return None
    if isinstance(body, dict):
        return body
    if isinstance(body, (str, bytes)):
        try:
            return json.loads(body)
        except (json.JSONDecodeError, ValueError):
            logger.error("Invalid JSON from Lambda %s", func_name)
            return {"errors": ["Invalid response format"]}
    return {"errors": ["Unexpected response type"]}
