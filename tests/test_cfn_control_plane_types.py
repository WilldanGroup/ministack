"""CloudFormation provisioners for the resource types a control-plane deploy
declares around its buses, DNS, federation, build capacity and API domain:
AWS::Events::Archive, AWS::Events::EventBusPolicy, AWS::Route53::RecordSetGroup,
AWS::IAM::OIDCProvider, AWS::CodeBuild::Fleet, AWS::AppSync::DomainName and
AWS::AppSync::DomainNameApiAssociation.

Each type is taken through create, read-back on its own service API, an
in-place update, a replacement of its create-only property, and delete.
"""

import json
import time
import uuid as _uuid_mod

import pytest
from botocore.exceptions import ClientError

ACCOUNT = "000000000000"
REGION = "us-east-1"
CLOUDFRONT_ZONE = "Z2FDTNDATAQYW2"


def _wait_stack(cfn, name, timeout=30):
    """Poll until the stack reaches a terminal status; a stack deleted by name
    answers "does not exist", which is its terminal state."""
    deadline = time.time() + timeout
    status = "UNKNOWN"
    while time.time() < deadline:
        try:
            stacks = cfn.describe_stacks(StackName=name)["Stacks"]
        except ClientError as exc:
            if "does not exist" in str(exc):
                return {"StackStatus": "DELETE_COMPLETE", "StackName": name}
            raise
        status = stacks[0]["StackStatus"]
        if not status.endswith("_IN_PROGRESS"):
            return stacks[0]
        time.sleep(0.5)
    raise TimeoutError(f"Stack {name} stuck at {status}")


def _outputs(stack):
    return {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}


def _reasons(cfn, stack_name):
    return " | ".join(
        e.get("ResourceStatusReason", "")
        for e in cfn.describe_stack_events(StackName=stack_name)["StackEvents"]
        if e.get("ResourceStatusReason")
    )


def _create(cfn, stack_name, template):
    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "CREATE_COMPLETE", _reasons(cfn, stack_name)
    return stack


def _update(cfn, stack_name, template):
    cfn.update_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    stack = _wait_stack(cfn, stack_name)
    assert stack["StackStatus"] == "UPDATE_COMPLETE", _reasons(cfn, stack_name)
    return stack


def _delete(cfn, stack_name):
    cfn.delete_stack(StackName=stack_name)
    assert _wait_stack(cfn, stack_name)["StackStatus"] == "DELETE_COMPLETE"


def _error_code(call):
    with pytest.raises(ClientError) as exc:
        call()
    return exc.value.response["Error"]["Code"]


# ---------------------------------------------------------------------------
# AWS::Events::Archive
# ---------------------------------------------------------------------------

def test_cfn_events_archive(cfn, eb):
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-archive-{uid}"
    bus_name = f"cfn-archive-bus-{uid}"
    name = f"cfn-archive-{uid}"

    def template(retention, description, pattern=None, archive_name=name):
        props = {
            "ArchiveName": archive_name,
            "SourceArn": {"Fn::GetAtt": ["Bus", "Arn"]},
            "RetentionDays": retention,
            "Description": description,
        }
        if pattern is not None:
            props["EventPattern"] = pattern
        return {
            "Resources": {
                "Bus": {"Type": "AWS::Events::EventBus", "Properties": {"Name": bus_name}},
                "Archive": {"Type": "AWS::Events::Archive", "Properties": props},
            },
            "Outputs": {
                "Name": {"Value": {"Ref": "Archive"}},
                "Arn": {"Value": {"Fn::GetAtt": ["Archive", "Arn"]}},
            },
        }

    outputs = _outputs(_create(cfn, stack_name, template(7, "first")))
    arn = f"arn:aws:events:{REGION}:{ACCOUNT}:archive/{name}"
    assert outputs == {"Name": name, "Arn": arn}
    archive = eb.describe_archive(ArchiveName=name)
    assert archive["ArchiveArn"] == arn
    assert archive["EventSourceArn"] == f"arn:aws:events:{REGION}:{ACCOUNT}:event-bus/{bus_name}"
    assert archive["RetentionDays"] == 7
    assert archive["Description"] == "first"
    assert archive["State"] == "ENABLED"

    # RetentionDays, Description and EventPattern update in place: same ARN.
    pattern = {"source": ["switchboard"]}
    _update(cfn, stack_name, template(30, "second", pattern))
    archive = eb.describe_archive(ArchiveName=name)
    assert archive["ArchiveArn"] == arn
    assert archive["RetentionDays"] == 30
    assert archive["Description"] == "second"
    assert json.loads(archive["EventPattern"]) == pattern

    # ArchiveName is create-only: the rename creates the new archive and
    # removes the old one.
    renamed = f"{name}-b"
    outputs = _outputs(_update(cfn, stack_name, template(30, "second", pattern, renamed)))
    assert outputs["Name"] == renamed
    assert outputs["Arn"] == f"arn:aws:events:{REGION}:{ACCOUNT}:archive/{renamed}"
    assert eb.describe_archive(ArchiveName=renamed)["RetentionDays"] == 30
    assert _error_code(lambda: eb.describe_archive(ArchiveName=name)) == "ResourceNotFoundException"

    _delete(cfn, stack_name)
    assert _error_code(lambda: eb.describe_archive(ArchiveName=renamed)) == "ResourceNotFoundException"


# ---------------------------------------------------------------------------
# AWS::Events::EventBusPolicy
# ---------------------------------------------------------------------------

def _bus_policy_statements(eb, bus_name):
    bus = eb.describe_event_bus(Name=bus_name)
    if "Policy" not in bus:
        return {}
    return {s["Sid"]: s for s in json.loads(bus["Policy"])["Statement"]}


def test_cfn_events_event_bus_policy(cfn, eb):
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-buspolicy-{uid}"
    bus_name = f"cfn-buspolicy-bus-{uid}"
    bus_arn = f"arn:aws:events:{REGION}:{ACCOUNT}:event-bus/{bus_name}"
    grant_sid = f"tenants-{uid}"
    legacy_sid = f"legacy-{uid}"
    # The bus and a statement from outside the stack, which every stack
    # operation has to leave alone.
    eb.create_event_bus(Name=bus_name)
    eb.put_permission(EventBusName=bus_name, StatementId="outside",
                      Action="events:PutEvents", Principal="*")

    def template(org_id, grant_sid=grant_sid):
        return {
            "Resources": {
                "Grant": {
                    "Type": "AWS::Events::EventBusPolicy",
                    "Properties": {
                        "EventBusName": bus_name,
                        "StatementId": grant_sid,
                        "Statement": {
                            "Effect": "Allow",
                            "Principal": {"AWS": "arn:aws:iam::111122223333:root"},
                            "Action": "events:PutEvents",
                            "Resource": bus_arn,
                            "Condition": {"StringEquals": {"aws:PrincipalOrgID": org_id}},
                        },
                    },
                },
                "Legacy": {
                    "Type": "AWS::Events::EventBusPolicy",
                    "Properties": {
                        "EventBusName": bus_name,
                        "StatementId": legacy_sid,
                        "Action": "events:PutEvents",
                        "Principal": "111122223333",
                    },
                },
            },
            "Outputs": {"GrantId": {"Value": {"Ref": "Grant"}}},
        }

    try:
        outputs = _outputs(_create(cfn, stack_name, template("o-first")))
        assert outputs["GrantId"] == grant_sid
        statements = _bus_policy_statements(eb, bus_name)
        assert set(statements) == {grant_sid, legacy_sid, "outside"}
        grant = statements[grant_sid]
        assert grant["Effect"] == "Allow"
        assert grant["Principal"] == {"AWS": "arn:aws:iam::111122223333:root"}
        assert grant["Action"] == "events:PutEvents"
        assert grant["Resource"] == bus_arn
        assert grant["Condition"] == {"StringEquals": {"aws:PrincipalOrgID": "o-first"}}
        assert statements[legacy_sid]["Principal"] == "111122223333"
        assert statements[legacy_sid]["Resource"] == bus_arn

        # The statement updates in place under its Sid; the others stay.
        _update(cfn, stack_name, template("o-second"))
        statements = _bus_policy_statements(eb, bus_name)
        assert set(statements) == {grant_sid, legacy_sid, "outside"}
        assert statements[grant_sid]["Condition"] == {"StringEquals": {"aws:PrincipalOrgID": "o-second"}}

        # StatementId is create-only: the new Sid appears, the old one goes.
        renamed = f"{grant_sid}-b"
        outputs = _outputs(_update(cfn, stack_name, template("o-second", renamed)))
        assert outputs["GrantId"] == renamed
        statements = _bus_policy_statements(eb, bus_name)
        assert set(statements) == {renamed, legacy_sid, "outside"}

        _delete(cfn, stack_name)
        assert set(_bus_policy_statements(eb, bus_name)) == {"outside"}
    finally:
        eb.remove_permission(EventBusName=bus_name, RemoveAllPermissions=True)
        eb.delete_event_bus(Name=bus_name)


# ---------------------------------------------------------------------------
# AWS::Route53::RecordSetGroup
# ---------------------------------------------------------------------------

def _zone_records(r53, zone_id):
    rrs = r53.list_resource_record_sets(HostedZoneId=zone_id)["ResourceRecordSets"]
    return {(r["Name"], r["Type"]): r for r in rrs}


def test_cfn_route53_record_set_group(cfn, r53):
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-rsg-{uid}"
    zone_name = f"cfnrsg{uid}.example.com."
    zone_id = r53.create_hosted_zone(Name=zone_name, CallerReference=uid)["HostedZone"]["Id"]
    app = f"app.{zone_name}"
    txt = f"_verify.{zone_name}"
    www = f"www.{zone_name}"

    def alias(name, rtype):
        return {"Name": name, "Type": rtype, "AliasTarget": {
            "DNSName": "d111111abcdef8.cloudfront.net", "HostedZoneId": CLOUDFRONT_ZONE}}

    def template(record_sets):
        return {
            "Resources": {"Records": {
                "Type": "AWS::Route53::RecordSetGroup",
                "Properties": {"HostedZoneId": zone_id, "Comment": "the site's names",
                               "RecordSets": record_sets},
            }},
            "Outputs": {"Group": {"Value": {"Ref": "Records"}}},
        }

    try:
        outputs = _outputs(_create(cfn, stack_name, template([
            alias(app, "A"), alias(app, "AAAA"),
            {"Name": txt, "Type": "TXT", "TTL": 60, "ResourceRecords": [{"Value": '"one"'}]},
        ])))
        assert outputs["Group"]
        records = _zone_records(r53, zone_id)
        assert records[(app, "A")]["AliasTarget"] == {
            "HostedZoneId": CLOUDFRONT_ZONE, "DNSName": "d111111abcdef8.cloudfront.net.",
            "EvaluateTargetHealth": False}
        assert (app, "AAAA") in records
        assert records[(txt, "TXT")]["TTL"] == 60
        assert records[(txt, "TXT")]["ResourceRecords"] == [{"Value": '"one"'}]

        # Reconciled against the new list: AAAA dropped, TXT changed, CNAME added.
        _update(cfn, stack_name, template([
            alias(app, "A"),
            {"Name": txt, "Type": "TXT", "TTL": 300, "ResourceRecords": [{"Value": '"two"'}]},
            {"Name": www, "Type": "CNAME", "TTL": 60, "ResourceRecords": [{"Value": app}]},
        ]))
        records = _zone_records(r53, zone_id)
        assert (app, "A") in records
        assert (app, "AAAA") not in records
        assert records[(txt, "TXT")]["TTL"] == 300
        assert records[(txt, "TXT")]["ResourceRecords"] == [{"Value": '"two"'}]
        assert records[(www, "CNAME")]["ResourceRecords"] == [{"Value": app}]

        _delete(cfn, stack_name)
        assert {t for _n, t in _zone_records(r53, zone_id)} <= {"NS", "SOA"}
    finally:
        r53.delete_hosted_zone(Id=zone_id)


# ---------------------------------------------------------------------------
# AWS::IAM::OIDCProvider
# ---------------------------------------------------------------------------

def test_cfn_iam_oidc_provider(cfn, iam):
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-oidc-{uid}"
    host = f"issuer-{uid}.example.com"
    url = f"https://{host}/oidc"
    arn = f"arn:aws:iam::{ACCOUNT}:oidc-provider/{host}/oidc"

    def template(client_ids, thumbprint, team, url=url):
        return {
            "Resources": {"Provider": {
                "Type": "AWS::IAM::OIDCProvider",
                "Properties": {
                    "Url": url,
                    "ClientIdList": client_ids,
                    "ThumbprintList": [thumbprint],
                    "Tags": [{"Key": "team", "Value": team}],
                },
            }},
            "Outputs": {
                "Ref": {"Value": {"Ref": "Provider"}},
                "Arn": {"Value": {"Fn::GetAtt": ["Provider", "Arn"]}},
            },
        }

    outputs = _outputs(_create(cfn, stack_name, template(["sts.amazonaws.com"], "0" * 40, "a")))
    assert outputs == {"Ref": arn, "Arn": arn}
    provider = iam.get_open_id_connect_provider(OpenIDConnectProviderArn=arn)
    assert provider["Url"] == url
    assert provider["ClientIDList"] == ["sts.amazonaws.com"]
    assert provider["ThumbprintList"] == ["0" * 40]
    tags = {t["Key"]: t["Value"] for t in provider["Tags"]}
    assert tags["team"] == "a"
    assert tags["aws:cloudformation:stack-name"] == stack_name

    # Client ids, thumbprints and tags update in place under the same ARN.
    _update(cfn, stack_name, template(["sts.amazonaws.com", "api"], "1" * 40, "b"))
    provider = iam.get_open_id_connect_provider(OpenIDConnectProviderArn=arn)
    assert provider["ClientIDList"] == ["sts.amazonaws.com", "api"]
    assert provider["ThumbprintList"] == ["1" * 40]
    assert {t["Key"]: t["Value"] for t in provider["Tags"]}["team"] == "b"

    # Url is create-only and is the ARN: the change replaces the provider.
    new_url = f"https://{host}/v2"
    new_arn = f"arn:aws:iam::{ACCOUNT}:oidc-provider/{host}/v2"
    outputs = _outputs(_update(cfn, stack_name, template(["api"], "1" * 40, "b", new_url)))
    assert outputs == {"Ref": new_arn, "Arn": new_arn}
    assert iam.get_open_id_connect_provider(OpenIDConnectProviderArn=new_arn)["Url"] == new_url
    assert _error_code(lambda: iam.get_open_id_connect_provider(
        OpenIDConnectProviderArn=arn)) == "NoSuchEntity"
    listed = {p["Arn"] for p in iam.list_open_id_connect_providers()["OpenIDConnectProviderList"]}
    assert new_arn in listed and arn not in listed

    _delete(cfn, stack_name)
    assert _error_code(lambda: iam.get_open_id_connect_provider(
        OpenIDConnectProviderArn=new_arn)) == "NoSuchEntity"


# ---------------------------------------------------------------------------
# AWS::CodeBuild::Fleet
# ---------------------------------------------------------------------------

def test_cfn_codebuild_fleet(cfn, codebuild):
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-fleet-{uid}"
    name = f"cfn-fleet-{uid}"
    scaling = {
        "MaxCapacity": 3,
        "ScalingType": "TARGET_TRACKING_SCALING",
        "TargetTrackingScalingConfigs": [{"MetricType": "FLEET_UTILIZATION_RATE", "TargetValue": 70}],
    }

    def template(base_capacity, scaling=None, name=name):
        fleet = {
            "Name": name,
            "BaseCapacity": base_capacity,
            "ComputeType": "BUILD_GENERAL1_SMALL",
            "EnvironmentType": "LINUX_CONTAINER",
            "OverflowBehavior": "ON_DEMAND",
            "Tags": [{"Key": "team", "Value": "platform"}],
        }
        if scaling is not None:
            fleet["ScalingConfiguration"] = scaling
        return {
            "Resources": {
                "Fleet": {"Type": "AWS::CodeBuild::Fleet", "Properties": fleet},
                "Project": {
                    "Type": "AWS::CodeBuild::Project",
                    "Properties": {
                        "Name": f"cfn-fleet-project-{uid}",
                        "Source": {"Type": "NO_SOURCE"},
                        "Artifacts": {"Type": "NO_ARTIFACTS"},
                        "Environment": {
                            "Type": "LINUX_CONTAINER",
                            "Image": "aws/codebuild/standard:7.0",
                            "ComputeType": "BUILD_GENERAL1_SMALL",
                            "Fleet": {"FleetArn": {"Fn::GetAtt": ["Fleet", "Arn"]}},
                        },
                        "ServiceRole": f"arn:aws:iam::{ACCOUNT}:role/codebuild-role",
                    },
                },
            },
            "Outputs": {
                "Ref": {"Value": {"Ref": "Fleet"}},
                "Arn": {"Value": {"Fn::GetAtt": ["Fleet", "Arn"]}},
                "Id": {"Value": {"Fn::GetAtt": ["Fleet", "Id"]}},
            },
        }

    outputs = _outputs(_create(cfn, stack_name, template(1, scaling)))
    arn = outputs["Arn"]
    assert outputs["Ref"] == arn
    assert arn.startswith(f"arn:aws:codebuild:{REGION}:{ACCOUNT}:fleet/{name}:")
    assert arn.endswith(":" + outputs["Id"])
    got = codebuild.batch_get_fleets(names=[arn])
    assert got["fleetsNotFound"] == []
    fleet = got["fleets"][0]
    assert fleet["name"] == name
    assert fleet["id"] == outputs["Id"]
    assert fleet["baseCapacity"] == 1
    assert fleet["computeType"] == "BUILD_GENERAL1_SMALL"
    assert fleet["environmentType"] == "LINUX_CONTAINER"
    assert fleet["overflowBehavior"] == "ON_DEMAND"
    assert fleet["status"]["statusCode"] == "ACTIVE"
    assert fleet["scalingConfiguration"]["maxCapacity"] == 3
    assert fleet["scalingConfiguration"]["targetTrackingScalingConfigs"] == [
        {"metricType": "FLEET_UTILIZATION_RATE", "targetValue": 70.0}]
    assert {t["key"]: t["value"] for t in fleet["tags"]}["team"] == "platform"
    # By name as well as by ARN, and listed.
    assert codebuild.batch_get_fleets(names=[name])["fleets"][0]["arn"] == arn
    assert arn in codebuild.list_fleets()["fleets"]

    # Capacity updates in place; the dropped scaling configuration goes.
    outputs = _outputs(_update(cfn, stack_name, template(2)))
    assert outputs["Arn"] == arn
    fleet = codebuild.batch_get_fleets(names=[arn])["fleets"][0]
    assert fleet["baseCapacity"] == 2
    assert "scalingConfiguration" not in fleet

    # Name is create-only: the rename creates a new fleet under a new ARN.
    renamed = f"{name}-b"
    outputs = _outputs(_update(cfn, stack_name, template(2, name=renamed)))
    assert outputs["Arn"] != arn
    assert outputs["Arn"].startswith(f"arn:aws:codebuild:{REGION}:{ACCOUNT}:fleet/{renamed}:")
    got = codebuild.batch_get_fleets(names=[arn, outputs["Arn"]])
    assert got["fleetsNotFound"] == [arn]
    assert got["fleets"][0]["name"] == renamed

    _delete(cfn, stack_name)
    assert codebuild.batch_get_fleets(names=[outputs["Arn"]])["fleetsNotFound"] == [outputs["Arn"]]
    assert outputs["Arn"] not in codebuild.list_fleets()["fleets"]


# ---------------------------------------------------------------------------
# AWS::AppSync::DomainName and AWS::AppSync::DomainNameApiAssociation
# ---------------------------------------------------------------------------

def test_cfn_appsync_domain_name_and_association(cfn, appsync):
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-appsync-domain-{uid}"
    domain = f"api-{uid}.example.com"
    certificate = f"arn:aws:acm:{REGION}:{ACCOUNT}:certificate/{uid}"

    def template(description, domain=domain):
        return {
            "Resources": {
                "Api": {"Type": "AWS::AppSync::GraphQLApi",
                        "Properties": {"Name": f"cfn-domain-api-{uid}", "AuthenticationType": "API_KEY"}},
                "Domain": {
                    "Type": "AWS::AppSync::DomainName",
                    "Properties": {
                        "DomainName": domain,
                        "CertificateArn": certificate,
                        "Description": description,
                        "Tags": [{"Key": "team", "Value": "platform"}],
                    },
                },
                "Association": {
                    "Type": "AWS::AppSync::DomainNameApiAssociation",
                    "Properties": {"ApiId": {"Fn::GetAtt": ["Api", "ApiId"]},
                                   "DomainName": {"Ref": "Domain"}},
                },
            },
            "Outputs": {
                "ApiId": {"Value": {"Fn::GetAtt": ["Api", "ApiId"]}},
                "Domain": {"Value": {"Ref": "Domain"}},
                "AppSyncDomainName": {"Value": {"Fn::GetAtt": ["Domain", "AppSyncDomainName"]}},
                "DomainAttr": {"Value": {"Fn::GetAtt": ["Domain", "DomainName"]}},
                "HostedZoneId": {"Value": {"Fn::GetAtt": ["Domain", "HostedZoneId"]}},
                "Arn": {"Value": {"Fn::GetAtt": ["Domain", "Arn"]}},
                "Association": {"Value": {"Ref": "Association"}},
                "AssociationId": {"Value": {"Fn::GetAtt": ["Association", "ApiAssociationIdentifier"]}},
                "AssociationDomain": {"Value": {"Fn::GetAtt": ["Association", "DomainName"]}},
            },
        }

    outputs = _outputs(_create(cfn, stack_name, template("first")))
    assert outputs["Domain"] == domain
    assert outputs["DomainAttr"] == domain
    assert outputs["AppSyncDomainName"].endswith(".cloudfront.net")
    assert outputs["HostedZoneId"] == CLOUDFRONT_ZONE
    assert outputs["Arn"] == f"arn:aws:appsync:{REGION}:{ACCOUNT}:domainnames/{domain}"
    assert outputs["Association"] == domain
    assert outputs["AssociationId"] == domain
    assert outputs["AssociationDomain"] == domain
    config = appsync.get_domain_name(domainName=domain)["domainNameConfig"]
    assert config["certificateArn"] == certificate
    assert config["description"] == "first"
    assert config["appsyncDomainName"] == outputs["AppSyncDomainName"]
    assert config["hostedZoneId"] == CLOUDFRONT_ZONE
    assert config["domainNameArn"] == outputs["Arn"]
    assert config["tags"]["team"] == "platform"
    association = appsync.get_api_association(domainName=domain)["apiAssociation"]
    assert association["apiId"] == outputs["ApiId"]
    assert association["associationStatus"] == "SUCCESS"
    assert domain in {d["domainName"] for d in appsync.list_domain_names()["domainNameConfigs"]}

    # Description updates in place: the CloudFront name is kept.
    updated = _outputs(_update(cfn, stack_name, template("second")))
    assert updated["AppSyncDomainName"] == outputs["AppSyncDomainName"]
    config = appsync.get_domain_name(domainName=domain)["domainNameConfig"]
    assert config["description"] == "second"
    assert config["appsyncDomainName"] == outputs["AppSyncDomainName"]

    # DomainName is create-only on both: the new domain is created and
    # associated, then the old one is disassociated and removed.
    renamed = f"api2-{uid}.example.com"
    updated = _outputs(_update(cfn, stack_name, template("second", renamed)))
    assert updated["Domain"] == renamed
    assert updated["Association"] == renamed
    assert appsync.get_api_association(domainName=renamed)["apiAssociation"]["apiId"] == outputs["ApiId"]
    assert _error_code(lambda: appsync.get_domain_name(domainName=domain)) == "NotFoundException"

    _delete(cfn, stack_name)
    assert _error_code(lambda: appsync.get_domain_name(domainName=renamed)) == "NotFoundException"
    assert _error_code(lambda: appsync.get_api_association(domainName=renamed)) == "NotFoundException"


# ---------------------------------------------------------------------------
# All six in one stack, the way the deploy declares them
# ---------------------------------------------------------------------------

def test_cfn_control_plane_types_deploy_together(cfn, r53):
    uid = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-control-plane-{uid}"
    zone_name = f"cfncp{uid}.example.com."
    template = {
        "Resources": {
            "Bus": {"Type": "AWS::Events::EventBus", "Properties": {"Name": f"cfn-cp-bus-{uid}"}},
            "Archive": {"Type": "AWS::Events::Archive", "Properties": {
                "ArchiveName": f"cfn-cp-{uid}", "RetentionDays": 90,
                "SourceArn": {"Fn::GetAtt": ["Bus", "Arn"]}}},
            "TenantPutEvents": {"Type": "AWS::Events::EventBusPolicy", "Properties": {
                "EventBusName": {"Ref": "Bus"},
                "StatementId": f"cfn-cp-{uid}-tenant-put-events",
                "Statement": {
                    "Sid": "ConnectedTenantAccountsPutEvents", "Effect": "Allow",
                    "Principal": "*", "Action": "events:PutEvents",
                    "Resource": {"Fn::GetAtt": ["Bus", "Arn"]},
                    "Condition": {"StringEquals": {"aws:PrincipalOrgID": "o-cfncp"}}}}},
            "Provider": {"Type": "AWS::IAM::OIDCProvider", "Properties": {
                "Url": f"https://cfncp-{uid}.example.com",
                "ClientIdList": ["sts.amazonaws.com"],
                "ThumbprintList": ["d09370a9864982a5047da46373540256d08a3c81"],
                "Tags": [{"Key": "bootstrap:ManagedBy", "Value": stack_name}]}},
            "Fleet": {"Type": "AWS::CodeBuild::Fleet", "Properties": {
                "Name": f"cfn-cp-{uid}-platform-build", "BaseCapacity": 1,
                "ComputeType": "BUILD_GENERAL1_MEDIUM", "EnvironmentType": "LINUX_CONTAINER",
                "OverflowBehavior": "ON_DEMAND",
                "Tags": [{"Key": "bootstrap:ManagedBy", "Value": stack_name}]}},
            "BuildProject": {"Type": "AWS::CodeBuild::Project", "Properties": {
                "Name": f"cfn-cp-{uid}-platform-build",
                "Source": {"Type": "NO_SOURCE"}, "Artifacts": {"Type": "NO_ARTIFACTS"},
                "Environment": {
                    "Type": "LINUX_CONTAINER", "Image": "public.ecr.aws/docker/library/alpine:3",
                    "ComputeType": "BUILD_GENERAL1_MEDIUM", "PrivilegedMode": True,
                    "Fleet": {"FleetArn": {"Fn::GetAtt": ["Fleet", "Arn"]}}},
                "ServiceRole": f"arn:aws:iam::{ACCOUNT}:role/cfn-cp-build"}},
            "Api": {"Type": "AWS::AppSync::GraphQLApi", "Properties": {
                "Name": f"cfn-cp-{uid}", "AuthenticationType": "AWS_IAM"}},
            "ApiDomain": {"Type": "AWS::AppSync::DomainName", "Properties": {
                "DomainName": f"api.{zone_name[:-1]}",
                "CertificateArn": f"arn:aws:acm:{REGION}:{ACCOUNT}:certificate/{uid}",
                "Description": "The control plane's own hostname."}},
            "ApiDomainAssociation": {"Type": "AWS::AppSync::DomainNameApiAssociation", "Properties": {
                "ApiId": {"Fn::GetAtt": ["Api", "ApiId"]}, "DomainName": {"Ref": "ApiDomain"}}},
            "Zone": {"Type": "AWS::Route53::HostedZone", "Properties": {"Name": zone_name}},
            "ApiRecords": {"Type": "AWS::Route53::RecordSetGroup", "Properties": {
                "HostedZoneId": {"Ref": "Zone"},
                "RecordSets": [
                    {"Name": f"api.{zone_name}", "Type": rtype, "AliasTarget": {
                        "DNSName": {"Fn::GetAtt": ["ApiDomain", "AppSyncDomainName"]},
                        "HostedZoneId": {"Fn::GetAtt": ["ApiDomain", "HostedZoneId"]}}}
                    for rtype in ("A", "AAAA")
                ]}},
        },
        "Outputs": {"ZoneId": {"Value": {"Ref": "Zone"}},
                    "ApiHost": {"Value": {"Fn::GetAtt": ["ApiDomain", "AppSyncDomainName"]}}},
    }
    stack = _create(cfn, stack_name, template)
    resources = cfn.describe_stack_resources(StackName=stack_name)["StackResources"]
    assert {r["ResourceStatus"] for r in resources} == {"CREATE_COMPLETE"}
    assert {r["ResourceType"] for r in resources} >= {
        "AWS::Events::Archive", "AWS::Events::EventBusPolicy", "AWS::Route53::RecordSetGroup",
        "AWS::IAM::OIDCProvider", "AWS::CodeBuild::Fleet", "AWS::AppSync::DomainName",
        "AWS::AppSync::DomainNameApiAssociation",
    }
    outputs = _outputs(stack)
    records = _zone_records(r53, outputs["ZoneId"])
    assert records[(f"api.{zone_name}", "A")]["AliasTarget"]["DNSName"] == outputs["ApiHost"] + "."
    assert records[(f"api.{zone_name}", "AAAA")]["AliasTarget"]["HostedZoneId"] == CLOUDFRONT_ZONE

    _delete(cfn, stack_name)
