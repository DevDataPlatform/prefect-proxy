"""interface with prefect's python client api"""
import os
import requests
from fastapi import HTTPException

from prefect.deployments import Deployment, run_deployment
from prefect.server.schemas.schedules import CronSchedule
from prefect.blocks.core import Block
from prefect_airbyte import AirbyteConnection, AirbyteServer

from prefect_gcp import GcpCredentials
from prefect_dbt.cli.configs import TargetConfigs
from prefect_dbt.cli.configs import BigQueryTargetConfigs
from prefect_dbt.cli.commands import DbtCoreOperation, ShellOperation
from prefect_dbt.cli import DbtCliProfile
from dotenv import load_dotenv
from logger import logger


from proxy.helpers import cleaned_name_for_prefectblock
from proxy.exception import PrefectException
from proxy.schemas import (
    AirbyteServerCreate,
    AirbyteConnectionCreate,
    PrefectShellSetup,
    DbtCoreCreate,
    DeploymentCreate,
)
from proxy.flows import (
    deployment_schedule_flow,
)

load_dotenv()

FLOW_RUN_FAILED = "FAILED"
FLOW_RUN_COMPLETED = "COMPLETED"
FLOW_RUN_SCHEDULED = "SCHEDULED"


def prefect_post(endpoint: str, payload: dict) -> dict:
    """POST request to prefect server"""
    if not isinstance(endpoint, str):
        raise TypeError("endpoint must be a string")
    if not isinstance(payload, dict):
        raise TypeError("payload must be a dictionary")

    root = os.getenv("PREFECT_API_URL")
    res = requests.post(f"{root}/{endpoint}", timeout=30, json=payload)
    logger.info(res.text)
    try:
        res.raise_for_status()
    except Exception as error:
        logger.exception(error)
        raise HTTPException(status_code=400, detail=res.text) from error
    return res.json()


def prefect_get(endpoint: str) -> dict:
    """GET request to prefect server"""
    if not isinstance(endpoint, str):
        raise TypeError("endpoint must be a string")

    root = os.getenv("PREFECT_API_URL")
    res = requests.get(f"{root}/{endpoint}", timeout=30)
    try:
        res.raise_for_status()
    except Exception as error:
        logger.exception(error)
        raise HTTPException(status_code=400, detail=res.text) from error
    return res.json()


def prefect_delete(endpoint: str) -> dict:
    """DELETE request to prefect server"""
    if not isinstance(endpoint, str):
        raise TypeError("endpoint must be a string")

    root = os.getenv("PREFECT_API_URL")
    res = requests.delete(f"{root}/{endpoint}", timeout=30)
    try:
        res.raise_for_status()
    except Exception as error:
        logger.exception(error)
        raise HTTPException(status_code=400, detail=res.text) from error
    return res.json()


def _block_id(block: Block) -> str:
    """Get the id of block"""
    return str(block.dict()["_block_document_id"])


# ================================================================================================
def post_filter_blocks(block_names) -> dict:
    """Filter and fetch prefect blocks based on the query parameter"""
    try:
        query = {
            "block_documents": {
                "operator": "and_",
                "name": {"any_": []},
            }
        }
        if block_names:
            query["block_documents"]["name"]["any_"] = block_names

        return prefect_post("block_documents/filter", query)
    except Exception as err:
        logger.exception(err)
        raise PrefectException("failed to create deployment") from err


# ================================================================================================
async def get_airbyte_server_block_id(blockname: str) -> str | None:
    """look up an airbyte server block by name and return block_id"""
    if not isinstance(blockname, str):
        raise TypeError("blockname must be a string")
    try:
        block = await AirbyteServer.load(blockname)
        logger.info("found airbyte server block named %s", blockname)
        return _block_id(block)
    except ValueError:
        logger.error("no airbyte server block named %s", blockname)
        return None


async def create_airbyte_server_block(payload: AirbyteServerCreate) -> str:
    """Create airbyte server block in prefect"""
    if not isinstance(payload, AirbyteServerCreate):
        raise TypeError("payload must be an AirbyteServerCreate")

    airbyteservercblock = AirbyteServer(
        server_host=payload.serverHost,
        server_port=payload.serverPort,
        api_version=payload.apiVersion,
    )
    try:
        block_name_for_save = cleaned_name_for_prefectblock(payload.blockName)
        await airbyteservercblock.save(block_name_for_save)
    except Exception as error:
        logger.exception(error)
        raise PrefectException("failed to create airbyte server block") from error
    logger.info("created airbyte server block named %s", payload.blockName)
    return _block_id(airbyteservercblock)


def update_airbyte_server_block(blockname: str):
    """We don't update server blocks"""
    if not isinstance(blockname, str):
        raise TypeError("blockname must be a string")
    raise PrefectException("not implemented")


def delete_airbyte_server_block(blockid: str):
    """Delete airbyte server block"""
    if not isinstance(blockid, str):
        raise TypeError("blockid must be a string")

    logger.info("deleting airbyte server block %s", blockid)
    return prefect_delete(f"block_documents/{blockid}")


# ================================================================================================
async def get_airbyte_connection_block_id(blockname: str) -> str | None:
    """look up airbyte connection block by name and return block_id"""
    if not isinstance(blockname, str):
        raise TypeError("blockname must be a string")
    try:
        block = await AirbyteConnection.load(blockname)
        logger.info("found airbyte connection block named %s", blockname)
        return _block_id(block)
    except ValueError:
        logger.error("no airbyte connection block named %s", blockname)
        # pylint: disable=raise-missing-from
        raise HTTPException(
            status_code=404, detail=f"No airbyte connection block named {blockname}"
        )


async def get_airbyte_connection_block(blockid: str) -> dict:
    """look up and return block data for an airbyte connection"""
    if not isinstance(blockid, str):
        raise TypeError("blockid must be a string")
    try:
        result = prefect_get(f"block_documents/{blockid}")
        logger.info("found airbyte connection block having id %s", blockid)
        return result
    except requests.exceptions.HTTPError:
        logger.error("no airbyte connection block having id %s", blockid)
        # pylint: disable=raise-missing-from
        raise HTTPException(
            status_code=404, detail=f"No airbyte connection block having id {blockid}"
        )


async def create_airbyte_connection_block(
    conninfo: AirbyteConnectionCreate,
) -> str:
    """Create airbyte connection block"""
    if not isinstance(conninfo, AirbyteConnectionCreate):
        raise TypeError("conninfo must be an AirbyteConnectionCreate")

    logger.info(conninfo)
    try:
        serverblock = await AirbyteServer.load(conninfo.serverBlockName)
    except ValueError as exc:
        logger.exception(exc)
        raise PrefectException(
            f"could not find Airbyte Server block named {conninfo.serverBlockName}"
        ) from exc

    connection_block = AirbyteConnection(
        airbyte_server=serverblock,
        connection_id=conninfo.connectionId,
    )
    try:
        block_name_for_save = cleaned_name_for_prefectblock(
            conninfo.connectionBlockName
        )
        await connection_block.save(block_name_for_save)
    except Exception as error:
        logger.exception(error)
        raise PrefectException(
            f"failed to create airbyte connection block for connection {conninfo.connectionId}"
        ) from error
    logger.info("created airbyte connection block %s", conninfo.connectionBlockName)

    return _block_id(connection_block)


def update_airbyte_connection_block(blockname: str):
    """We don't update connection blocks"""
    if not isinstance(blockname, str):
        raise TypeError("blockname must be a string")

    raise PrefectException("not implemented")


def delete_airbyte_connection_block(blockid: str) -> dict:
    """Delete airbyte connection block in prefect"""
    if not isinstance(blockid, str):
        raise TypeError("blockid must be a string")

    logger.info("deleting airbyte connection block %s", blockid)

    return prefect_delete(f"block_documents/{blockid}")


# ================================================================================================
async def get_shell_block_id(blockname: str) -> str | None:
    """look up a shell operation block by name and return block_id"""
    if not isinstance(blockname, str):
        raise TypeError("blockname must be a string")

    try:
        block = await ShellOperation.load(blockname)
        return _block_id(block)
    except ValueError:
        # pylint: disable=raise-missing-from
        raise HTTPException(
            status_code=404, detail=f"No shell operation block named {blockname}"
        )


async def create_shell_block(shell: PrefectShellSetup) -> str:
    """Create a prefect shell block"""
    if not isinstance(shell, PrefectShellSetup):
        raise TypeError("shell must be a PrefectShellSetup")

    shell_operation_block = ShellOperation(
        commands=shell.commands, env=shell.env, working_dir=shell.workingDir
    )
    try:
        block_name_for_save = cleaned_name_for_prefectblock(shell.blockName)
        await shell_operation_block.save(block_name_for_save)
    except Exception as error:
        logger.exception(error)
        raise PrefectException("failed to create shell block") from error
    logger.info("created shell operation block %s", shell.blockName)
    return _block_id(shell_operation_block)


def delete_shell_block(blockid: str) -> dict:
    """Delete a prefect shell block"""
    if not isinstance(blockid, str):
        raise TypeError("blockid must be a string")

    logger.info("deleting shell operation block %s", blockid)
    return prefect_delete(f"block_documents/{blockid}")


# ================================================================================================
async def get_dbtcore_block_id(blockname: str) -> str | None:
    """look up a dbt core operation block by name and return block_id"""
    if not isinstance(blockname, str):
        raise TypeError("blockname must be a string")

    try:
        block = await DbtCoreOperation.load(blockname)
        return _block_id(block)
    except ValueError:
        # pylint: disable=raise-missing-from
        raise HTTPException(
            status_code=404, detail=f"No dbt core operation block named {blockname}"
        )


async def _create_dbt_cli_profile(payload: DbtCoreCreate) -> DbtCliProfile:
    """credentials are decrypted by now"""
    if not isinstance(payload, DbtCoreCreate):
        raise TypeError("payload must be a DbtCoreCreate")
    logger.info(payload)

    if payload.wtype == "postgres":
        target_configs = TargetConfigs(
            type="postgres",
            schema=payload.profile.target_configs_schema,
            extras={
                "user": payload.credentials["username"],
                "password": payload.credentials["password"],
                "dbname": payload.credentials["database"],
                "host": payload.credentials["host"],
                "port": payload.credentials["port"],
            },
        )

    elif payload.wtype == "bigquery":
        dbcredentials = GcpCredentials(service_account_info=payload.credentials)
        target_configs = BigQueryTargetConfigs(
            credentials=dbcredentials,
            schema=payload.profile.target_configs_schema,
            extras={
                "location": payload.bqlocation,
            },
        )
    else:
        raise PrefectException("unknown wtype: " + payload.wtype)

    try:
        dbt_cli_profile = DbtCliProfile(
            name=payload.profile.name,
            target=payload.profile.target,
            target_configs=target_configs,
        )
        await dbt_cli_profile.save(
            cleaned_name_for_prefectblock(payload.profile.name), overwrite=True
        )
    except Exception as error:
        logger.exception(error)
        raise PrefectException("failed to create dbt cli profile") from error

    return dbt_cli_profile


async def create_dbt_core_block(payload: DbtCoreCreate):
    """Create a dbt core block in prefect"""
    if not isinstance(payload, DbtCoreCreate):
        raise TypeError("payload must be a DbtCoreCreate")
    logger.info(payload)

    dbt_cli_profile = await _create_dbt_cli_profile(payload)
    dbt_core_operation = DbtCoreOperation(
        commands=payload.commands,
        env=payload.env,
        working_dir=payload.working_dir,
        profiles_dir=payload.profiles_dir,
        project_dir=payload.project_dir,
        dbt_cli_profile=dbt_cli_profile,
    )
    cleaned_blockname = cleaned_name_for_prefectblock(payload.blockName)
    try:
        await dbt_core_operation.save(cleaned_blockname, overwrite=True)
    except Exception as error:
        logger.exception(error)
        raise PrefectException("failed to create dbt core op block") from error

    logger.info("created dbt core operation block %s", payload.blockName)

    return _block_id(dbt_core_operation), cleaned_blockname


def delete_dbt_core_block(block_id: str) -> dict:
    """Delete a dbt core block in prefect"""
    if not isinstance(block_id, str):
        raise TypeError("block_id must be a string")

    logger.info("deleting dbt core operation block %s", block_id)
    return prefect_delete(f"block_documents/{block_id}")


async def update_postgres_credentials(dbt_blockname, new_extras):
    """updates the database credentials inside a dbt postgres block"""
    try:
        block: DbtCoreOperation = await DbtCoreOperation.load(dbt_blockname)
    except Exception as error:
        raise PrefectException("no dbt core op block named " + dbt_blockname) from error

    if block.dbt_cli_profile.target_configs.type != "postgres":
        raise TypeError("wrong blocktype")

    aliases = {
        "dbname": "database",
        "username": "user",
    }

    extras = block.dbt_cli_profile.target_configs.dict()["extras"]
    cleaned_extras = {}
    # copy existing extras over to cleaned_extras with the right keys
    for key, value in extras.items():
        cleaned_extras[aliases.get(key, key)] = value

    # copy new extras over to cleaned_extras with the right keys
    for key, value in new_extras.items():
        cleaned_extras[aliases.get(key, key)] = value

    block.dbt_cli_profile.target_configs = TargetConfigs(
        type=block.dbt_cli_profile.target_configs.type,
        schema=block.dbt_cli_profile.target_configs.dict()["schema"],
        extras=cleaned_extras,
    )

    try:
        await block.dbt_cli_profile.save(
            name=block.dbt_cli_profile.name, overwrite=True
        )
        await block.save(dbt_blockname, overwrite=True)
    except Exception as error:
        logger.exception(error)
        raise PrefectException("failed to update dbt cli profile [postgres]") from error


async def update_bigquery_credentials(dbt_blockname: str, credentials: dict):
    """updates the database credentials inside a dbt bigquery block"""
    try:
        block: DbtCoreOperation = await DbtCoreOperation.load(dbt_blockname)
    except Exception as error:
        raise PrefectException("no dbt core op block named " + dbt_blockname) from error

    if block.dbt_cli_profile.target_configs.type != "bigquery":
        raise TypeError("wrong blocktype")

    dbcredentials = GcpCredentials(service_account_info=credentials)

    block.dbt_cli_profile.target_configs = BigQueryTargetConfigs(
        credentials=dbcredentials,
        schema=block.dbt_cli_profile.target_configs.dict()["schema_"],
        extras=block.dbt_cli_profile.target_configs.dict()["extras"],
    )

    try:
        await block.dbt_cli_profile.save(
            name=cleaned_name_for_prefectblock(block.dbt_cli_profile.name),
            overwrite=True,
        )
        await block.save(dbt_blockname, overwrite=True)
    except Exception as error:
        logger.exception(error)
        raise PrefectException("failed to update dbt cli profile [bigquery]") from error


async def update_target_configs_schema(dbt_blockname: str, target_configs_schema: str):
    """updates the target inside a dbt bigquery block"""
    try:
        block: DbtCoreOperation = await DbtCoreOperation.load(dbt_blockname)
    except Exception as error:
        raise PrefectException("no dbt core op block named " + dbt_blockname) from error

    block.dbt_cli_profile.target_configs.schema = target_configs_schema

    try:
        await block.dbt_cli_profile.save(
            name=cleaned_name_for_prefectblock(block.dbt_cli_profile.name),
            overwrite=True,
        )
        await block.save(dbt_blockname, overwrite=True)
    except Exception as error:
        logger.exception(error)
        raise PrefectException(
            "failed to update dbt cli profile target_configs schema for "
            + dbt_blockname
        ) from error


# ================================================================================================
async def post_deployment(payload: DeploymentCreate) -> dict:
    """create a deployment from a flow and a schedule"""
    if not isinstance(payload, DeploymentCreate):
        raise TypeError("payload must be a DeploymentCreate")
    logger.info(payload)

    logger.info(payload)

    deployment = await Deployment.build_from_flow(
        flow=deployment_schedule_flow.with_options(name=payload.flow_name),
        name=payload.deployment_name,
        work_queue_name="ddp",
        tags=[payload.org_slug],
    )
    deployment.parameters = {
        "airbyte_blocks": payload.connection_blocks,
        "dbt_blocks": payload.dbt_blocks,
    }
    deployment.schedule = CronSchedule(cron=payload.cron) if payload.cron else None
    try:
        deployment_id = await deployment.apply()
    except Exception as error:
        logger.exception(error)
        raise PrefectException("failed to create deployment") from error
    return {"id": deployment_id, "name": deployment.name}


def get_deployment(deployment_id: str) -> dict:
    """Fetch deployment and its details"""
    if not isinstance(deployment_id, str):
        raise TypeError("deployment_id must be a string")

    res = prefect_get(f"deployments/{deployment_id}")
    logger.info("Fetched deployment with ID: %s", deployment_id)
    return res


def get_flow_runs_by_deployment_id(deployment_id: str, limit: int) -> list:
    """
    Fetch flow runs of a deployment that are FAILED/COMPLETED,
    sorted by descending start time of each run
    """
    if not isinstance(deployment_id, str):
        raise TypeError("deployment_id must be a string")
    if not isinstance(limit, int):
        raise TypeError("limit must be an integer")
    if limit < 0:
        raise ValueError("limit must be a positive integer")
    logger.info("fetching flow runs for deployment %s", deployment_id)

    query = {
        "sort": "START_TIME_DESC",
        "deployments": {"id": {"any_": [deployment_id]}},
        "flow_runs": {
            "operator": "and_",
            "state": {"type": {"any_": [FLOW_RUN_COMPLETED, FLOW_RUN_FAILED]}},
        },
    }

    if limit > 0:
        query["limit"] = limit

    flow_runs = []

    try:
        result = prefect_post("flow_runs/filter", query)
    except Exception as error:
        logger.exception(error)
        raise PrefectException(
            f"failed to fetch flow_runs for deployment {deployment_id}"
        ) from error
    for flow_run in result:
        flow_runs.append(
            {
                "id": flow_run["id"],
                "name": flow_run["name"],
                "tags": flow_run["tags"],
                "startTime": flow_run["start_time"],
                "expectedStartTime": flow_run["expected_start_time"],
                "totalRunTime": flow_run["total_run_time"],
                "status": flow_run["state"]["type"],
            }
        )

    return flow_runs


def get_deployments_by_filter(org_slug: str, deployment_ids=None) -> list:
    # pylint: disable=dangerous-default-value
    """fetch all deployments by org"""
    if not isinstance(org_slug, str):
        raise TypeError("org_slug must be a string")
    if not isinstance(deployment_ids, list):
        raise TypeError("deployment_ids must be a list")
    query = {
        "deployments": {
            "operator": "and_",
            "tags": {"all_": [org_slug]},
            "id": {"any_": deployment_ids},
        }
    }

    try:
        res = prefect_post(
            "deployments/filter",
            query,
        )
    except Exception as error:
        logger.exception(error)
        raise PrefectException("failed to fetch deployments by filter") from error

    deployments = []

    for deployment in res:
        deployments.append(
            {
                "name": deployment["name"],
                "deploymentId": deployment["id"],
                "tags": deployment["tags"],
                "cron": deployment["schedule"]["cron"],
                "isScheduleActive": deployment["is_schedule_active"],
            }
        )

    return deployments


async def post_deployment_flow_run(deployment_id: str):
    # pylint: disable=broad-exception-caught
    """Create deployment flow run"""

    if not isinstance(deployment_id, str):
        raise TypeError("deployment_id must be a string")
    try:
        flow_run = await run_deployment(deployment_id, timeout=0)
        return {"flow_run_id": flow_run.id}
    except Exception as exc:
        logger.exception(exc)
        raise PrefectException("Failed to create deployment flow run") from exc


def parse_log(log: dict) -> dict:
    """select level, timestamp, message from ..."""
    if not isinstance(log, dict):
        raise TypeError("log must be a dict")
    return {
        "level": log["level"],
        "timestamp": log["timestamp"],
        "message": log["message"],
    }


def traverse_flow_run_graph(flow_run_id: str, flow_runs: list) -> list:
    """
    This recursive function will read through the graph
    and return all sub flow run ids of the parent that can potentially have logs
    """
    if not isinstance(flow_run_id, str):
        raise TypeError("flow_run_id must be a string")
    if not isinstance(flow_runs, list):
        raise TypeError("flow_runs must be a list")
    flow_runs.append(flow_run_id)
    if flow_run_id is None:
        return flow_runs

    flow_graph_data = prefect_get(f"flow_runs/{flow_run_id}/graph")

    if len(flow_graph_data) == 0:
        return flow_runs

    for flow in flow_graph_data:
        if (
            "state" in flow
            and "state_details" in flow["state"]
            and flow["state"]["state_details"]["child_flow_run_id"]
        ):
            traverse_flow_run_graph(
                flow["state"]["state_details"]["child_flow_run_id"], flow_runs
            )

    return flow_runs


def get_flow_run_logs(flow_run_id: str, offset: int) -> dict:
    """return logs from a flow run"""
    if not isinstance(flow_run_id, str):
        raise TypeError("flow_run_id must be a string")
    if not isinstance(offset, int):
        raise TypeError("offset must be an integer")
    flow_run_ids = traverse_flow_run_graph(flow_run_id, [])

    logs = prefect_post(
        "logs/filter",
        {
            "logs": {
                "operator": "and_",
                "flow_run_id": {"any_": flow_run_ids},
            },
            "sort": "TIMESTAMP_ASC",
            "offset": offset,
        },
    )
    return {
        "offset": offset,
        "logs": list(map(parse_log, logs)),
    }


def get_flow_runs_by_name(flow_run_name: str) -> dict:
    """Query flow run from the name"""
    if not isinstance(flow_run_name, str):
        raise TypeError("flow_run_name must be a string")
    query = {
        "flow_runs": {"operator": "and_", "name": {"any_": [flow_run_name]}},
    }

    try:
        flow_runs = prefect_post("flow_runs/filter", query)
    except Exception as error:
        logger.exception(error)
        raise PrefectException("failed to fetch flow-runs by name") from error
    return flow_runs


def get_flow_run(flow_run_id: str) -> dict:
    """Get a flow run by its id"""
    try:
        flow_run = prefect_get(f"flow_runs/{flow_run_id}")
    except Exception as err:
        logger.exception(err)
        raise PrefectException("failed to fetch a flow-run") from err
    return flow_run


def set_deployment_schedule(deployment_id: str, status: str) -> None:
    """Set deployment schedule to active or inactive"""
    # both the apis return null below
    if status == "active":
        prefect_post(f"deployments/{deployment_id}/set_schedule_active", {})

    if status == "inactive":
        prefect_post(f"deployments/{deployment_id}/set_schedule_inactive", {})

    return None
