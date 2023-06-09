from unittest.mock import AsyncMock, patch

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from proxy.main import (
    airbytesync,
    app,
    dbtrun,
    delete_block,
    delete_deployment,
    get_airbyte_connection_by_blockid,
    get_airbyte_connection_by_blockname,
    get_airbyte_server,
    get_dbtcore,
    get_flow_run_logs_paginated,
    get_flow_runs,
    get_flowrun,
    get_read_deployment,
    get_shell,
    post_airbyte_connection,
    post_airbyte_connection_blocks,
    post_airbyte_server,
    post_bulk_delete_blocks,
    post_create_deployment_flow_run,
    post_dataflow,
    post_dbtcore,
    post_deployment_set_schedule,
    post_deployments,
    post_shell,
    sync_airbyte_connection_flow,
    sync_dbtcore_flow,
)

from proxy.schemas import (
    AirbyteConnectionBlocksFetch,
    AirbyteConnectionCreate,
    AirbyteServerCreate,
    DbtCoreCreate,
    DbtProfileCreate,
    DeploymentCreate,
    DeploymentFetch,
    FlowRunRequest,
    PrefectBlocksDelete,
    PrefectShellSetup,
    RunFlow,
)

app = FastAPI()
client = TestClient(app)


def test_airbytesync_success():
    block_name = "example_block"
    flow_name = "example_flow"
    flow_run_name = "example_flow_run"
    inner_result = {"status": "success", "result": "example_result"}
    expected_result = {"status": "success", "result": inner_result}

    with patch(
        "proxy.main.run_airbyte_connection_flow"
    ) as mock_run_airbyte_connection_flow:
        with patch.object(
            mock_run_airbyte_connection_flow.with_options.return_value, "with_options"
        ) as mock_with_options:
            mock_with_options.return_value = lambda x: inner_result
            result = airbytesync(block_name, flow_name, flow_run_name)
            assert result == expected_result


def test_airbytesync_failure():
    block_name = "example_block"
    flow_name = "example_flow"
    flow_run_name = "example_flow_run"
    inner_result = {"status": "failed", "result": "example_failure_result"}
    expected_result = {"status": "success", "result": inner_result}

    with patch(
        "proxy.main.run_airbyte_connection_flow"
    ) as mock_run_airbyte_connection_flow:
        with patch.object(
            mock_run_airbyte_connection_flow.with_options.return_value, "with_options"
        ) as mock_with_options:
            mock_with_options.return_value = lambda x: inner_result
            result = airbytesync(block_name, flow_name, flow_run_name)
            assert result == expected_result


def test_airbyte_sync_with_invalid_block_name():
    invalid_block_name = None
    flow_name = "example_flow"
    flow_run_name = "example_flow_run"

    with pytest.raises(Exception):
        airbytesync(invalid_block_name, flow_name, flow_run_name)


def test_airbyte_sync_invalid_flow_name():
    block_name = "example_block"
    invalid_flow_name = None
    flow_run_name = "example_flow_run"

    with pytest.raises(Exception):
        airbytesync(block_name, invalid_flow_name, flow_run_name)


def test_airbyte_sync_invalid_flow_run_name():
    block_name = "example_block"
    flow_name = "example_flow"
    invalid_flow_run_name = None

    with pytest.raises(Exception):
        airbytesync(block_name, flow_name, invalid_flow_run_name)


def test_dbtrun_success():
    block_name = "example_block"
    flow_name = "example_flow"
    flow_run_name = "example_flow_run"
    expected_result = {"result": "example_result"}

    with patch("proxy.main.run_dbtcore_flow") as mock_run_dbtcore_flow:
        with patch.object(
            mock_run_dbtcore_flow.with_options.return_value, "with_options"
        ) as mock_with_options:
            mock_with_options.return_value = lambda x: expected_result
            result = dbtrun(block_name, flow_name, flow_run_name)
            assert result == expected_result


def test_dbtrun_failure():
    block_name = "example_block"
    flow_name = "example_flow"
    flow_run_name = "example_flow_run"
    expected_result = {"result": "example_failure_result", "status": "failed"}

    with patch("proxy.main.run_dbtcore_flow") as mock_run_dbtcore_flow:
        with patch.object(
            mock_run_dbtcore_flow.with_options.return_value, "with_options"
        ) as mock_with_options:
            mock_with_options.return_value = lambda x: expected_result
            result = dbtrun(block_name, flow_name, flow_run_name)
            assert result == expected_result


def test_run_dbt_exception():
    block_name = "example_block"
    flow_name = "example_flow"
    flow_run_name = "example_flow_run"
    expected_result = {"result": "example_failure_result", "status": "failed"}

    def mock_with_options(*args, **kwargs):
        raise HTTPException(status_code=400, detail="Job 12345 failed.")

    with patch("proxy.main.run_dbtcore_flow.with_options", new=mock_with_options):
        try:
            result = dbtrun(block_name, flow_name, flow_run_name)
            assert result == expected_result
        except HTTPException as e:
            assert e.status_code == 400
            assert e.detail == "Job 12345 failed."


def test_dbtrun_invalid_block_name():
    block_name = None
    flow_name = "example_flow"
    flow_run_name = "example_flow_run"

    with pytest.raises(TypeError):
        dbtrun(block_name, flow_name, flow_run_name)


def test_dbtrun_invalid_flow_name():
    block_name = "example_block"
    flow_name = None
    flow_run_name = "example_flow_run"

    with pytest.raises(TypeError):
        dbtrun(block_name, flow_name, flow_run_name)


def test_dbtrun_invalid_flow_run_name():
    block_name = "example_block"
    flow_name = "example_flow"
    flow_run_name = None

    with pytest.raises(TypeError):
        dbtrun(block_name, flow_name, flow_run_name)


@pytest.mark.asyncio
async def test_post_bulk_delete_blocks_success():
    payload = PrefectBlocksDelete(block_ids=["12345", "67890"])
    with patch("proxy.main.requests.delete") as mock_delete:
        mock_delete.return_value.raise_for_status.return_value = None
        response = await post_bulk_delete_blocks(payload)
        assert response == {"deleted_blockids": ["12345", "67890"]}


def test_post_airbyte_connection_blocks_success():
    payload = AirbyteConnectionBlocksFetch(block_names=["test_block"])
    with patch(
        "proxy.main.post_filter_blocks",
        return_value=[
            {"name": "test_block", "data": {"connection_id": "12345"}, "id": "67890"}
        ],
    ):
        response = post_airbyte_connection_blocks(payload)
        assert response == [
            {"name": "test_block", "connectionId": "12345", "id": "67890"}
        ]


@pytest.mark.asyncio
async def test_get_airbyte_server_success():
    with patch("proxy.main.get_airbyte_server_block_id", return_value="12345"):
        response = await get_airbyte_server("test_block")
        assert response == {"block_id": "12345"}


@pytest.mark.asyncio
async def test_get_airbyte_server_failure():
    with patch("proxy.main.get_airbyte_server_block_id") as mock_get_block_id:
        mock_get_block_id.side_effect = Exception("Test error")
        with pytest.raises(HTTPException) as excinfo:
            await get_airbyte_server("test_block")
        assert excinfo.value.status_code == 500
        assert excinfo.value.detail == "Internal server error"


@pytest.mark.asyncio
async def test_get_airbyte_server_invalid_block_name():
    with pytest.raises(TypeError) as excinfo:
        await get_airbyte_server(None)
    assert excinfo.value.args[0] == "blockname must be a string"

        
@pytest.mark.asyncio
async def test_post_airbyte_server_success():
    payload = AirbyteServerCreate(
        blockName="testserver",
        serverHost="http://test-server.com",
        serverPort=8000,
        apiVersion="v1",
    )
    with patch("proxy.main.create_airbyte_server_block", return_value="12345"):
        response = await post_airbyte_server(payload)
        assert response == {"block_id": "12345"}


@pytest.mark.asyncio
async def test_post_airbyte_server_failure():
    payload = AirbyteServerCreate(
        blockName="testserver",
        serverHost="http://test-server.com",
        serverPort=8000,
        apiVersion="v1",
    )
    with patch(
        "proxy.main.create_airbyte_server_block", side_effect=Exception("test error")
    ):
        with pytest.raises(HTTPException) as excinfo:
            await post_airbyte_server(payload)
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "failed to create airbyte server block"


@pytest.mark.asyncio
async def test_post_airbyte_server_with_invalid_payload():
    payload = None
    with pytest.raises(TypeError) as excinfo:
        await post_airbyte_server(payload)
    assert excinfo.value.args[0] == "payload is invalid"


@pytest.mark.asyncio
async def test_get_airbyte_connection_by_blockname_success():
    with patch("proxy.main.get_airbyte_connection_block_id", return_value="12345"):
        response = await get_airbyte_connection_by_blockname("test_block")
        assert response == {"block_id": "12345"}


@pytest.mark.asyncio
async def test_get_airbyte_connection_by_blockname_failure():
    with patch("proxy.main.get_airbyte_connection_block_id", return_value=None):
        with pytest.raises(HTTPException) as excinfo:
            await get_airbyte_connection_by_blockname("test_block")
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "no block having name test_block"


@pytest.mark.asyncio
async def test_get_airbyte_connection_by_blockname_invalid_blockname():
    with patch(
        "proxy.main.get_airbyte_connection_block_id"
    ) as get_airbyte_connection_block_id_mock:
        get_airbyte_connection_block_id_mock.return_value = None
        blockname = "invalid_blockname"
        with pytest.raises(HTTPException) as exc_info:
            await get_airbyte_connection_by_blockname(blockname)
        assert exc_info.value.status_code == 400
        assert exc_info.value.detail == "no block having name " + blockname


@pytest.mark.asyncio
async def test_get_airbyte_connection_by_blockid_success():
    block_data = {"id": "12345", "name": "test_block", "url": "http://test-block.com"}
    with patch("proxy.main.get_airbyte_connection_block", return_value=block_data):
        response = await get_airbyte_connection_by_blockid("12345")
        assert response == block_data


@pytest.mark.asyncio
async def test_get_airbyte_connection_by_blockid_failure():
    with patch("proxy.main.get_airbyte_connection_block", return_value=None):
        with pytest.raises(HTTPException) as excinfo:
            await get_airbyte_connection_by_blockid("12345")
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "no block having id 12345"


@pytest.mark.asyncio
async def test_get_airbyte_connection_by_blockid_invalid_blockid():
    with patch(
        "proxy.main.get_airbyte_connection_block"
    ) as get_airbyte_connection_block_mock:
        get_airbyte_connection_block_mock.return_value = None
        blockid = "invalid_blockid"
        with pytest.raises(HTTPException) as exc_info:
            await get_airbyte_connection_by_blockid(blockid)
        assert exc_info.value.status_code == 400
        assert exc_info.value.detail == "no block having id " + blockid


@pytest.mark.asyncio
async def test_post_airbyte_connection_success():
    payload = AirbyteConnectionCreate(
        serverBlockName="testserver",
        connectionId="12345",
        connectionBlockName="test_connection",
    )
    with patch("proxy.main.create_airbyte_connection_block", return_value="12345"):
        response = await post_airbyte_connection(payload)
        assert response == {"block_id": "12345"}


@pytest.mark.asyncio
async def test_post_airbyte_connection_failure():
    payload = AirbyteConnectionCreate(
        serverBlockName="testserver",
        connectionId="12345",
        connectionBlockName="test_connection",
    )
    with patch(
        "proxy.main.create_airbyte_connection_block",
        side_effect=Exception("test error"),
    ):
        with pytest.raises(HTTPException) as excinfo:
            await post_airbyte_connection(payload)
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "failed to create airbyte connection block"


@pytest.mark.asyncio
async def test_post_airbyte_connection_with_invalid_payload():
    payload = AirbyteConnectionCreate(
        serverBlockName="testserver",
        connectionId="12345",
        connectionBlockName="test_connection",
    )
    with pytest.raises(HTTPException) as excinfo:
        await post_airbyte_connection(payload)
    assert excinfo.value.status_code == 400
    assert excinfo.value.detail == "failed to create airbyte connection block"


@pytest.mark.asyncio
async def test_get_shell_success():
    with patch("proxy.main.get_shell_block_id", return_value="12345"):
        response = await get_shell("test_block")
        assert response == {"block_id": "12345"}


@pytest.mark.asyncio
async def test_get_shell_failure():
    with patch("proxy.main.get_shell_block_id", return_value=None):
        with pytest.raises(HTTPException) as excinfo:
            await get_shell("test_block")
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "no block having name test_block"


@pytest.mark.asyncio
async def test_get_shell_invalid_blockname():
    with pytest.raises(TypeError) as excinfo:
        await get_shell(None)
    assert excinfo.value.args[0] == "blockname must be a string"


@pytest.mark.asyncio
async def test_post_shell_success():
    payload = PrefectShellSetup(
        blockName="test_shell",
        commands=['echo "Hello, World!"'],
        workingDir="test_dir",
        env={"TEST_ENV": "test_value"},
    )
    with patch("proxy.main.create_shell_block", return_value="12345"):
        response = await post_shell(payload)
        assert response == {"block_id": "12345"}


@pytest.mark.asyncio
async def test_post_shell_failure():
    payload = PrefectShellSetup(
        blockName="test_shell",
        commands=['echo "Hello, World!"'],
        workingDir="test_dir",
        env={"TEST_ENV": "test_value"},
    )
    with patch("proxy.main.create_shell_block", side_effect=Exception("test error")):
        with pytest.raises(HTTPException) as excinfo:
            await post_shell(payload)
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "failed to create shell block"


@pytest.mark.asyncio
async def test_post_shell_invalid_payload():
    payload = None
    with pytest.raises(TypeError) as excinfo:
        await post_shell(payload)
    assert excinfo.value.args[0] == "payload is invalid"


@pytest.mark.asyncio
async def test_get_dbtcore_success():
    with patch("proxy.main.get_dbtcore_block_id", return_value="12345"):
        response = await get_dbtcore("test_block")
        assert response == {"block_id": "12345"}


@pytest.mark.asyncio
async def test_get_dbtcore_failure():
    with patch("proxy.main.get_dbtcore_block_id", return_value=None):
        with pytest.raises(HTTPException) as excinfo:
            await get_dbtcore("test_block")
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "no block having name test_block"


@pytest.mark.asyncio
async def test_get_dbtcore_invalid_blockname():
    with pytest.raises(TypeError) as excinfo:
        await get_dbtcore(None)
    assert excinfo.value.args[0] == "blockname must be a string"


@pytest.mark.asyncio
async def test_post_dbtcore_success():
    payload = DbtCoreCreate(
        blockName="test_dbt",
        profile=DbtProfileCreate(
            name="test_profile",
            target="test_target",
            target_configs_schema="test_schema",
        ),
        wtype="postgres",
        credentials={"TEST_CREDS": "test_creds"},
        commands=['echo "Hello, World!"'],
        env={"TEST_ENV": "test_value"},
        working_dir="test_dir",
        profiles_dir="test_profiles_dir",
        project_dir="test_project_dir",
    )
    with patch(
        "proxy.main.create_dbt_core_block", return_value=("12345", "test_dbt_cleaned")
    ):
        response = await post_dbtcore(payload)
        assert response == {"block_id": "12345", "block_name": "test_dbt_cleaned"}


@pytest.mark.asyncio
async def test_post_dbtcore_failure():
    payload = DbtCoreCreate(
        blockName="test_dbt",
        profile=DbtProfileCreate(
            name="test_profile",
            target="test_target",
            target_configs_schema="test_schema",
        ),
        wtype="postgres",
        credentials={"TEST_CREDS": "test_creds"},
        commands=['echo "Hello, World!"'],
        env={"TEST_ENV": "test_value"},
        working_dir="test_dir",
        profiles_dir="test_profiles_dir",
        project_dir="test_project_dir",
    )
    with patch("proxy.main.create_dbt_core_block", side_effect=Exception("test error")):
        with pytest.raises(HTTPException) as excinfo:
            await post_dbtcore(payload)
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "failed to create dbt core block"


@pytest.mark.asyncio
async def test_post_dbtcore_invalid_payload():
    payload = None
    with pytest.raises(TypeError) as excinfo:
        await post_dbtcore(payload)
    assert excinfo.value.args[0] == "payload is invalid"


@pytest.mark.asyncio
async def test_delete_block_success():
    with patch("proxy.main.requests.delete") as mock_delete:
        mock_delete.return_value.status_code = 204
        response = await delete_block("12345")
        assert response is None


@pytest.mark.asyncio
async def test_delete_block_failure():
    with patch("proxy.main.requests.delete") as mock_delete:
        mock_delete.return_value.raise_for_status.side_effect = Exception("test error")
        mock_delete.return_value.text = "test error"
        with pytest.raises(HTTPException) as excinfo:
            await delete_block("12345")
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "test error"


@pytest.mark.asyncio
async def test_delete_block_invalid_blockid():
    with pytest.raises(TypeError) as excinfo:
        await delete_block(None)
    assert excinfo.value.args[0] == "block_id must be a string"


@pytest.mark.asyncio
async def test_sync_airbyte_connection_flow_success():
    payload = RunFlow(
        blockName="test_block", flowName="test_flow", flowRunName="test_flow_run"
    )
    with patch("proxy.main.airbytesync", return_value="test result"):
        response = await sync_airbyte_connection_flow(payload)
        assert response == "test result"


@pytest.mark.asyncio
async def test_sync_airbyte_connection_flow_failure():
    payload = RunFlow(blockName="", flowName="test_flow", flowRunName="test_flow_run")
    with pytest.raises(HTTPException) as excinfo:
        await sync_airbyte_connection_flow(payload)
    assert excinfo.value.status_code == 400
    assert excinfo.value.detail == "received empty blockName"


@pytest.mark.asyncio
async def test_sync_airbyte_connection_flow_invalid_payload():
    payload = None
    with pytest.raises(TypeError) as excinfo:
        await sync_airbyte_connection_flow(payload)
    assert excinfo.value.args[0] == "payload is invalid"


@pytest.mark.asyncio
async def test_sync_dbtcore_flow_success():
    payload = RunFlow(
        blockName="test_block", flowName="test_flow", flowRunName="test_flow_run"
    )
    with patch("proxy.main.dbtrun", return_value="test result"):
        response = await sync_dbtcore_flow(payload)
        assert response == {"status": "success", "result": "test result"}


@pytest.mark.asyncio
async def test_sync_dbtcore_flow_failure():
    payload = RunFlow(blockName="", flowName="test_flow", flowRunName="test_flow_run")
    with pytest.raises(HTTPException) as excinfo:
        await sync_dbtcore_flow(payload)
    assert excinfo.value.status_code == 400
    assert excinfo.value.detail == "received empty blockName"


@pytest.mark.asyncio
async def test_sync_dbtcore_flow_invalid_payload():
    payload = None
    with pytest.raises(TypeError) as excinfo:
        await sync_dbtcore_flow(payload)
    assert excinfo.value.args[0] == "payload is invalid"


@pytest.mark.asyncio
async def test_post_dataflow_success():
    payload = DeploymentCreate(
        flow_name="test_block",
        deployment_name="FULL",
        org_slug="test_org",
        connection_blocks=[{"name": "test_block"}],
        dbt_blocks=[{"name": "test_block"}],
        cron="* * * * *",
    )
    with patch("proxy.main.post_deployment", return_value={"id": "67890"}):
        response = await post_dataflow(payload)
        assert response == {"deployment": {"id": "67890"}}


@pytest.mark.asyncio
async def test_post_dataflow_failure():
    payload = DeploymentCreate(
        flow_name="test_block",
        deployment_name="FULL",
        org_slug="test_org",
        connection_blocks=[{"name": "test_block"}],
        dbt_blocks=[{"name": "test_block"}],
        cron="* * * * *",
    )
    with patch("proxy.main.post_deployment", side_effect=Exception("test error")):
        with pytest.raises(HTTPException) as excinfo:
            await post_dataflow(payload)
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "failed to create deployment"


@pytest.mark.asyncio
async def test_post_dataflow_invalid_payload():
    payload = None
    with pytest.raises(TypeError) as excinfo:
        await post_dataflow(payload)
    assert excinfo.value.args[0] == "payload is invalid"


@pytest.mark.asyncio
async def test_get_flowrun_success():
    payload = FlowRunRequest(name="test_flow_run")
    with patch("proxy.main.get_flow_runs_by_name", return_value=[{"id": "12345"}]):
        response = await get_flowrun(payload)
        assert response == {"flow_run": {"id": "12345"}}


@pytest.mark.asyncio
async def test_get_flowrun_failure():
    payload = FlowRunRequest(name="test_flow_run")
    with patch("proxy.main.get_flow_runs_by_name", return_value=[]):
        with pytest.raises(HTTPException) as excinfo:
            await get_flowrun(payload)
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "no such flow run"


@pytest.mark.asyncio
async def test_get_flowrun_invalid_payload():
    payload = None
    with pytest.raises(TypeError) as excinfo:
        await get_flowrun(payload)
    assert excinfo.value.args[0] == "payload is invalid"


def test_get_flow_runs_success():
    with patch(
        "proxy.main.get_flow_runs_by_deployment_id", return_value=[{"id": "12345"}]
    ):
        response = get_flow_runs("67890")
        assert response == {"flow_runs": [{"id": "12345"}]}


def test_get_flow_runs_failure():
    with patch(
        "proxy.main.get_flow_runs_by_deployment_id", side_effect=Exception("test error")
    ):
        with pytest.raises(HTTPException) as excinfo:
            get_flow_runs("67890")
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "failed to fetch flow_runs for deployment"


def test_get_flow_runs_invalid_deployment_id():
    with pytest.raises(TypeError) as excinfo:
        get_flow_runs(None, 0)
    assert excinfo.value.args[0] == "deployment_id must be a string"


def test_get_flow_runs_with_invalid_limit():
    with pytest.raises(TypeError) as excinfo:
        get_flow_runs("67890", None)
    assert excinfo.value.args[0] == "limit must be an integer"


def test_get_flow_runs_limit_less_than_zero():
    with pytest.raises(ValueError) as excinfo:
        get_flow_runs("67890", -1)
    assert excinfo.value.args[0] == "limit must be positive"


def test_post_deployments_success():
    payload = DeploymentFetch(org_slug="test_org", deployment_ids=["12345"])
    with patch("proxy.main.get_deployments_by_filter", return_value=[{"id": "12345"}]):
        response = post_deployments(payload)
        assert response == {"deployments": [{"id": "12345"}]}


def test_post_deployments_failure():
    payload = DeploymentFetch(org_slug="test_org", deployment_ids=["12345"])
    with patch(
        "proxy.main.get_deployments_by_filter", side_effect=Exception("test error")
    ):
        with pytest.raises(HTTPException) as excinfo:
            post_deployments(payload)
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "failed to filter deployments"


def test_post_deployments_invalid_payload():
    payload = None
    with pytest.raises(TypeError) as excinfo:
        post_deployments(payload)
    assert excinfo.value.args[0] == "payload is invalid"


def test_get_flow_run_logs_paginated_success():
    with patch("proxy.main.get_flow_run_logs", return_value="test logs"):
        response = get_flow_run_logs_paginated("12345")
        assert response == "test logs"


def test_get_flow_run_logs_paginated_failure():
    with patch("proxy.main.get_flow_run_logs", side_effect=Exception("test error")):
        with pytest.raises(HTTPException) as excinfo:
            get_flow_run_logs_paginated("12345")
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "failed to fetch logs for flow_run"


def test_get_flow_run_logs_paginated_invalid_flow_run_id():
    with pytest.raises(TypeError) as excinfo:
        get_flow_run_logs_paginated(None, 0)
    assert excinfo.value.args[0] == "flow_run_id must be a string"


def test_get_flow_run_logs_paginated_invalid_offset():
    with pytest.raises(TypeError) as excinfo:
        get_flow_run_logs_paginated("12345", None)
    assert excinfo.value.args[0] == "offset must be an integer"


def test_get_flow_run_logs_paginated_offset_less_than_zero():
    with pytest.raises(ValueError) as excinfo:
        get_flow_run_logs_paginated("12345", -1)
    assert excinfo.value.args[0] == "offset must be positive"


def test_get_read_deployment_success():
    deployment_id = "test-deployment-id"
    mock_deployment_data = {
        "name": "test-deployment",
        "id": deployment_id,
        "tags": ["tag1", "tag2"],
        "schedule": {"cron": "* * * * *"},
        "is_schedule_active": True,
        "parameters": {"airbyte_blocks": []},
    }
    with patch("proxy.main.get_deployment") as mock_get_deployment:
        mock_get_deployment.return_value = mock_deployment_data
        response = get_read_deployment(deployment_id)
    assert response["deploymentId"] == deployment_id


def test_get_read_deployment_failure():
    with patch("proxy.main.get_deployment", side_effect=Exception("test error")):
        with pytest.raises(HTTPException) as excinfo:
            deployment_id = "test-deployment-id"
            get_read_deployment(deployment_id)
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "failed to fetch deployment " + deployment_id


def test_get_read_deployment_invalid_deployment_id():
    with pytest.raises(TypeError) as excinfo:
        get_read_deployment(None)
    assert excinfo.value.args[0] == "deployment_id must be a string"


def test_delete_deployment_success():
    with patch("proxy.main.requests.delete") as mock_delete:
        mock_delete.return_value.raise_for_status.return_value = None
        response = delete_deployment("12345")
        assert response is None


def test_delete_deployment_failure():
    with patch("proxy.main.requests.delete") as mock_delete:
        mock_delete.return_value.raise_for_status.side_effect = Exception("test error")
        mock_delete.return_value.text = "test error"
        with pytest.raises(HTTPException) as excinfo:
            delete_deployment("12345")
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "test error"


def test_delete_deployment_invalid_deployment_id():
    with pytest.raises(TypeError) as excinfo:
        delete_deployment(None)
    assert excinfo.value.args[0] == "deployment_id must be a string"


@pytest.mark.asyncio
async def test_post_create_deployment_flow_run_success():
    with patch("proxy.main.post_deployment_flow_run", return_value="test result"):
        response = await post_create_deployment_flow_run("12345")
        assert response == "test result"


@pytest.mark.asyncio
async def test_post_create_deployment_flow_run_failure():
    with patch(
        "proxy.main.post_deployment_flow_run", side_effect=Exception("test error")
    ):
        with pytest.raises(HTTPException) as excinfo:
            await post_create_deployment_flow_run("12345")
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "failed to create flow_run for deployment"


@pytest.mark.asyncio
async def test_post_create_deployment_flow_run_invalid_deployment_id():
    with pytest.raises(TypeError) as excinfo:
        await post_create_deployment_flow_run(None)
    assert excinfo.value.args[0] == "deployment_id must be a string"


def test_post_deployment_set_schedule_success():
    with patch("proxy.main.set_deployment_schedule"):
        response = post_deployment_set_schedule("12345", "active")
        assert response == {"success": 1}


def test_post_deployment_set_schedule_failure():
    with pytest.raises(HTTPException) as excinfo:
        post_deployment_set_schedule("12345", "invalid_status")
    assert excinfo.value.status_code == 422
    assert excinfo.value.detail == "incorrect status value"


def test_post_deployment_set_schedule_invalid_deployment_id():
    with pytest.raises(TypeError) as excinfo:
        post_deployment_set_schedule(None, "active")
    assert excinfo.value.args[0] == "deployment_id must be a string"


def test_post_deployment_set_schedule_invalid_status():
    with pytest.raises(TypeError) as excinfo:
        post_deployment_set_schedule("12345", None)
    assert excinfo.value.args[0] == "status must be a string"