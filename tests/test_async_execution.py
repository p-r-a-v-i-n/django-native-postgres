from unittest import mock

import pytest
from django.db import connections


@pytest.mark.asyncio
async def test_aexecute_forwards_query_to_process_executor():
    connection = connections["default"]
    executor = mock.Mock()
    executor.execute = mock.AsyncMock(return_value=object())
    sql = "SELECT id FROM example WHERE active = %s"
    params = (True,)

    with mock.patch.object(
        connection,
        "get_async_executor",
        return_value=executor,
        create=True,
    ):
        result = await connection.aexecute(sql, params)

    assert result is executor.execute.return_value
    executor.execute.assert_awaited_once_with(sql=sql, params=params)
    assert connection.connection is None
