# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Unit tests for GTF Celery executor state transitions."""

from unittest.mock import MagicMock, patch
from uuid import UUID

from superset_core.tasks.types import TaskStatus

from superset.tasks.scheduler import execute_task

TEST_UUID = UUID("b8b61b7b-1cd3-4a31-a74a-0a95341afc06")


def test_async_execution_delegates_to_shared_runner() -> None:
    task = MagicMock(
        uuid=TEST_UUID,
        status=TaskStatus.PENDING.value,
        properties_dict={"execution_mode": "async", "timeout": 1},
        payload_dict={},
    )
    final_task = MagicMock(status=TaskStatus.SUCCESS.value)

    def executor() -> None:
        pass

    with (
        patch("superset.tasks.scheduler.TaskDAO.find_one_or_none", return_value=task),
        patch("superset.tasks.scheduler.TaskRegistry.get_executor") as get_executor,
        patch(
            "superset.tasks.executor.execute_task_callable",
            return_value=final_task,
        ) as execute,
    ):
        get_executor.return_value = executor

        result = execute_task.run(str(TEST_UUID), "test.task", (), {})

    assert result == {
        "status": TaskStatus.SUCCESS.value,
        "task_uuid": str(TEST_UUID),
    }
    execute.assert_called_once_with(
        task=task,
        task_name="test.task",
        function=executor,
        args=(),
        kwargs={},
        timeout=1,
    )
