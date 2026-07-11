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
"""Tests for the authoritative GTF execution lifecycle."""

from unittest.mock import MagicMock, patch
from uuid import UUID

from superset_core.tasks.types import TaskStatus

from superset.tasks.executor import execute_task_callable

TEST_UUID = UUID("b8b61b7b-1cd3-4a31-a74a-0a95341afc06")


def task_model(status: TaskStatus) -> MagicMock:
    return MagicMock(
        uuid=TEST_UUID,
        status=status.value,
        properties_dict={},
        payload_dict={},
    )


def test_success_claims_finalizing_before_publication() -> None:
    task = task_model(TaskStatus.PENDING)
    current = task_model(TaskStatus.FINALIZING)
    final = task_model(TaskStatus.SUCCESS)
    context = MagicMock(
        can_finalize=True,
        terminal_status=TaskStatus.SUCCESS,
    )
    context.properties_snapshot.return_value = {}
    function = MagicMock()
    stats_logger = MagicMock()

    with (
        patch("superset.tasks.executor.InternalStatusTransitionCommand") as transition,
        patch("superset.tasks.executor.TaskContext", return_value=context),
        patch("superset.tasks.executor.use_context"),
        patch(
            "superset.tasks.executor.TaskDAO.find_one_or_none",
            side_effect=[current, final],
        ),
        patch("superset.tasks.executor.TaskManager.publish_completion") as publish,
        patch(
            "superset.tasks.executor.current_app",
            MagicMock(config={"STATS_LOGGER": stats_logger}),
        ),
    ):
        transition.return_value.run.side_effect = [True, True, True]
        result = execute_task_callable(task, "test.task", function, (), {})

    assert result is final
    assert [call.kwargs["new_status"] for call in transition.call_args_list] == [
        TaskStatus.IN_PROGRESS,
        TaskStatus.FINALIZING,
        TaskStatus.SUCCESS,
    ]
    assert transition.call_args_list[2].kwargs["expected_status"] == (
        TaskStatus.FINALIZING
    )
    context._run_cleanup.assert_called_once_with(run_finalizers=True)
    publish.assert_called_once_with(TEST_UUID, TaskStatus.SUCCESS.value)
    stats_logger.incr.assert_called_once_with("gtf.task.success")


def test_cancel_winning_finalization_cas_cannot_become_success() -> None:
    task = task_model(TaskStatus.PENDING)
    aborting = task_model(TaskStatus.ABORTING)
    final = task_model(TaskStatus.ABORTED)
    context = MagicMock(
        can_finalize=False,
        terminal_status=TaskStatus.ABORTED,
    )
    context.properties_snapshot.return_value = {}
    stats_logger = MagicMock()

    with (
        patch("superset.tasks.executor.InternalStatusTransitionCommand") as transition,
        patch("superset.tasks.executor.TaskContext", return_value=context),
        patch("superset.tasks.executor.use_context"),
        patch(
            "superset.tasks.executor.TaskDAO.find_one_or_none",
            side_effect=[aborting, final],
        ),
        patch("superset.tasks.executor.TaskManager.publish_completion") as publish,
        patch(
            "superset.tasks.executor.current_app",
            MagicMock(config={"STATS_LOGGER": stats_logger}),
        ),
    ):
        transition.return_value.run.side_effect = [True, False, True]
        result = execute_task_callable(task, "test.task", lambda: None, (), {})

    assert result is final
    context.detect_abort_before_finalization.assert_called_once_with()
    context._run_cleanup.assert_called_once_with(run_finalizers=False)
    terminal = transition.call_args_list[2]
    assert terminal.kwargs["new_status"] == TaskStatus.ABORTED
    assert terminal.kwargs["expected_status"] == TaskStatus.ABORTING
    publish.assert_called_once_with(TEST_UUID, TaskStatus.ABORTED.value)
    stats_logger.incr.assert_not_called()


def test_pre_aborted_task_publishes_completion_without_execution() -> None:
    task = task_model(TaskStatus.ABORTING)
    final = task_model(TaskStatus.ABORTED)
    function = MagicMock()

    with (
        patch("superset.tasks.executor.InternalStatusTransitionCommand") as transition,
        patch(
            "superset.tasks.executor.TaskDAO.find_one_or_none",
            return_value=final,
        ),
        patch("superset.tasks.executor.TaskManager.publish_completion") as publish,
    ):
        transition.return_value.run.return_value = True
        result = execute_task_callable(task, "test.task", function, (), {})

    assert result is final
    function.assert_not_called()
    publish.assert_called_once_with(TEST_UUID, TaskStatus.ABORTED.value)
