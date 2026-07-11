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
"""Single lifecycle runner shared by inline and asynchronous GTF execution."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any, TYPE_CHECKING

from flask import current_app
from superset_core.tasks.types import TaskStatus

from superset.commands.tasks.internal_update import InternalStatusTransitionCommand
from superset.daos.tasks import TaskDAO
from superset.stats_logger import BaseStatsLogger
from superset.tasks.ambient_context import use_context
from superset.tasks.constants import ABORT_STATES, TERMINAL_STATES
from superset.tasks.context import TaskContext
from superset.tasks.manager import TaskManager

if TYPE_CHECKING:
    from superset.models.tasks import Task

logger = logging.getLogger(__name__)


def _claim_finalization(task: Task, context: TaskContext) -> bool:
    """Fence result publication against a concurrent cancellation."""
    claimed = InternalStatusTransitionCommand(
        task_uuid=task.uuid,
        new_status=TaskStatus.FINALIZING,
        expected_status=TaskStatus.IN_PROGRESS,
    ).run()
    current = TaskDAO.find_one_or_none(uuid=task.uuid)
    if claimed:
        task.status = TaskStatus.FINALIZING.value
    elif current and current.status == TaskStatus.ABORTING.value:
        context.detect_abort_before_finalization()
    return claimed


def _publish_terminal_status(
    task: Task,
    task_name: str,
    context: TaskContext,
    claimed_finalization: bool,
) -> Task:
    """Publish one terminal CAS and notify task waiters."""
    terminal_status = context.terminal_status
    properties = context.properties_snapshot()
    if terminal_status == TaskStatus.FAILURE and not properties.get("error_message"):
        properties["error_message"] = "Task lifecycle handlers did not complete"

    expected = TaskStatus.FINALIZING if claimed_finalization else TaskStatus.ABORTING
    transitioned = InternalStatusTransitionCommand(
        task_uuid=task.uuid,
        new_status=terminal_status,
        expected_status=expected,
        properties=properties,
        set_ended_at=True,
    ).run()
    logger.info(
        "Task %s (uuid=%s) %s with status %s",
        task_name,
        task.uuid,
        "completed" if transitioned else "was already resolved",
        terminal_status.value,
    )
    if transitioned and terminal_status in (TaskStatus.SUCCESS, TaskStatus.FAILURE):
        stats_logger: BaseStatsLogger = current_app.config["STATS_LOGGER"]
        stats_logger.incr(f"gtf.task.{terminal_status.value}")

    final_task = TaskDAO.find_one_or_none(uuid=task.uuid) or task
    if final_task.status in TERMINAL_STATES:
        TaskManager.publish_completion(task.uuid, final_task.status)
    return final_task


def execute_task_callable(
    task: Task,
    task_name: str,
    function: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    timeout: int | None = None,
) -> Task:
    """Execute one task through the authoritative GTF state machine."""
    task_uuid = task.uuid
    if task.status in ABORT_STATES:
        InternalStatusTransitionCommand(
            task_uuid=task_uuid,
            new_status=TaskStatus.ABORTED,
            expected_status=[TaskStatus.PENDING, TaskStatus.ABORTING],
            set_ended_at=True,
        ).run()
        final_task = TaskDAO.find_one_or_none(uuid=task_uuid) or task
        if final_task.status in TERMINAL_STATES:
            TaskManager.publish_completion(task_uuid, final_task.status)
        return final_task

    if not InternalStatusTransitionCommand(
        task_uuid=task_uuid,
        new_status=TaskStatus.IN_PROGRESS,
        expected_status=TaskStatus.PENDING,
        set_started_at=True,
    ).run():
        logger.info(
            "Task %s (uuid=%s) was claimed or cancelled concurrently",
            task_name,
            task_uuid,
        )
        return TaskDAO.find_one_or_none(uuid=task_uuid) or task

    task.status = TaskStatus.IN_PROGRESS.value
    context = TaskContext(task)
    if timeout:
        context.start_timeout_timer(timeout)

    try:
        with use_context(context):
            function(*args, **kwargs)
    except Exception as ex:  # pylint: disable=broad-except
        context.capture_execution_failure(ex)
        logger.error(
            "Execution of task %s (uuid=%s) failed: %s",
            task_name,
            task_uuid,
            str(ex),
            exc_info=True,
        )

    claimed_finalization = _claim_finalization(task, context)
    context.mark_execution_completed()
    context._run_cleanup(  # pylint: disable=protected-access
        run_finalizers=claimed_finalization and context.can_finalize
    )

    return _publish_terminal_status(
        task,
        task_name,
        context,
        claimed_finalization,
    )
