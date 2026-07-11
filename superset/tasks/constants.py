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
"""Constants for the Global Task Framework (GTF)."""

from superset_core.tasks.types import TaskStatus

# Terminal states: Task execution has ended and dedup_key slot is freed
TERMINAL_STATES: frozenset[str] = frozenset(
    {
        TaskStatus.SUCCESS.value,
        TaskStatus.FAILURE.value,
        TaskStatus.ABORTED.value,
        TaskStatus.TIMED_OUT.value,
    }
)

# Active states: Task is still in progress and dedup_key is reserved
ACTIVE_STATES: frozenset[str] = frozenset(
    {
        TaskStatus.PENDING.value,
        TaskStatus.IN_PROGRESS.value,
        TaskStatus.FINALIZING.value,
        TaskStatus.ABORTING.value,
    }
)

ALLOWED_STATUS_TRANSITIONS: dict[TaskStatus, frozenset[TaskStatus]] = {
    TaskStatus.PENDING: frozenset(
        {TaskStatus.IN_PROGRESS, TaskStatus.ABORTED, TaskStatus.FAILURE}
    ),
    TaskStatus.IN_PROGRESS: frozenset({TaskStatus.FINALIZING, TaskStatus.ABORTING}),
    TaskStatus.FINALIZING: frozenset(
        {TaskStatus.SUCCESS, TaskStatus.FAILURE, TaskStatus.TIMED_OUT}
    ),
    TaskStatus.ABORTING: frozenset(
        {TaskStatus.ABORTED, TaskStatus.FAILURE, TaskStatus.TIMED_OUT}
    ),
    TaskStatus.SUCCESS: frozenset(),
    TaskStatus.FAILURE: frozenset(),
    TaskStatus.ABORTED: frozenset(),
    TaskStatus.TIMED_OUT: frozenset(),
}


def is_allowed_status_transition(
    current: TaskStatus,
    target: TaskStatus,
) -> bool:
    """Return whether the centralized GTF state graph permits an edge."""
    return target in ALLOWED_STATUS_TRANSITIONS[current]


def ensure_allowed_status_transition(
    current: TaskStatus,
    target: TaskStatus,
) -> None:
    """Raise when a production command attempts an edge outside the GTF graph."""
    if not is_allowed_status_transition(current, target):
        raise ValueError(
            f"Invalid GTF status transition from {current.value} to {target.value}"
        )


# Abortable states: Task can be aborted (for pending or abortable in-progress)
ABORTABLE_STATES: frozenset[str] = frozenset(
    {
        TaskStatus.PENDING.value,
        TaskStatus.IN_PROGRESS.value,
    }
)

# Abort-related states: Task is being or has been aborted
ABORT_STATES: frozenset[str] = frozenset(
    {
        TaskStatus.ABORTING.value,
        TaskStatus.ABORTED.value,
    }
)
