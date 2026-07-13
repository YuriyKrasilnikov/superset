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
"""Tests for internal GTF update command responsibility boundaries."""

from uuid import UUID

import pytest
from superset_core.tasks.types import TaskStatus

from superset.commands.tasks.internal_update import (
    InternalStatusTransitionCommand,
    InternalUpdateTaskCommand,
)
from superset.tasks.constants import ALLOWED_STATUS_TRANSITIONS

TASK_UUID = UUID("b8b61b7b-1cd3-4a31-a74a-0a95341afc06")


def test_payload_update_has_no_status_transition_fields() -> None:
    command = InternalUpdateTaskCommand(TASK_UUID, payload={"result": "ready"})

    command.validate()


@pytest.mark.parametrize(
    ("source", "target"),
    [
        (source, target)
        for source, targets in ALLOWED_STATUS_TRANSITIONS.items()
        for target in targets
    ],
)
def test_status_transition_accepts_every_graph_edge(
    source: TaskStatus,
    target: TaskStatus,
) -> None:
    command = InternalStatusTransitionCommand(TASK_UUID, target, source)

    command.validate()


@pytest.mark.parametrize(
    ("source", "target"),
    [
        (TaskStatus.IN_PROGRESS, TaskStatus.SUCCESS),
        (TaskStatus.IN_PROGRESS, TaskStatus.FAILURE),
        (TaskStatus.FINALIZING, TaskStatus.ABORTING),
        (TaskStatus.SUCCESS, TaskStatus.FAILURE),
    ],
)
def test_status_transition_rejects_edges_outside_graph(
    source: TaskStatus,
    target: TaskStatus,
) -> None:
    command = InternalStatusTransitionCommand(TASK_UUID, target, source)

    with pytest.raises(ValueError, match="Invalid GTF status transition"):
        command.validate()
