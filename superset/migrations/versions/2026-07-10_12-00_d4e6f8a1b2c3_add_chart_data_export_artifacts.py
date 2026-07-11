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
"""Add chart-data export artifact metadata.

Revision ID: d4e6f8a1b2c3
Revises: 8f3a1b2c4d5e
Create Date: 2026-07-10 12:00:00.000000
"""

from sqlalchemy import BigInteger, Column, DateTime, Integer, String, UniqueConstraint
from sqlalchemy_utils import UUIDType

from superset.migrations.shared.utils import (
    create_fks_for_table,
    create_index,
    create_table,
    drop_fks_for_table,
    drop_index,
    drop_table,
)

revision = "d4e6f8a1b2c3"
down_revision = "8f3a1b2c4d5e"

TABLE = "chart_data_export_artifacts"


def upgrade() -> None:
    create_table(
        TABLE,
        Column("id", Integer, primary_key=True),
        Column("uuid", UUIDType(binary=True), nullable=False),
        Column("task_id", Integer, nullable=False),
        Column("owner_id", Integer, nullable=True),
        Column("datasource_id", String(64), nullable=False),
        Column("datasource_type", String(32), nullable=False),
        Column("storage_key", String(36), nullable=False),
        Column("filename", String(255), nullable=False),
        Column("content_type", String(128), nullable=False),
        Column("size_bytes", BigInteger, nullable=False),
        Column("sha256", String(64), nullable=False),
        Column("expires_at", DateTime, nullable=False),
        Column("created_on", DateTime, nullable=True),
        Column("changed_on", DateTime, nullable=True),
        Column("created_by_fk", Integer, nullable=True),
        Column("changed_by_fk", Integer, nullable=True),
        UniqueConstraint("uuid", name="uq_chart_data_export_artifacts_uuid"),
        UniqueConstraint("task_id", name="uq_chart_data_export_artifacts_task_id"),
        UniqueConstraint(
            "storage_key", name="uq_chart_data_export_artifacts_storage_key"
        ),
    )
    create_index(TABLE, "ix_chart_data_export_artifacts_task_id", ["task_id"])
    create_index(TABLE, "ix_chart_data_export_artifacts_owner_id", ["owner_id"])
    create_index(TABLE, "ix_chart_data_export_artifacts_expires_at", ["expires_at"])
    create_fks_for_table(
        "fk_chart_data_export_artifacts_task_id_tasks",
        TABLE,
        "tasks",
        ["task_id"],
        ["id"],
        ondelete="CASCADE",
    )
    create_fks_for_table(
        "fk_chart_data_export_artifacts_owner_id_ab_user",
        TABLE,
        "ab_user",
        ["owner_id"],
        ["id"],
        ondelete="SET NULL",
    )
    for column in ("created_by_fk", "changed_by_fk"):
        create_fks_for_table(
            f"fk_chart_data_export_artifacts_{column}_ab_user",
            TABLE,
            "ab_user",
            [column],
            ["id"],
            ondelete="SET NULL",
        )


def downgrade() -> None:
    drop_fks_for_table(
        TABLE,
        [
            "fk_chart_data_export_artifacts_task_id_tasks",
            "fk_chart_data_export_artifacts_owner_id_ab_user",
            "fk_chart_data_export_artifacts_created_by_fk_ab_user",
            "fk_chart_data_export_artifacts_changed_by_fk_ab_user",
        ],
    )
    drop_index(TABLE, "ix_chart_data_export_artifacts_task_id")
    drop_index(TABLE, "ix_chart_data_export_artifacts_owner_id")
    drop_index(TABLE, "ix_chart_data_export_artifacts_expires_at")
    drop_table(TABLE)
