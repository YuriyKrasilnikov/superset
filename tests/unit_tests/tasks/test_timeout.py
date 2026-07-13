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
"""Unit tests for GTF timeout handling."""

import threading
import time
from unittest.mock import MagicMock, patch
from uuid import UUID

import pytest
from superset_core.tasks.types import TaskOptions, TaskScope, TaskStatus

from superset.tasks.context import TaskContext
from superset.tasks.decorators import TaskWrapper

TEST_UUID = UUID("b8b61b7b-1cd3-4a31-a74a-0a95341afc06")

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def mock_flask_app():
    """Create a properly configured mock Flask app."""
    mock_app = MagicMock()
    mock_app.config = {
        "TASK_ABORT_POLLING_DEFAULT_INTERVAL": 0.1,
    }
    # Make app_context() return a proper context manager
    mock_app.app_context.return_value.__enter__ = MagicMock(return_value=None)
    mock_app.app_context.return_value.__exit__ = MagicMock(return_value=None)
    return mock_app


@pytest.fixture
def mock_task_abortable():
    """Create a mock task that is abortable."""
    task = MagicMock()
    task.uuid = TEST_UUID
    task.status = "in_progress"
    task.properties_dict = {"is_abortable": True}
    task.payload_dict = {}
    # Set real values for dedup_key generation (used by UpdateTaskCommand lock)
    task.scope = "shared"
    task.task_type = "test_task"
    task.task_key = "test_key"
    task.user_id = 1
    return task


@pytest.fixture
def mock_task_not_abortable():
    """Create a mock task that is NOT abortable."""
    task = MagicMock()
    task.uuid = TEST_UUID
    task.status = "in_progress"
    task.properties_dict = {}  # No is_abortable means it's not abortable
    task.payload_dict = {}
    # Set real values for dedup_key generation (used by UpdateTaskCommand lock)
    task.scope = "shared"
    task.task_type = "test_task"
    task.task_key = "test_key"
    task.user_id = 1
    return task


@pytest.fixture
def task_context_for_timeout(mock_flask_app, mock_task_abortable):
    """Create TaskContext with mocked dependencies for timeout tests."""
    # Ensure mock_task has required attributes for TaskContext
    mock_task_abortable.payload_dict = {}

    with (
        patch("superset.tasks.context.current_app") as mock_current_app,
        patch("superset.daos.tasks.TaskDAO") as mock_dao,
        patch("superset.tasks.manager.cache_manager") as mock_cache_manager,
    ):
        # Disable Redis by making distributed_coordination return None
        mock_cache_manager.distributed_coordination = None

        # Configure current_app mock
        mock_current_app.config = mock_flask_app.config
        mock_current_app._get_current_object.return_value = mock_flask_app

        # Configure TaskDAO mock
        mock_dao.find_one_or_none.return_value = mock_task_abortable

        ctx = TaskContext(mock_task_abortable)
        ctx._app = mock_flask_app

        yield ctx

        # Cleanup: stop timers if started
        ctx.stop_timeout_timer()
        if ctx._abort_listener:
            ctx.stop_abort_polling()


# =============================================================================
# TaskWrapper._merge_options Timeout Tests
# =============================================================================


class TestTimeoutMerging:
    """Test timeout merging behavior in TaskWrapper._merge_options."""

    def test_merge_options_decorator_timeout_used_when_no_override(self):
        """Test that decorator timeout is used when no override is provided."""

        def dummy_func():
            pass

        wrapper = TaskWrapper(
            name="test_task",
            func=dummy_func,
            default_options=TaskOptions(),
            scope=TaskScope.PRIVATE,
            default_timeout=300,  # 5-minute default
        )

        merged = wrapper._merge_options(None)
        assert merged.timeout == 300

    def test_merge_options_override_timeout_takes_precedence(self):
        """Test that TaskOptions timeout overrides decorator default."""

        def dummy_func():
            pass

        wrapper = TaskWrapper(
            name="test_task",
            func=dummy_func,
            default_options=TaskOptions(),
            scope=TaskScope.PRIVATE,
            default_timeout=300,  # 5-minute default
        )

        override = TaskOptions(timeout=600)  # 10-minute override
        merged = wrapper._merge_options(override)
        assert merged.timeout == 600

    def test_merge_options_no_timeout_when_not_configured(self):
        """Test that no timeout is set when not configured anywhere."""

        def dummy_func():
            pass

        wrapper = TaskWrapper(
            name="test_task",
            func=dummy_func,
            default_options=TaskOptions(),
            scope=TaskScope.PRIVATE,
            default_timeout=None,  # No default timeout
        )

        merged = wrapper._merge_options(None)
        assert merged.timeout is None

    def test_merge_options_override_with_other_options_preserves_timeout(self):
        """Test that setting other options doesn't lose decorator timeout."""

        def dummy_func():
            pass

        wrapper = TaskWrapper(
            name="test_task",
            func=dummy_func,
            default_options=TaskOptions(),
            scope=TaskScope.PRIVATE,
            default_timeout=300,
        )

        # Override only task_key, not timeout
        override = TaskOptions(task_key="my-key")
        merged = wrapper._merge_options(override)

        # Should keep decorator timeout since override.timeout is None
        assert merged.timeout == 300
        assert merged.task_key == "my-key"


# =============================================================================
# TaskContext Timeout Timer Tests
# =============================================================================


class TestTimeoutTimer:
    """Test TaskContext timeout timer behavior."""

    def test_start_timeout_timer_sets_timer(self, task_context_for_timeout):
        """Test that start_timeout_timer creates a timer."""
        ctx = task_context_for_timeout

        assert ctx._timeout_timer is None

        ctx.start_timeout_timer(10)

        assert ctx._timeout_timer is not None
        assert ctx._timeout_triggered is False

    def test_start_timeout_timer_is_idempotent(self, task_context_for_timeout):
        """Test that starting timer twice doesn't create duplicate timers."""
        ctx = task_context_for_timeout

        ctx.start_timeout_timer(10)
        first_timer = ctx._timeout_timer

        ctx.start_timeout_timer(20)  # Try to start again
        second_timer = ctx._timeout_timer

        assert first_timer is second_timer

    def test_stop_timeout_timer_cancels_timer(self, task_context_for_timeout):
        """Test that stop_timeout_timer cancels the timer."""
        ctx = task_context_for_timeout

        ctx.start_timeout_timer(10)
        assert ctx._timeout_timer is not None

        ctx.stop_timeout_timer()

        assert ctx._timeout_timer is None

    def test_stop_timeout_timer_safe_when_no_timer(self, task_context_for_timeout):
        """Test that stop_timeout_timer doesn't fail when no timer exists."""
        ctx = task_context_for_timeout

        assert ctx._timeout_timer is None
        ctx.stop_timeout_timer()  # Should not raise
        assert ctx._timeout_timer is None

    def test_timeout_triggered_property_initially_false(self, task_context_for_timeout):
        """Test that timeout_triggered is False initially."""
        ctx = task_context_for_timeout
        assert ctx.timeout_triggered is False

    def test_cleanup_stops_timeout_timer(self, task_context_for_timeout):
        """Test that _run_cleanup stops the timeout timer."""
        ctx = task_context_for_timeout

        ctx.start_timeout_timer(10)
        assert ctx._timeout_timer is not None

        ctx._run_cleanup()

        assert ctx._timeout_timer is None


class TestTimeoutTrigger:
    """Test timeout trigger behavior when timer fires."""

    def test_late_timeout_does_not_reopen_completed_execution(
        self,
        task_context_for_timeout: TaskContext,
    ) -> None:
        ctx = task_context_for_timeout
        ctx.mark_execution_completed()
        with patch("superset.tasks.context.threading.Timer") as timer:
            ctx.start_timeout_timer(10)
            timer.call_args.args[1]()

        assert ctx.timeout_triggered is False
        assert ctx.interruption_status is None

    def test_late_abort_does_not_run_handler_after_execution_completed(
        self,
        task_context_for_timeout: TaskContext,
    ) -> None:
        handler = MagicMock()
        task_context_for_timeout._abort_handlers.append(handler)

        task_context_for_timeout.mark_execution_completed()
        task_context_for_timeout._on_abort_detected()

        handler.assert_not_called()
        assert task_context_for_timeout.interruption_status is None

    def test_abort_handler_and_execution_completion_are_serialized(
        self,
        task_context_for_timeout: TaskContext,
    ) -> None:
        handler_started = threading.Event()
        release_handler = threading.Event()
        completion_returned = threading.Event()

        def handler() -> None:
            handler_started.set()
            assert release_handler.wait(timeout=1)

        def complete_execution() -> None:
            task_context_for_timeout.mark_execution_completed()
            completion_returned.set()

        task_context_for_timeout._abort_handlers.append(handler)
        abort_thread = threading.Thread(
            target=task_context_for_timeout._on_abort_detected
        )
        completion_thread = threading.Thread(target=complete_execution)

        abort_thread.start()
        assert handler_started.wait(timeout=1)
        completion_thread.start()
        assert not completion_returned.wait(timeout=0.05)

        release_handler.set()
        abort_thread.join(timeout=1)
        completion_thread.join(timeout=1)

        assert not abort_thread.is_alive()
        assert not completion_thread.is_alive()
        assert completion_returned.is_set()
        assert task_context_for_timeout.interruption_status == TaskStatus.ABORTED

    def test_timeout_cas_loss_does_not_claim_interruption(
        self,
        task_context_for_timeout: TaskContext,
    ) -> None:
        ctx = task_context_for_timeout
        with (
            patch("superset.tasks.context.threading.Timer") as timer,
            patch(
                "superset.commands.tasks.internal_update."
                "InternalStatusTransitionCommand"
            ) as transition,
        ):
            transition.return_value.run.return_value = False
            ctx.start_timeout_timer(10)
            timer.call_args.args[1]()

        transition.assert_called_once()
        assert ctx.timeout_triggered is False
        assert ctx.interruption_status is None

    def test_timeout_triggers_abort_when_abortable(
        self, mock_flask_app, mock_task_abortable
    ):
        """Test that timeout triggers abort handlers when task is abortable."""
        abort_called = False

        with (
            patch("superset.tasks.context.current_app") as mock_current_app,
            patch("superset.daos.tasks.TaskDAO") as mock_dao,
            patch(
                "superset.commands.tasks.internal_update."
                "InternalStatusTransitionCommand"
            ) as mock_transition,
            patch("superset.tasks.manager.cache_manager") as mock_cache_manager,
        ):
            # Disable Redis by making distributed_coordination return None
            mock_cache_manager.distributed_coordination = None

            mock_current_app.config = mock_flask_app.config
            mock_current_app._get_current_object.return_value = mock_flask_app
            mock_dao.find_one_or_none.return_value = mock_task_abortable
            mock_transition.return_value.run.return_value = True

            # Registration updates TaskContext's authoritative property cache via a
            # zero-read SQL write; the originally fetched ORM snapshot stays stale.
            mock_task_abortable.properties_dict = {"is_abortable": False}
            ctx = TaskContext(mock_task_abortable)
            ctx._app = mock_flask_app

            @ctx.on_abort
            def handle_abort():
                nonlocal abort_called
                abort_called = True

            # Start short timeout
            ctx.start_timeout_timer(1)

            # Wait for timeout to fire
            time.sleep(1.5)

            # Abort handler should have been called
            assert abort_called
            assert ctx._timeout_triggered
            assert ctx._abort_detected

            mock_transition.assert_called_once_with(
                task_uuid=TEST_UUID,
                new_status=TaskStatus.ABORTING,
                expected_status=TaskStatus.IN_PROGRESS,
                properties={
                    "is_abortable": True,
                    "error_message": "Task timed out",
                },
            )

            # Cleanup
            ctx.stop_timeout_timer()
            if ctx._abort_listener:
                ctx.stop_abort_polling()

    def test_timeout_logs_warning_when_not_abortable(
        self, mock_flask_app, mock_task_not_abortable
    ):
        """Test that timeout logs warning when task has no abort handler."""
        with (
            patch("superset.tasks.context.current_app") as mock_current_app,
            patch("superset.daos.tasks.TaskDAO") as mock_dao,
            patch("superset.tasks.context.logger") as mock_logger,
            patch("superset.tasks.manager.cache_manager") as mock_cache_manager,
        ):
            # Disable Redis by making distributed_coordination return None
            mock_cache_manager.distributed_coordination = None

            mock_current_app.config = mock_flask_app.config
            mock_current_app._get_current_object.return_value = mock_flask_app
            mock_dao.find_one_or_none.return_value = mock_task_not_abortable

            ctx = TaskContext(mock_task_not_abortable)
            ctx._app = mock_flask_app

            # No abort handler registered

            # Start short timeout
            ctx.start_timeout_timer(1)

            # Wait for timeout to fire
            time.sleep(1.5)

            # Should have logged warning
            mock_logger.warning.assert_called()
            warning_call = mock_logger.warning.call_args
            assert "no abort handler" in warning_call[0][0].lower()
            assert ctx._timeout_triggered
            assert not ctx._abort_detected  # No abort since no handler

            # Cleanup
            ctx.stop_timeout_timer()

    def test_timeout_does_not_trigger_if_already_aborting(
        self, mock_flask_app, mock_task_abortable
    ):
        """Test that timeout doesn't re-trigger abort if already aborting."""
        abort_count = 0

        with (
            patch("superset.tasks.context.current_app") as mock_current_app,
            patch("superset.daos.tasks.TaskDAO") as mock_dao,
            patch(
                "superset.commands.tasks.internal_update."
                "InternalStatusTransitionCommand"
            ),
            patch("superset.tasks.manager.cache_manager") as mock_cache_manager,
        ):
            # Disable Redis by making distributed_coordination return None
            mock_cache_manager.distributed_coordination = None

            mock_current_app.config = mock_flask_app.config
            mock_current_app._get_current_object.return_value = mock_flask_app
            mock_dao.find_one_or_none.return_value = mock_task_abortable

            ctx = TaskContext(mock_task_abortable)
            ctx._app = mock_flask_app

            @ctx.on_abort
            def handle_abort():
                nonlocal abort_count
                abort_count += 1

            # Pre-set abort detected
            ctx._abort_detected = True

            # Start short timeout
            ctx.start_timeout_timer(1)

            # Wait for timeout to fire
            time.sleep(1.5)

            # Handler should NOT have been called since already aborting
            assert abort_count == 0

            # Cleanup
            ctx.stop_timeout_timer()
            if ctx._abort_listener:
                ctx.stop_abort_polling()


# =============================================================================
# Task Decorator Timeout Tests
# =============================================================================


class TestTaskDecoratorTimeout:
    """Test @task decorator timeout parameter."""

    def test_task_decorator_accepts_timeout(self):
        """Test that @task decorator accepts timeout parameter."""
        from superset.tasks.decorators import task
        from superset.tasks.registry import TaskRegistry

        @task(name="test_timeout_task_1", timeout=300)
        def timeout_test_task_1():
            pass

        assert isinstance(timeout_test_task_1, TaskWrapper)
        assert timeout_test_task_1.default_timeout == 300

        # Cleanup registry
        TaskRegistry._tasks.pop("test_timeout_task_1", None)

    def test_task_decorator_without_timeout(self):
        """Test that @task decorator works without timeout."""
        from superset.tasks.decorators import task
        from superset.tasks.registry import TaskRegistry

        @task(name="test_timeout_task_2")
        def timeout_test_task_2():
            pass

        assert isinstance(timeout_test_task_2, TaskWrapper)
        assert timeout_test_task_2.default_timeout is None

        # Cleanup registry
        TaskRegistry._tasks.pop("test_timeout_task_2", None)

    def test_task_decorator_with_all_params(self):
        """Test that @task decorator accepts all parameters together."""
        from superset.tasks.decorators import task
        from superset.tasks.registry import TaskRegistry

        @task(name="test_timeout_task_3", scope=TaskScope.SHARED, timeout=600)
        def timeout_test_task_3():
            pass

        assert timeout_test_task_3.name == "test_timeout_task_3"
        assert timeout_test_task_3.scope == TaskScope.SHARED
        assert timeout_test_task_3.default_timeout == 600

        # Cleanup registry
        TaskRegistry._tasks.pop("test_timeout_task_3", None)


# =============================================================================
# Timeout Terminal State Tests
# =============================================================================


@pytest.mark.parametrize(
    ("timeout_triggered", "abort_detected", "handlers_completed", "expected"),
    [
        (False, False, False, None),
        (True, False, False, TaskStatus.TIMED_OUT),
        (False, True, True, TaskStatus.ABORTED),
        (True, True, True, TaskStatus.TIMED_OUT),
        (False, True, False, TaskStatus.FAILURE),
        (True, True, False, TaskStatus.FAILURE),
    ],
)
def test_interruption_status_matrix(
    task_context_for_timeout: TaskContext,
    timeout_triggered: bool,
    abort_detected: bool,
    handlers_completed: bool,
    expected: TaskStatus | None,
) -> None:
    task_context_for_timeout._timeout_triggered = timeout_triggered
    task_context_for_timeout._abort_detected = abort_detected
    task_context_for_timeout._abort_handlers_completed = handlers_completed

    assert task_context_for_timeout.interruption_status == expected


@pytest.mark.parametrize(
    ("execution_failed", "handler_failed", "interruption", "expected"),
    [
        (False, False, None, TaskStatus.SUCCESS),
        (True, False, None, TaskStatus.FAILURE),
        (False, True, None, TaskStatus.FAILURE),
        (True, True, TaskStatus.TIMED_OUT, TaskStatus.FAILURE),
        (False, False, TaskStatus.TIMED_OUT, TaskStatus.TIMED_OUT),
        (False, False, TaskStatus.ABORTED, TaskStatus.ABORTED),
    ],
)
def test_terminal_status_precedence(
    task_context_for_timeout: TaskContext,
    execution_failed: bool,
    handler_failed: bool,
    interruption: TaskStatus | None,
    expected: TaskStatus,
) -> None:
    task_context_for_timeout._execution_failed = execution_failed
    task_context_for_timeout._handler_failure_detected = handler_failed

    if interruption == TaskStatus.TIMED_OUT:
        task_context_for_timeout._timeout_triggered = True
    elif interruption == TaskStatus.ABORTED:
        task_context_for_timeout._abort_detected = True
        task_context_for_timeout._abort_handlers_completed = True

    assert task_context_for_timeout.terminal_status == expected


class TestTimeoutTerminalState:
    """Test timeout transitions to correct terminal state (TIMED_OUT vs FAILURE)."""

    def test_timeout_triggered_flag_set_on_timeout(
        self, mock_flask_app, mock_task_abortable
    ):
        """Test that timeout_triggered flag is set when timeout fires."""
        with (
            patch("superset.tasks.context.current_app") as mock_current_app,
            patch("superset.daos.tasks.TaskDAO") as mock_dao,
            patch(
                "superset.commands.tasks.internal_update."
                "InternalStatusTransitionCommand"
            ),
            patch("superset.tasks.manager.cache_manager") as mock_cache_manager,
        ):
            # Disable Redis by making distributed_coordination return None
            mock_cache_manager.distributed_coordination = None

            mock_current_app.config = mock_flask_app.config
            mock_current_app._get_current_object.return_value = mock_flask_app
            mock_dao.find_one_or_none.return_value = mock_task_abortable

            ctx = TaskContext(mock_task_abortable)
            ctx._app = mock_flask_app

            @ctx.on_abort
            def handle_abort():
                pass

            # Initially not triggered
            assert ctx.timeout_triggered is False

            # Start short timeout
            ctx.start_timeout_timer(1)

            # Wait for timeout to fire
            time.sleep(1.5)

            # Should be set after timeout
            assert ctx.timeout_triggered is True

            # Cleanup
            ctx.stop_timeout_timer()
            if ctx._abort_listener:
                ctx.stop_abort_polling()

    def test_user_abort_does_not_set_timeout_triggered(
        self, mock_flask_app, mock_task_abortable
    ):
        """Test that user abort doesn't set timeout_triggered flag."""
        with (
            patch("superset.tasks.context.current_app") as mock_current_app,
            patch("superset.daos.tasks.TaskDAO") as mock_dao,
            patch(
                "superset.commands.tasks.internal_update."
                "InternalStatusTransitionCommand"
            ),
            patch("superset.tasks.manager.cache_manager") as mock_cache_manager,
        ):
            # Disable Redis by making distributed_coordination return None
            mock_cache_manager.distributed_coordination = None

            mock_current_app.config = mock_flask_app.config
            mock_current_app._get_current_object.return_value = mock_flask_app
            mock_dao.find_one_or_none.return_value = mock_task_abortable

            ctx = TaskContext(mock_task_abortable)
            ctx._app = mock_flask_app

            @ctx.on_abort
            def handle_abort():
                pass

            # Simulate user abort (not timeout)
            ctx._on_abort_detected()

            # timeout_triggered should still be False
            assert ctx.timeout_triggered is False
            # But abort_detected should be True
            assert ctx._abort_detected is True

            # Cleanup
            if ctx._abort_listener:
                ctx.stop_abort_polling()

    def test_abort_handlers_completed_tracks_success(
        self, mock_flask_app, mock_task_abortable
    ):
        """Test that abort_handlers_completed flag tracks successful
        handler execution."""
        with (
            patch("superset.tasks.context.current_app") as mock_current_app,
            patch("superset.daos.tasks.TaskDAO") as mock_dao,
            patch(
                "superset.commands.tasks.internal_update."
                "InternalStatusTransitionCommand"
            ),
            patch("superset.tasks.manager.cache_manager") as mock_cache_manager,
        ):
            # Disable Redis by making distributed_coordination return None
            mock_cache_manager.distributed_coordination = None

            mock_current_app.config = mock_flask_app.config
            mock_current_app._get_current_object.return_value = mock_flask_app
            mock_dao.find_one_or_none.return_value = mock_task_abortable

            ctx = TaskContext(mock_task_abortable)
            ctx._app = mock_flask_app

            @ctx.on_abort
            def handle_abort():
                pass  # Successful handler

            # Initially not completed
            assert ctx.abort_handlers_completed is False

            # Trigger abort handlers
            ctx._trigger_abort_handlers()

            # Should be marked as completed
            assert ctx.abort_handlers_completed is True

            # Cleanup
            if ctx._abort_listener:
                ctx.stop_abort_polling()

    def test_abort_handlers_completed_false_on_exception(
        self, mock_flask_app, mock_task_abortable
    ):
        """Test that abort_handlers_completed is False when handler throws."""
        with (
            patch("superset.tasks.context.current_app") as mock_current_app,
            patch("superset.daos.tasks.TaskDAO") as mock_dao,
            patch(
                "superset.commands.tasks.internal_update."
                "InternalStatusTransitionCommand"
            ),
            patch("superset.tasks.manager.cache_manager") as mock_cache_manager,
        ):
            # Disable Redis by making distributed_coordination return None
            mock_cache_manager.distributed_coordination = None

            mock_current_app.config = mock_flask_app.config
            mock_current_app._get_current_object.return_value = mock_flask_app
            mock_dao.find_one_or_none.return_value = mock_task_abortable

            ctx = TaskContext(mock_task_abortable)
            ctx._app = mock_flask_app

            @ctx.on_abort
            def handle_abort():
                raise ValueError("Handler failed")

            # Initially not completed
            assert ctx.abort_handlers_completed is False

            # Trigger abort handlers (will catch the exception internally)
            ctx._trigger_abort_handlers()

            # Should NOT be marked as completed since handler threw
            assert ctx.abort_handlers_completed is False

            # Cleanup
            if ctx._abort_listener:
                ctx.stop_abort_polling()
