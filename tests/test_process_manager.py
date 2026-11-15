"""Synthetic-only subprocess/IPC tests; no HTTP, database, model or network service."""

import asyncio
import ast
from dataclasses import replace
import json
import multiprocessing
from pathlib import Path
import socket
import threading
from contextlib import asynccontextmanager
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from mooby.api.deadline_policy import Deadline, DeadlinePolicy
from mooby.api.process_manager import ProcessManager, Outcome, JobType, SyntheticJob
from mooby.api import process_protocol as wire
from mooby.api import process_manager as pm


class ControlledProcess:
    """Owned lifecycle double. Blocking gates affect only the supervisor thread."""

    def __init__(self, *, target, args, daemon, block_start=False, block_terminate=False,
                 refuse_exit=False, require_kill=False):
        self.peer = args[0].dup()
        self.start_entered, self.terminate_entered = threading.Event(), threading.Event()
        self.start_gate, self.terminate_gate, self.exited = threading.Event(), threading.Event(), threading.Event()
        if not block_start:
            self.start_gate.set()
        if not block_terminate:
            self.terminate_gate.set()
        self.refuse_exit = refuse_exit
        self.require_kill = require_kill
        self.joins = self.closes = self.kills = 0

    def start(self):
        self.start_entered.set()
        if not self.start_gate.wait(10):
            raise TimeoutError()

    @property
    def exitcode(self):
        return 0 if self.exited.is_set() else None

    def terminate(self):
        self.terminate_entered.set()
        if not self.terminate_gate.wait(10):
            raise TimeoutError()
        if not self.refuse_exit and not self.require_kill:
            self.exited.set()

    def kill(self):
        self.kills += 1
        if not self.refuse_exit:
            self.exited.set()

    def join(self, timeout):
        assert timeout == 0
        assert self.exited.is_set()
        self.joins += 1

    def close(self):
        self.closes += 1
        self.peer.close()

    def unblock(self):
        self.start_gate.set()
        self.terminate_gate.set()
        self.exited.set()


class JoinGateProcess(ControlledProcess):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.join_entered, self.join_gate = threading.Event(), threading.Event()

    def join(self, timeout):
        self.join_entered.set()
        if not self.join_gate.wait(5):
            raise TimeoutError()
        super().join(timeout)

    def unblock(self):
        self.join_gate.set()
        super().unblock()


class ClosedAwareProcess(ControlledProcess):
    @property
    def exitcode(self):
        if self.closes:
            raise ValueError("process object is closed")
        return super().exitcode


class ProcessManagerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.managers = []
        self.children_before = {child.pid for child in multiprocessing.active_children()}
        self.threads_before = set(threading.enumerate())
        self.tasks_before = asyncio.all_tasks()
        self.doubles = []

    async def asyncTearDown(self):
        owned_processes, owned_threads, owned_tasks = {}, set(), set()
        problems = []
        original_failure = None

        def collect_owned():
            for manager in self.managers:
                for slot in tuple(manager._slots.values()):
                    if slot.process is not None:
                        owned_processes[id(slot.process)] = slot.process
                    if slot.thread is not None:
                        owned_threads.add(slot.thread)
                    if slot.task is not None:
                        owned_tasks.add(slot.task)

        try:
            collect_owned()
            for double in self.doubles:
                double.unblock()
            for manager in self.managers:
                report = await asyncio.wait_for(manager.shutdown(), 5)
                if not report.complete:
                    report = await asyncio.wait_for(manager.reap_quarantined(), 5)
                self.assertTrue(report.complete, manager.snapshot())
        except BaseException as error:
            original_failure = error
        finally:
            # Always inspect/fallback even if shutdown/assertion raised. Only
            # manager-owned handles are eligible, not arbitrary new children.
            collect_owned()
            try:
                extra = [child for child in multiprocessing.active_children()
                         if id(child) in owned_processes and child.pid not in self.children_before]
                if extra:
                    problems.append("owned child required fallback cleanup")
                for child in extra:
                    try:
                        child.kill()
                        # Do not race a live supervisor's reaping/handle close.
                        if not any(slot.process is child and slot.thread is not None and slot.thread.is_alive()
                                   for manager in self.managers for slot in manager._slots.values()):
                            child.join(timeout=2)
                    except Exception:
                        problems.append("owned-child fallback failed")
            except Exception:
                problems.append("owned-child inspection failed")
            unexpected = asyncio.all_tasks() - self.tasks_before - {asyncio.current_task()} - owned_tasks
            if unexpected:
                problems.append("test tasks required fallback cleanup")
            pending = {task for task in owned_tasks | unexpected if not task.done()}
            for task in pending:
                task.cancel()
            if pending:
                _, pending = await asyncio.wait(pending, timeout=2)
                if pending:
                    problems.append("pending owned supervision tasks")
            # Yield to terminal publication/thread exit; never join a thread
            # indefinitely or block its event-loop callback while waiting.
            end = Deadline.after(2)
            while any(thread.is_alive() for thread in owned_threads) and not end.expired():
                await asyncio.sleep(.005)
            try:
                if any(id(child) in owned_processes for child in multiprocessing.active_children()):
                    problems.append("live owned child processes")
                if any(thread.is_alive() for thread in owned_threads):
                    problems.append("live owned supervisory threads")
                if asyncio.all_tasks() - self.tasks_before - {asyncio.current_task()}:
                    problems.append("pending test tasks")
            except Exception:
                problems.append("final resource inspection failed")
        if original_failure is not None:
            if problems:
                original_failure.add_note("Cleanup unresolved: " + "; ".join(problems))
            raise original_failure
        self.assertFalse(problems, "; ".join(problems))

    def manager(self, **changes):
        manager = ProcessManager(replace(DeadlinePolicy(), **changes))
        self.managers.append(manager)
        return manager

    async def execute(self, manager, kind, *, text="synthetic", delay_ms=0, seconds=5):
        return await asyncio.wait_for(manager.execute(SyntheticJob(kind, text, delay_ms),
                                                       Deadline.after(seconds)), 8)

    async def until(self, predicate):
        async with asyncio.timeout(5):
            while not predicate():
                await asyncio.sleep(.005)  # Poll observable thread/process state, not guessed delay.

    def double_factory(self, **settings):
        def create(**kwargs):
            double = ControlledProcess(**kwargs, **settings)
            self.doubles.append(double)
            return double
        return create

    async def delayed_success(self, final_clock, *, advance_at_delivery=False):
        manager = self.manager(max_active_jobs=1)
        now = [100.0]
        parent = Deadline(101.0, lambda: now[0])

        def factory(**kwargs):
            process = JoinGateProcess(**kwargs)
            self.doubles.append(process)
            return process

        original_drive = manager._drive

        async def delivery_gap(slot):
            result = await original_drive(slot)
            now[0] = 102.0
            return result

        with patch.object(manager._context, "Process", side_effect=factory), \
             patch.object(manager, "_drive", side_effect=delivery_gap if advance_at_delivery else original_drive):
            task = asyncio.create_task(manager.execute(SyntheticJob(JobType.SUCCESS), parent))
            await self.until(lambda: self.doubles and manager.snapshot().running == 1)
            process = self.doubles[-1]
            process.peer.setblocking(False)
            request = await wire.receive_async(process.peer, wire.MAX_REQUEST_BYTES)
            await asyncio.get_running_loop().sock_sendall(process.peer, wire.response(request, text="synthetic-ok"))
            process.exited.set()
            await self.until(lambda: process.join_entered.is_set()
                             and next(iter(manager._slots.values())).phase.value == "completed")
            now[0] = final_clock
            process.join_gate.set()
            result = await asyncio.wait_for(task, 5)
        self.assertTrue(result.reaped)
        self.assertEqual((manager.snapshot().active_process_slots, manager.snapshot().supervisory_threads), (0, 0))
        self.assertEqual((process.joins, process.closes), (1, 1))
        return result

    async def test_final_deadline_after_delayed_reaping(self):
        result = await self.delayed_success(102.0)
        self.assertEqual(result.outcome, Outcome.DEADLINE_EXPIRED)
        self.assertIsNone(result.output)

    async def test_final_deadline_at_exact_expiry_after_reaping(self):
        result = await self.delayed_success(101.0)
        self.assertEqual(result.outcome, Outcome.DEADLINE_EXPIRED)
        self.assertIsNone(result.output)

    async def test_final_success_before_deadline_after_reaping(self):
        result = await self.delayed_success(100.5)
        self.assertEqual(result.outcome, Outcome.SUCCESS)
        self.assertEqual(result.output, "synthetic-ok")

    async def test_deadline_rechecked_at_public_delivery_after_drive_completed(self):
        result = await self.delayed_success(100.5, advance_at_delivery=True)
        self.assertEqual(result.outcome, Outcome.DEADLINE_EXPIRED)
        self.assertIsNone(result.output)

    @asynccontextmanager
    async def held_reaped_report(self):
        manager = self.manager(max_active_jobs=1)
        publish = manager._finished
        held = []
        jobs = []

        def factory(**kwargs):
            process = ClosedAwareProcess(**kwargs)
            self.doubles.append(process)
            return process

        def delayed_publication(slot, report, *attempt):
            if report.reaped and not held:
                held.append((slot, report, *attempt))
            else:
                publish(slot, report, *attempt)

        with patch.object(manager._context, "Process", side_effect=factory) as factory_mock, \
             patch.object(manager, "_finished", side_effect=delayed_publication):
            result = await self.execute(manager, JobType.SUCCESS, seconds=.05)
            self.assertEqual(result.outcome, Outcome.RETIREMENT_INCOMPLETE)
            await self.until(lambda: manager.snapshot().supervisory_threads == 0)
            self.assertTrue(held)
            slot = held[0][0]
            fixture = SimpleNamespace(manager=manager, process=self.doubles[-1], slot=slot,
                                      report_args=held[0], publish=publish, jobs=jobs,
                                      factory=factory_mock)
            try:
                yield fixture
            finally:
                for job in jobs:
                    if not job.done():
                        job.cancel()
                if jobs:
                    await asyncio.wait_for(asyncio.gather(*jobs, return_exceptions=True), 5)
                await self.until(lambda: manager.snapshot().supervisory_threads == 0)
                # Publish the actual successful report, not a fabricated recovery.
                publish(*held[0])
                await manager.reap_quarantined()

    async def test_delayed_report_publication_never_retries_closed_handle(self):
        async with self.held_reaped_report() as f:
            started = asyncio.Event()
            thread_start = threading.Thread.start

            async def collect():
                started.set()
                return await f.manager.reap_quarantined()

            with patch.object(threading.Thread, "start", autospec=True, side_effect=thread_start) as start:
                task = asyncio.create_task(collect())
                f.jobs.append(task)
                await started.wait()
                self.assertEqual(start.call_count, 0)
                self.assertEqual(f.manager.snapshot().active_process_slots, 1)
                self.assertFalse(f.slot.finished.done())
                f.publish(*f.report_args)
                self.assertTrue((await task).complete)
            self.assertEqual((f.process.joins, f.process.closes, f.factory.call_count), (1, 1, 1))
            # Duplicate failure cannot regress terminal successful publication.
            f.publish(f.slot, pm._Report(False), *f.report_args[2:])
            self.assertTrue(f.slot.report.reaped)
            self.assertTrue((await f.manager.reap_quarantined()).complete)
            self.assertEqual(f.manager.snapshot().reaped_count, 1)
        self.assertEqual((await self.execute(f.manager, JobType.SUCCESS)).outcome, Outcome.SUCCESS)

    async def test_concurrent_collectors_wait_for_same_delayed_report(self):
        async with self.held_reaped_report() as f:
            entered = [asyncio.Event(), asyncio.Event()]

            async def collect(index):
                entered[index].set()
                return await f.manager.reap_quarantined()

            f.jobs.extend(asyncio.create_task(collect(index)) for index in range(2))
            for event in entered:
                await event.wait()
            self.assertEqual(f.manager.snapshot().supervisory_threads, 0)
            self.assertFalse(f.slot.finished.done())
            f.publish(*f.report_args)
            self.assertTrue(all(report.complete for report in await asyncio.gather(*f.jobs)))
            self.assertEqual((f.process.joins, f.process.closes), (1, 1))

    async def test_shutdown_during_pending_report_remains_incomplete(self):
        async with self.held_reaped_report() as f:
            self.assertFalse((await f.manager.shutdown()).complete)
            self.assertEqual(f.manager.snapshot().active_process_slots, 1)
            f.publish(*f.report_args)
            self.assertTrue((await f.manager.reap_quarantined()).complete)
            self.assertTrue(f.manager.snapshot().closed)

    async def test_unpublished_report_timeout_retains_unknown_slot_without_retry(self):
        async with self.held_reaped_report() as f:
            attempt = f.slot.attempt
            report = await asyncio.wait_for(f.manager.reap_quarantined(), 3)
            self.assertFalse(report.complete)
            self.assertIs(f.slot.attempt, attempt)
            self.assertFalse(f.slot.finished.done())
            self.assertEqual((f.manager.snapshot().active_process_slots,
                              f.manager.snapshot().supervisory_threads), (1, 0))
            self.assertEqual((f.process.joins, f.process.closes), (1, 1))

    async def test_retry_thread_start_failure_publishes_failure_for_later_retry(self):
        manager = self.manager(max_active_jobs=1)
        with patch.object(manager._context, "Process", side_effect=self.double_factory(refuse_exit=True)) as factory:
            self.assertEqual((await self.execute(manager, JobType.SUCCESS, seconds=.05)).outcome,
                             Outcome.RETIREMENT_INCOMPLETE)
            await self.until(lambda: manager.snapshot().supervisory_threads == 0)
            slot = next(iter(manager._slots.values()))
            previous = slot.attempt
            with patch.object(threading.Thread, "start", side_effect=RuntimeError("synthetic")):
                self.assertFalse((await manager.reap_quarantined()).complete)
            self.assertIsNot(slot.attempt, previous)
            self.assertTrue(slot.finished.done())
            self.assertFalse(slot.report.reaped)
            self.assertEqual(manager.snapshot().active_process_slots, 1)
            self.doubles[-1].exited.set()
            self.assertTrue((await manager.reap_quarantined()).complete)
            self.assertEqual(factory.call_count, 1)

    async def test_cancellation_while_report_pending_keeps_slot_owned(self):
        async with self.held_reaped_report() as f:
            entered = asyncio.Event()

            async def collect():
                entered.set()
                return await f.manager.reap_quarantined()

            task = asyncio.create_task(collect())
            f.jobs.append(task)
            await entered.wait()
            task.cancel("pending-report-cancel")
            with self.assertRaises(asyncio.CancelledError) as caught:
                await task
            self.assertEqual(caught.exception.args, ("pending-report-cancel",))
            self.assertEqual(f.manager.snapshot().active_process_slots, 1)
            self.assertFalse(f.slot.finished.done())
            f.publish(*f.report_args)
            self.assertTrue((await f.manager.reap_quarantined()).complete)

    async def test_stale_attempt_reports_cannot_replace_current_terminal_report(self):
        manager = self.manager(max_active_jobs=1)
        publish, supervise = manager._finished, manager._supervise
        held, exit_gate = [], threading.Event()
        ready = asyncio.Event()

        def hold_current(slot, report, attempt):
            held.append((slot, report, attempt))
            ready.set()

        def hold_thread_exit(slot, existing, attempt):
            supervise(slot, existing, attempt)
            if existing and not exit_gate.wait(5):
                raise TimeoutError()

        with patch.object(manager._context, "Process", side_effect=self.double_factory(refuse_exit=True)) as factory:
            self.assertEqual((await self.execute(manager, JobType.SUCCESS, seconds=.05)).outcome,
                             Outcome.RETIREMENT_INCOMPLETE)
            await self.until(lambda: manager.snapshot().supervisory_threads == 0)
            slot = next(iter(manager._slots.values()))
            previous = slot.attempt
            self.assertFalse(slot.report.reaped)
            process = self.doubles[-1]
            process.exited.set()
            task = None
            try:
                with patch.object(manager, "_finished", side_effect=hold_current), \
                     patch.object(manager, "_supervise", side_effect=hold_thread_exit):
                    task = asyncio.create_task(manager.reap_quarantined())
                    await asyncio.wait_for(ready.wait(), 5)
                    self.assertIsNot(slot.attempt, previous)
                    publish(slot, pm._Report(True, 0), previous)
                    publish(slot, pm._Report(False), previous)
                    self.assertIsNone(slot.report)
                    self.assertFalse(slot.finished.done())
                    publish(*held[0])
                    publish(slot, pm._Report(False), slot.attempt)
                    self.assertTrue(slot.report.reaped)
                    self.assertFalse(manager._finalize(slot))  # Supervisor still owns its thread.
                    self.assertEqual(manager.snapshot().active_process_slots, 1)
                    exit_gate.set()
                    self.assertTrue((await task).complete)
            finally:
                exit_gate.set()
                if task is not None:
                    await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 5)
            publish(slot, pm._Report(False), previous)
            self.assertTrue(slot.report.reaped)
            self.assertEqual((process.joins, process.closes, factory.call_count), (1, 1, 1))

    async def test_success_and_recovery(self):
        manager = self.manager()
        for index in range(2):
            result = await self.execute(manager, JobType.SUCCESS, text="synthetic-ok")
            self.assertEqual(result.outcome, Outcome.SUCCESS)
            self.assertEqual(result.output, "synthetic-ok")
            self.assertTrue(result.reaped)
            self.assertEqual(manager.snapshot().active_process_slots, 0)
            self.assertEqual(manager.snapshot().reaped_count, index + 1)

    async def test_child_error_and_crash(self):
        manager = self.manager()
        for kind, outcome in ((JobType.ERROR, Outcome.CHILD_ERROR), (JobType.CRASH, Outcome.CHILD_CRASH)):
            result = await self.execute(manager, kind)
            self.assertEqual(result.outcome, outcome)
            self.assertIsNone(result.output)
            self.assertTrue(result.reaped)

    async def test_unresponsive_deadline_is_reaped(self):
        manager = self.manager()
        result = await self.execute(manager, JobType.UNRESPONSIVE, seconds=.25)
        self.assertEqual(result.outcome, Outcome.DEADLINE_EXPIRED)
        self.assertTrue(result.reaped)

    async def test_two_concurrent_jobs_and_bounded_capacity(self):
        manager = self.manager()
        tasks = [asyncio.create_task(self.execute(manager, JobType.DELAY, delay_ms=200)) for _ in range(2)]
        await self.until(lambda: manager.snapshot().running == 2)
        self.assertEqual(manager.snapshot().active_process_slots, 2)
        self.assertEqual(manager.snapshot().available_capacity, 0)
        results = await asyncio.gather(*tasks)
        self.assertTrue(all(r.outcome is Outcome.SUCCESS and r.reaped for r in results))
        self.assertEqual(manager.snapshot().available_capacity, 2)

    async def test_identifier_collision_cannot_overwrite_owned_process_slot(self):
        manager = self.manager()
        with patch("mooby.api.process_manager.uuid4", return_value=SimpleNamespace(hex="a"*32)):
            first = asyncio.create_task(self.execute(manager, JobType.DELAY, delay_ms=300))
            await self.until(lambda: manager.snapshot().running == 1)
            second = await self.execute(manager, JobType.SUCCESS)
            self.assertEqual(second.outcome, Outcome.UNAVAILABLE)
            self.assertEqual(manager.snapshot().active_process_slots, 1)
            self.assertEqual((await first).outcome, Outcome.SUCCESS)

    async def test_saturation_uses_only_existing_admission_queue(self):
        manager = self.manager(max_active_jobs=1, max_waiting_jobs=1)
        first = asyncio.create_task(self.execute(manager, JobType.DELAY, delay_ms=1000))
        await self.until(lambda: manager.snapshot().running == 1)
        second = asyncio.create_task(self.execute(manager, JobType.SUCCESS))
        await self.until(lambda: manager.snapshot().waiting_requests == 1)
        third = await self.execute(manager, JobType.SUCCESS)
        self.assertEqual(third.outcome, Outcome.QUEUE_FULL)
        self.assertEqual((await second).outcome, Outcome.ADMISSION_EXPIRED)
        self.assertEqual((await first).outcome, Outcome.SUCCESS)
        self.assertEqual(manager.snapshot().reaped_count, 1)

    async def test_idle_shutdown_and_closed_submission(self):
        manager = self.manager()
        self.assertTrue((await manager.shutdown()).complete)
        self.assertTrue((await manager.shutdown()).complete)
        self.assertEqual((await self.execute(manager, JobType.SUCCESS)).outcome, Outcome.CLOSED)

    async def test_ipc_faults_and_oversized_child_output(self):
        manager = self.manager()
        for kind, outcome in ((JobType.BAD_VERSION, Outcome.PROTOCOL_ERROR),
                              (JobType.BROKEN_IPC, Outcome.PROTOCOL_ERROR),
                              (JobType.OVERSIZE_FRAME, Outcome.PAYLOAD_TOO_LARGE),
                              (JobType.OVERSIZE_OUTPUT, Outcome.PAYLOAD_TOO_LARGE)):
            with self.subTest(kind=kind):
                result = await self.execute(manager, kind)
                self.assertEqual(result.outcome, outcome)
                self.assertTrue(result.reaped)
                self.assertIsNone(result.output)

    async def test_oversized_serialized_input_rejected_before_process_creation(self):
        manager = self.manager()
        # Char count fits, JSON escaping exceeds the byte limit.
        with patch.object(manager._context, "Process") as factory:
            result = await self.execute(manager, JobType.SUCCESS, text="\u2603" * 2000)
            self.assertEqual(result.outcome, Outcome.PAYLOAD_TOO_LARGE)
            factory.assert_not_called()
        self.assertEqual(manager.snapshot().active_process_slots, 0)

    async def test_invalid_job_and_expired_parent_never_spawn(self):
        manager = self.manager()
        with patch.object(manager._context, "Process") as factory:
            self.assertEqual((await manager.execute(object(), Deadline.after(5))).outcome,
                             Outcome.PROTOCOL_ERROR)
            result = await self.execute(manager, JobType.SUCCESS, seconds=0)
            self.assertEqual(result.outcome, Outcome.DEADLINE_EXPIRED)
            factory.assert_not_called()

    async def test_expiry_after_reserving_slot_but_before_drive_never_spawns(self):
        manager = self.manager()
        entered, proceed = asyncio.Event(), asyncio.Event()
        now = [100.0]
        deadline = Deadline(101.0, lambda: now[0])
        drive = manager._drive

        async def held_drive(slot):
            entered.set()
            await proceed.wait()
            return await drive(slot)

        with patch.object(manager, "_drive", side_effect=held_drive), \
             patch.object(manager._context, "Process") as factory:
            task = asyncio.create_task(manager.execute(SyntheticJob(JobType.SUCCESS), deadline))
            await asyncio.wait_for(entered.wait(), 2)
            now[0] = 102.0
            proceed.set()
            self.assertEqual((await task).outcome, Outcome.DEADLINE_EXPIRED)
            factory.assert_not_called()
        self.assertEqual(manager.snapshot().active_process_slots, 0)

    async def test_cancel_while_queued_preserves_native_cancellation(self):
        manager = self.manager(max_active_jobs=1)
        active = asyncio.create_task(self.execute(manager, JobType.DELAY, delay_ms=400))
        await self.until(lambda: manager.snapshot().running == 1)
        queued = asyncio.create_task(self.execute(manager, JobType.SUCCESS))
        await self.until(lambda: manager.snapshot().waiting_requests == 1)
        queued.cancel("queued-cancel")
        with self.assertRaises(asyncio.CancelledError) as caught:
            await queued
        self.assertIs(type(caught.exception), asyncio.CancelledError)
        self.assertEqual(manager.snapshot().waiting_requests, 0)
        self.assertEqual((await active).outcome, Outcome.SUCCESS)

    async def test_process_construction_failure_is_unavailable_without_leaked_slot(self):
        manager = self.manager()
        with patch.object(manager._context, "Process", side_effect=OSError("synthetic-private")):
            result = await self.execute(manager, JobType.SUCCESS)
        self.assertEqual(result.outcome, Outcome.UNAVAILABLE)
        self.assertEqual(manager.snapshot().active_process_slots, 0)
        self.assertEqual(manager.snapshot().reaped_count, 0)

    async def test_socket_setup_failure_closes_both_descriptors_without_spawning(self):
        manager = self.manager()
        left, right = MagicMock(), MagicMock()
        left.setblocking.side_effect = OSError("synthetic")
        with patch("mooby.api.process_manager.socket.socketpair", return_value=(left, right)), \
             patch.object(manager._context, "Process") as factory:
            result = await self.execute(manager, JobType.SUCCESS)
        self.assertEqual(result.outcome, Outcome.UNAVAILABLE)
        left.close.assert_called_once()
        right.close.assert_called_once()
        factory.assert_not_called()

    async def test_execution_deadline_during_delay(self):
        manager = self.manager()
        result = await self.execute(manager, JobType.DELAY, delay_ms=1000, seconds=.25)
        self.assertEqual(result.outcome, Outcome.DEADLINE_EXPIRED)
        self.assertTrue(result.reaped)

    async def test_partial_frame_cannot_block_event_loop_or_defeat_deadline(self):
        manager = self.manager()
        ticks = 0
        task = asyncio.create_task(self.execute(manager, JobType.PARTIAL_FRAME, seconds=.25))
        while not task.done():
            ticks += 1
            await asyncio.sleep(.01)
        self.assertGreater(ticks, 3)
        self.assertEqual((await task).outcome, Outcome.DEADLINE_EXPIRED)
        self.assertTrue((await task).reaped)

    async def test_external_cancellation_during_ipc_preserves_identity(self):
        manager = self.manager()
        reading = asyncio.Event()
        original_receive = wire.receive_async
        seen = []

        async def receive(*args):
            reading.set()
            return await original_receive(*args)

        async def job():
            try:
                await manager.execute(SyntheticJob(JobType.DELAY, delay_ms=3000), Deadline.after(5))
            except asyncio.CancelledError as error:
                seen.append(error)
                raise

        with patch.object(wire, "receive_async", side_effect=receive):
            task = asyncio.create_task(job())
            await asyncio.wait_for(reading.wait(), 5)
            task.cancel("local-synthetic-cancel")
            with self.assertRaises(asyncio.CancelledError) as caught:
                await asyncio.wait_for(task, 5)
        self.assertIs(type(caught.exception), asyncio.CancelledError)
        self.assertIs(caught.exception, seen[0])
        self.assertEqual(caught.exception.args, ("local-synthetic-cancel",))
        self.assertEqual(manager.snapshot().active_process_slots, 0)

    async def test_asyncio_timeout_is_not_replaced_by_manager(self):
        manager = self.manager()
        with self.assertRaises(TimeoutError):
            async with asyncio.timeout(.2):
                await manager.execute(SyntheticJob(JobType.DELAY, delay_ms=1000), Deadline.after(5))
        self.assertEqual(manager.snapshot().active_process_slots, 0)

    async def test_cancellation_during_startup_and_responsive_loop(self):
        manager = self.manager(max_active_jobs=1)
        with patch.object(manager._context, "Process", side_effect=self.double_factory(block_start=True)):
            task = asyncio.create_task(manager.execute(SyntheticJob(JobType.SUCCESS), Deadline.after(5)))
            await self.until(lambda: self.doubles and self.doubles[0].start_entered.is_set())
            # Waiting for blocked start happened on the thread, not this event loop.
            task.cancel()
            self.doubles[0].start_gate.set()
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(task, 5)
        self.assertEqual(manager.snapshot().active_process_slots, 0)

    async def test_repeated_cancellation_during_cleanup_keeps_original_exception(self):
        manager = self.manager(max_active_jobs=1)
        with patch.object(manager._context, "Process", side_effect=self.double_factory(block_terminate=True)):
            task = asyncio.create_task(manager.execute(SyntheticJob(JobType.SUCCESS), Deadline.after(5)))
            await self.until(lambda: manager.snapshot().running == 1)
            task.cancel("first-cancellation")
            await self.until(lambda: self.doubles[0].terminate_entered.is_set())
            task.cancel("second-cancellation")
            self.doubles[0].terminate_gate.set()
            with self.assertRaises(asyncio.CancelledError) as caught:
                await asyncio.wait_for(task, 5)
            self.assertEqual(caught.exception.args, ("first-cancellation",))
        self.assertEqual(self.doubles[0].joins, 1)
        self.assertEqual(self.doubles[0].closes, 1)

    async def test_quarantine_holds_capacity_and_no_replacement_spawning(self):
        manager = self.manager(max_active_jobs=1)
        with patch.object(manager._context, "Process", side_effect=self.double_factory(refuse_exit=True)) as factory:
            result = await self.execute(manager, JobType.SUCCESS, seconds=.05)
            self.assertEqual(result.outcome, Outcome.RETIREMENT_INCOMPLETE)
            self.assertEqual(result.cause, Outcome.DEADLINE_EXPIRED)
            state = manager.snapshot()
            self.assertEqual((state.active_process_slots, state.unreaped, state.available_capacity), (1,1,0))
            for _ in range(3):
                self.assertEqual((await self.execute(manager, JobType.SUCCESS)).outcome, Outcome.UNAVAILABLE)
            self.assertEqual(factory.call_count, 1)
            self.assertFalse((await manager.shutdown()).complete)
            await self.until(lambda: manager.snapshot().supervisory_threads == 0)
            self.doubles[0].exited.set()
            report = await manager.reap_quarantined()
            self.assertTrue(report.complete)
            self.assertEqual(factory.call_count, 1)
            self.assertEqual(self.doubles[0].joins, 1)
            self.assertTrue((await manager.reap_quarantined()).complete)
            self.assertEqual(manager.snapshot().reaped_count, 1)

    async def test_blocked_start_retains_thread_slot_until_later_collection(self):
        manager = self.manager(max_active_jobs=1)
        with patch.object(manager._context, "Process", side_effect=self.double_factory(block_start=True)) as factory:
            result = await self.execute(manager, JobType.SUCCESS, seconds=.05)
            self.assertEqual(result.outcome, Outcome.RETIREMENT_INCOMPLETE)
            self.assertEqual(manager.snapshot().supervisory_threads, 1)
            self.assertEqual((await self.execute(manager, JobType.SUCCESS)).outcome, Outcome.UNAVAILABLE)
            self.assertEqual(factory.call_count, 1)
            self.doubles[0].unblock()
            await self.until(lambda: manager.snapshot().supervisory_threads == 0)
            self.assertTrue((await manager.reap_quarantined()).complete)

    async def test_concurrent_reaping_never_double_joins_or_replaces(self):
        manager = self.manager(max_active_jobs=1)
        with patch.object(manager._context, "Process", side_effect=self.double_factory(refuse_exit=True)) as factory:
            result = await self.execute(manager, JobType.SUCCESS, seconds=.05)
            self.assertEqual(result.outcome, Outcome.RETIREMENT_INCOMPLETE)
            await self.until(lambda: manager.snapshot().supervisory_threads == 0)
            self.doubles[0].exited.set()
            await asyncio.gather(manager.reap_quarantined(), manager.reap_quarantined())
            self.assertEqual(manager.snapshot().active_process_slots, 0)
            self.assertEqual(self.doubles[0].joins, 1)
            self.assertEqual(factory.call_count, 1)

    async def test_cancel_reaping_restores_quarantine_accounting(self):
        manager = self.manager(max_active_jobs=1)
        with patch.object(manager._context, "Process", side_effect=self.double_factory(refuse_exit=True)):
            self.assertEqual((await self.execute(manager, JobType.SUCCESS, seconds=.05)).outcome,
                             Outcome.RETIREMENT_INCOMPLETE)
            await self.until(lambda: manager.snapshot().supervisory_threads == 0)
            self.doubles[0].terminate_entered.clear()
            self.doubles[0].terminate_gate.clear()
            task = asyncio.create_task(manager.reap_quarantined())
            await self.until(lambda: self.doubles[0].terminate_entered.is_set())
            task.cancel("reap-cancel")
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertEqual(manager.snapshot().unreaped, 1)
            self.doubles[0].unblock()
            await self.until(lambda: manager.snapshot().supervisory_threads == 0)
            self.assertTrue((await manager.reap_quarantined()).complete)

    async def test_termination_escalates_to_kill_and_reaps_once(self):
        manager = self.manager(max_active_jobs=1)
        with patch.object(manager._context, "Process", side_effect=self.double_factory(require_kill=True)):
            result = await self.execute(manager, JobType.SUCCESS, seconds=.05)
            self.assertEqual(result.outcome, Outcome.DEADLINE_EXPIRED)
            self.assertTrue(result.reaped)
            self.assertEqual(self.doubles[0].kills, 1)
            self.assertEqual(self.doubles[0].joins, 1)

    async def test_supervisory_thread_start_failure_does_not_leak_sockets_or_capacity(self):
        manager = self.manager()
        with patch("mooby.api.process_manager.Thread.start", side_effect=RuntimeError("synthetic")):
            self.assertEqual((await self.execute(manager, JobType.SUCCESS)).outcome, Outcome.UNAVAILABLE)
        self.assertEqual(manager.snapshot().active_process_slots, 0)

    async def test_start_failure_is_conservatively_quarantined(self):
        manager = self.manager(max_active_jobs=1)
        factory = self.double_factory()

        def fail_start(**kwargs):
            process = factory(**kwargs)
            process.start = lambda: (_ for _ in ()).throw(OSError("synthetic-private-message"))
            return process

        with patch.object(manager._context, "Process", side_effect=fail_start):
            result = await self.execute(manager, JobType.SUCCESS)
            self.assertEqual(result.outcome, Outcome.RETIREMENT_INCOMPLETE)
            self.assertEqual(result.cause, Outcome.UNAVAILABLE)
            self.assertNotIn("synthetic-private-message", repr(result))
            self.doubles[0].exited.set()
            self.assertTrue((await manager.reap_quarantined()).complete)

    async def test_shutdown_stops_active_job_and_rejects_queued_job(self):
        manager = self.manager(max_active_jobs=1)
        active = asyncio.create_task(self.execute(manager, JobType.DELAY, delay_ms=3000))
        await self.until(lambda: manager.snapshot().running == 1)
        waiting = asyncio.create_task(self.execute(manager, JobType.SUCCESS))
        await self.until(lambda: manager.snapshot().waiting_requests == 1)
        report = await manager.shutdown()
        self.assertTrue(report.complete)
        self.assertEqual((await waiting).outcome, Outcome.CLOSED)
        self.assertTrue((await active).reaped)

    async def test_shutdown_cancellation_is_native_and_does_not_orphan_cleanup(self):
        manager = self.manager(max_active_jobs=1)
        with patch.object(manager._context, "Process", side_effect=self.double_factory(block_terminate=True)):
            job = asyncio.create_task(manager.execute(SyntheticJob(JobType.SUCCESS), Deadline.after(5)))
            await self.until(lambda: manager.snapshot().running == 1)
            shutdown = asyncio.create_task(manager.shutdown())
            await self.until(lambda: self.doubles[0].terminate_entered.is_set())
            shutdown.cancel("shutdown-cancel")
            with self.assertRaises(asyncio.CancelledError) as caught:
                await shutdown
            self.assertEqual(caught.exception.args, ("shutdown-cancel",))
            self.doubles[0].terminate_gate.set()
            await job
            self.assertTrue((await manager.shutdown()).complete)

    async def test_hard_cap_and_runtime_integration_boundary(self):
        with self.assertRaises(ValueError):
            ProcessManager(replace(DeadlinePolicy(), max_active_jobs=3))
        manager = self.manager()
        self.assertEqual(manager._context.get_start_method(), "spawn")
        self.assertEqual(manager.snapshot().supervisory_threads, 0)
        root = Path(__file__).resolve().parents[1]
        for name in ("app.py", "repository.py", "config.py", "observability.py"):
            tree = ast.parse((root/"src/mooby/api"/name).read_text())
            for node in ast.walk(tree):
                if isinstance(node, (ast.Import, ast.ImportFrom)):
                    self.assertNotIn("process_manager", ast.unparse(node))


class CleanupOrderingTests(unittest.IsolatedAsyncioTestCase):
    async def cleanup_failure(self, *, shutdown_error=None, fallback_error=False):
        case = ProcessManagerTests("test_success_and_recovery")
        await case.asyncSetUp()
        killed = []
        owned = SimpleNamespace(pid=999991, kill=MagicMock(), join=MagicMock())
        unrelated = SimpleNamespace(pid=999992, kill=MagicMock(), join=MagicMock())

        def kill():
            if fallback_error:
                raise OSError("synthetic fallback failure")
            killed.append(owned)

        owned.kill.side_effect = kill
        manager = SimpleNamespace(
            _slots={"owned": SimpleNamespace(process=owned, thread=None, task=None)},
            shutdown=AsyncMock(return_value=SimpleNamespace(complete=False), side_effect=shutdown_error),
            reap_quarantined=AsyncMock(return_value=SimpleNamespace(complete=False)),
            snapshot=lambda: "incomplete synthetic manager",
        )
        case.managers = [manager]

        def active_children():
            return ([owned] if owned not in killed else []) + [unrelated]

        expected = type(shutdown_error) if shutdown_error else AssertionError
        with patch("test_process_manager.multiprocessing.active_children", side_effect=active_children) as inspect:
            with self.assertRaises(expected) as caught:
                await case.asyncTearDown()
        self.assertGreaterEqual(inspect.call_count, 2)
        owned.kill.assert_called_once()
        unrelated.kill.assert_not_called()
        unrelated.join.assert_not_called()
        if not fallback_error:
            owned.join.assert_called_once_with(timeout=2)
        if shutdown_error is not None:
            self.assertIs(caught.exception, shutdown_error)
        return caught.exception

    async def test_incomplete_shutdown_still_cleans_only_owned_child_and_preserves_assertion(self):
        error = await self.cleanup_failure()
        self.assertIn("incomplete synthetic manager", str(error))

    async def test_shutdown_exception_still_runs_fallback_and_preserves_exception_identity(self):
        await self.cleanup_failure(shutdown_error=RuntimeError("synthetic shutdown failure"))

    async def test_unresolved_owned_resource_is_reported_without_hiding_original_failure(self):
        error = await self.cleanup_failure(fallback_error=True)
        notes = " ".join(error.__notes__)
        self.assertIn("owned-child fallback failed", notes)
        self.assertIn("live owned child processes", notes)


class ProtocolTests(unittest.IsolatedAsyncioTestCase):
    def test_invalid_envelopes_and_values(self):
        valid = {"v":1, "id":"a"*32, "type":"success", "budget_ms":100,
                 "expires_at_seconds":100.0,
                 "input":{"text":"synthetic", "delay_ms":0}}
        for change in ({"v":2}, {"v":True}, {"type":[]}, {"id":"private"},
                       {"budget_ms":0}, {"expires_at_seconds":float('inf')},
                       {"input":{"text":"synthetic", "extra":True}}):
            with self.assertRaises(wire.ProtocolError):
                wire.validate_request({**valid, **change})
        for data in (b'{"v":1,"v":1}', b'[]', b'{"n":NaN}', b'not-json'):
            with self.assertRaises(wire.ProtocolError):
                wire.decode(data, wire.MAX_REQUEST_BYTES)

    def test_response_identity_and_allowlist(self):
        value={"v":1,"id":"a"*32,"type":"success","status":"success","output":"ok","error":"none"}
        for change in ({"id":"b"*32}, {"extra":"private"}, {"error":"raw-exception"},
                       {"status":"failure"}, {"output":{}}, {"type":"delay"}):
            with self.assertRaises(wire.ProtocolError):
                wire.validate_response({**value,**change}, "a"*32, "success")

    async def test_frame_size_rejected_before_body_read(self):
        left,right=socket.socketpair()
        left.setblocking(False)
        try:
            right.sendall(wire.HEADER.pack(wire.MAX_RESPONSE_BYTES+1))
            # Peer remains open and sends no payload; rejection must not wait for it.
            with self.assertRaises(wire.PayloadTooLarge):
                await asyncio.wait_for(wire.receive_async(left,wire.MAX_RESPONSE_BYTES),1)
        finally:
            left.close();right.close()

    async def test_truncated_header_and_body(self):
        for data in (b'\x00',wire.HEADER.pack(8)+b'{}'):
            left,right=socket.socketpair()
            left.setblocking(False)
            try:
                right.sendall(data);right.close()
                with self.assertRaises(wire.ProtocolError):
                    await wire.receive_async(left,wire.MAX_RESPONSE_BYTES)
            finally:
                left.close();right.close()
