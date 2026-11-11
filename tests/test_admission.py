"""Inactive admission tests: event-synchronized asyncio only; no execution workers."""

import ast
import asyncio
from dataclasses import replace
from pathlib import Path
import unittest
from unittest.mock import patch

from mooby.api.admission import AdmissionCancelled, AdmissionController, AdmissionLease, AdmissionOutcome
from mooby.api.deadline_policy import Deadline, DeadlinePolicy


class Clock:
    def __init__(self):
        self.now = 100.0

    def __call__(self):
        return self.now


class ManualTimer:
    def __init__(self, delay, callback, args):
        self.delay, self.callback, self.args = delay, callback, args
        self.cancelled = False

    def cancel(self):
        self.cancelled = True

    def fire(self):
        self.callback(*self.args)


class AdmissionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.controllers = []
        self.tasks_before = asyncio.all_tasks()

    async def asyncTearDown(self):
        remaining = asyncio.all_tasks() - self.tasks_before - {asyncio.current_task()}
        for task in remaining:
            task.cancel()
        if remaining:
            await asyncio.gather(*remaining, return_exceptions=True)
        self.assertEqual(remaining, set(), "test left pending admission tasks")
        for controller in self.controllers:
            state = controller.snapshot()
            self.assertEqual((state.active_jobs, state.waiting_jobs), (0, 0))
        # Cancelled loop handles may remain in its heap until the next tick;
        # none may retain a live admission timeout.
        for handle in asyncio.get_running_loop()._scheduled:
            owner = getattr(handle._callback, "__self__", None)
            self.assertFalse(owner in self.controllers and not handle.cancelled())

    def controller(self, **changes):
        policy = replace(DeadlinePolicy(), max_active_jobs=1, **changes)
        controller = AdmissionController(policy)
        self.controllers.append(controller)
        return controller

    def parent(self, clock=None, seconds=5):
        return Deadline.after(seconds, **({"clock":clock} if clock else {}))

    async def start_job(self, controller, *, parent=None, order=None, name=None):
        started, entered, finish = asyncio.Event(), asyncio.Event(), asyncio.Event()
        results = []

        async def job():
            started.set()
            async with controller.admit(parent or self.parent()) as result:
                results.append(result)
                if order is not None and result.accepted:
                    order.append(name)
                entered.set()
                if result.accepted:
                    await finish.wait()
                return result

        task = asyncio.create_task(job())
        await started.wait()  # job reaches its first blocking await before resuming us
        return task, entered, finish, results

    def timers(self, controller):
        handles = []
        loop = asyncio.get_running_loop()
        original = loop.call_later

        def schedule(delay, callback, *args, **kwargs):
            if getattr(callback, "__self__", None) is controller:
                handle = ManualTimer(delay, callback, args)
                handles.append(handle)
                return handle
            return original(delay, callback, *args, **kwargs)

        return patch.object(loop, "call_later", schedule), handles

    async def test_immediate_admission_and_scope_release(self):
        controller = self.controller()
        async with controller.admit(self.parent()) as result:
            self.assertTrue(result.accepted)
            self.assertEqual(result.outcome, AdmissionOutcome.ACCEPTED)
            self.assertEqual(controller.snapshot().active_jobs, 1)
            self.assertEqual(controller.snapshot().available_active_capacity, 0)
        self.assertTrue(result.lease.released)
        self.assertEqual(controller.snapshot().available_active_capacity, 1)

    async def test_policy_default_allows_two_active_jobs(self):
        controller = AdmissionController(DeadlinePolicy())
        self.controllers.append(controller)
        async with controller.admit(self.parent()) as first:
            async with controller.admit(self.parent()) as second:
                self.assertTrue(first.accepted and second.accepted)
                self.assertEqual(controller.snapshot().active_jobs, 2)

    async def test_queue_capacity_and_full_rejection(self):
        controller = self.controller(max_waiting_jobs=2)
        async with controller.admit(self.parent()):
            jobs = [await self.start_job(controller) for _ in range(2)]
            state = controller.snapshot()
            self.assertEqual((state.active_jobs, state.waiting_jobs), (1, 2))
            async with controller.admit(self.parent()) as result:
                self.assertEqual(result.outcome, AdmissionOutcome.QUEUE_FULL)
                self.assertIsNone(result.lease)
            controller.close()
            for task, _, _, _ in jobs:
                self.assertEqual((await task).outcome, AdmissionOutcome.CLOSED)

    async def test_zero_waiting_mode_still_allows_immediate_admission(self):
        controller = self.controller(max_waiting_jobs=0, admission_wait_ms=0)
        async with controller.admit(self.parent()) as first:
            self.assertTrue(first.accepted)
            async with controller.admit(self.parent()) as second:
                self.assertEqual(second.outcome, AdmissionOutcome.QUEUE_FULL)
            self.assertEqual(controller.snapshot().waiting_jobs, 0)

    async def test_expired_parent_rejected_even_with_free_capacity(self):
        controller = self.controller()
        async with controller.admit(self.parent(seconds=0)) as result:
            self.assertEqual(result.outcome, AdmissionOutcome.PARENT_EXPIRED)

    async def test_real_event_loop_admission_timer(self):
        controller = self.controller(admission_wait_ms=1)
        async with controller.admit(self.parent()):
            task, _, _, _ = await self.start_job(controller)
            result = await asyncio.wait_for(task, timeout=1)
            self.assertEqual(result.outcome, AdmissionOutcome.WAIT_EXPIRED)
            self.assertEqual(controller.snapshot().waiting_jobs, 0)

    async def test_admission_wait_uses_shorter_child_deadline(self):
        controller, clock = self.controller(), Clock()
        patcher, handles = self.timers(controller)
        async with controller.admit(self.parent()):
            with patcher:
                task, _, _, _ = await self.start_job(controller, parent=self.parent(clock))
                self.assertAlmostEqual(handles[0].delay, .1)
                clock.now += .2
                handles[0].fire()
                result = await task
            self.assertEqual(result.outcome, AdmissionOutcome.WAIT_EXPIRED)
            self.assertTrue(handles[0].cancelled)

    async def test_parent_deadline_shortens_admission_wait(self):
        controller, clock = self.controller(), Clock()
        patcher, handles = self.timers(controller)
        async with controller.admit(self.parent()):
            with patcher:
                task, _, _, _ = await self.start_job(controller, parent=self.parent(clock, .02))
                self.assertAlmostEqual(handles[0].delay, .02)
                clock.now += .02
                handles[0].fire()
                self.assertEqual((await task).outcome, AdmissionOutcome.PARENT_EXPIRED)

    async def test_early_timer_rechecks_absolute_deadline(self):
        controller, clock = self.controller(), Clock()
        patcher, handles = self.timers(controller)
        async with controller.admit(self.parent()):
            with patcher:
                task, _, _, _ = await self.start_job(controller, parent=self.parent(clock))
                clock.now += .04
                handles[0].fire()
                self.assertAlmostEqual(handles[1].delay, .06)
                self.assertFalse(task.done())
                clock.now += .1
                handles[1].fire()
                self.assertEqual((await task).outcome, AdmissionOutcome.WAIT_EXPIRED)

    async def test_release_rechecks_deadline_before_timer_callback(self):
        controller, clock = self.controller(), Clock()
        patcher, handles = self.timers(controller)
        async with controller.admit(self.parent()) as owner:
            with patcher:
                task, _, _, _ = await self.start_job(controller, parent=self.parent(clock, .02))
                clock.now += .03
                owner.lease.release()
                self.assertEqual((await task).outcome, AdmissionOutcome.PARENT_EXPIRED)
            self.assertTrue(handles[0].cancelled)

    async def test_cancel_before_job_starts_has_no_capacity_effect(self):
        controller = self.controller()

        async def job():
            async with controller.admit(self.parent()):
                self.fail("cancelled task entered admission")

        task = asyncio.create_task(job())
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(task.cancelled())

    async def test_pending_cancellation_prevents_immediate_acquisition(self):
        controller = self.controller()

        async def job():
            asyncio.current_task().cancel()
            async with controller.admit(self.parent()):
                self.fail("pending cancellation accepted")

        task = asyncio.create_task(job())
        with self.assertRaises(AdmissionCancelled) as error:
            await task
        self.assertEqual(error.exception.outcome, AdmissionOutcome.CANCELLED)

    async def test_cancel_queued_waiter_frees_waiting_capacity(self):
        controller = self.controller(max_waiting_jobs=1)
        patcher, handles = self.timers(controller)
        async with controller.admit(self.parent()):
            with patcher:
                task, _, _, _ = await self.start_job(controller)
                task.cancel("PRIVATE cancellation message")
                with self.assertRaises(AdmissionCancelled) as error:
                    await task
            self.assertNotIn("PRIVATE", str(error.exception))
            self.assertTrue(handles[0].cancelled)
            self.assertEqual(controller.snapshot().waiting_jobs, 0)
            next_task, _, _, _ = await self.start_job(controller)
            controller.close()
            self.assertEqual((await next_task).outcome, AdmissionOutcome.CLOSED)

    async def test_cancel_after_grant_before_task_resumes_recovers_slot(self):
        controller = self.controller()
        async with controller.admit(self.parent()) as owner:
            task, _, _, _ = await self.start_job(controller)
            owner.lease.release()  # reserves an active lease for the waiting task
            self.assertEqual(controller.snapshot().active_jobs, 1)
            task.cancel()
            with self.assertRaises(AdmissionCancelled):
                await task
            self.assertEqual(controller.snapshot().active_jobs, 0)

    async def test_cancel_inside_accepted_scope_releases_slot(self):
        controller = self.controller()
        task, entered, _, _ = await self.start_job(controller)
        await entered.wait()
        task.cancel()
        with self.assertRaises(AdmissionCancelled):
            await task
        self.assertTrue(task.cancelled())

    async def test_double_cancellation_before_resumption_does_not_leak(self):
        controller = self.controller()
        async with controller.admit(self.parent()) as owner:
            task, _, _, _ = await self.start_job(controller)
            owner.lease.release()
            task.cancel()
            task.cancel()
            with self.assertRaises(AdmissionCancelled):
                await task

    async def test_repeated_release_is_idempotent(self):
        controller = self.controller()
        async with controller.admit(self.parent()) as result:
            self.assertTrue(result.lease.release())
            self.assertFalse(result.lease.release())
        self.assertFalse(result.lease.release())
        self.assertEqual(controller.snapshot().available_active_capacity, 1)

    async def test_unregistered_lease_cannot_release_someone_elses_slot(self):
        controller = self.controller()
        async with controller.admit(self.parent()):
            self.assertFalse(AdmissionLease(controller).release())
            self.assertEqual(controller.snapshot().active_jobs, 1)

    async def test_concurrent_double_release_hands_off_exactly_one_slot(self):
        controller = self.controller()
        gate = asyncio.Event()
        async with controller.admit(self.parent()) as owner:
            task, entered, finish, _ = await self.start_job(controller)

            async def release():
                await gate.wait()
                return owner.lease.release()

            releases = [asyncio.create_task(release()) for _ in range(2)]
            gate.set()
            self.assertEqual(sorted(await asyncio.gather(*releases)), [False, True])
            await entered.wait()
            self.assertEqual(controller.snapshot().active_jobs, 1)
            async with controller.admit(self.parent(seconds=0)) as rejected:
                self.assertEqual(rejected.outcome, AdmissionOutcome.PARENT_EXPIRED)
            finish.set()
            await task

    async def test_scope_exception_releases_capacity(self):
        controller = self.controller()
        with self.assertRaisesRegex(ValueError, "controlled"):
            async with controller.admit(self.parent()):
                raise ValueError("controlled")

    async def test_fifo_handoff_and_no_barging(self):
        controller, order = self.controller(max_waiting_jobs=4), []
        async with controller.admit(self.parent()) as owner:
            jobs = [await self.start_job(controller, order=order, name=i) for i in range(3)]
            owner.lease.release()
            await jobs[0][1].wait()
            jobs.append(await self.start_job(controller, order=order, name=3))
            for task, entered, finish, _ in jobs:
                await entered.wait()
                self.assertEqual(controller.snapshot().active_jobs, 1)
                finish.set()
                self.assertTrue((await task).accepted)
        self.assertEqual(order, [0,1,2,3])

    async def test_cancel_first_waiter_preserves_fifo_for_remaining_waiters(self):
        controller, order = self.controller(), []
        async with controller.admit(self.parent()) as owner:
            first = await self.start_job(controller, order=order, name=1)
            second = await self.start_job(controller, order=order, name=2)
            first[0].cancel()
            # Cancellation is scheduled but its cleanup has not resumed yet.
            owner.lease.release()
            with self.assertRaises(AdmissionCancelled):
                await first[0]
            await second[1].wait()
            second[2].set()
            await second[0]
        self.assertEqual(order, [2])

    async def test_closed_controller_rejects_and_close_is_idempotent(self):
        controller = self.controller()
        self.assertTrue(controller.close())
        self.assertFalse(controller.close())
        async with controller.admit(self.parent()) as result:
            self.assertEqual(result.outcome, AdmissionOutcome.CLOSED)
        self.assertTrue(controller.snapshot().closed)

    async def test_close_wakes_all_waiters_without_releasing_active_job(self):
        controller = self.controller()
        patcher, handles = self.timers(controller)
        async with controller.admit(self.parent()):
            with patcher:
                jobs = [await self.start_job(controller) for _ in range(2)]
                controller.close()
                self.assertEqual(controller.snapshot().active_jobs, 1)
                self.assertEqual(controller.snapshot().waiting_jobs, 0)
                for task, _, _, _ in jobs:
                    self.assertEqual((await task).outcome, AdmissionOutcome.CLOSED)
            self.assertTrue(all(handle.cancelled for handle in handles))

    async def test_close_after_grant_before_resumption_reclaims_unclaimed_lease(self):
        controller = self.controller()
        async with controller.admit(self.parent()) as owner:
            task, _, _, _ = await self.start_job(controller)
            owner.lease.release()
            controller.close()
            self.assertEqual((await task).outcome, AdmissionOutcome.CLOSED)

    async def test_parent_expires_after_grant_before_resumption(self):
        controller, clock = self.controller(), Clock()
        async with controller.admit(self.parent()) as owner:
            task, _, _, _ = await self.start_job(controller, parent=self.parent(clock, .02))
            owner.lease.release()
            clock.now += .03
            self.assertEqual((await task).outcome, AdmissionOutcome.PARENT_EXPIRED)

    async def test_admission_expires_after_grant_before_resumption(self):
        controller, clock = self.controller(), Clock()
        async with controller.admit(self.parent()) as owner:
            task, _, _, _ = await self.start_job(controller, parent=self.parent(clock))
            owner.lease.release()
            clock.now += .2
            self.assertEqual((await task).outcome, AdmissionOutcome.WAIT_EXPIRED)

    async def test_expired_front_waiter_does_not_block_next_live_waiter(self):
        controller, clock = self.controller(), Clock()
        async with controller.admit(self.parent()) as owner:
            first = await self.start_job(controller, parent=self.parent(clock, .02))
            second = await self.start_job(controller)
            clock.now += .03
            owner.lease.release()
            self.assertEqual((await first[0]).outcome, AdmissionOutcome.PARENT_EXPIRED)
            await second[1].wait()
            self.assertEqual(controller.snapshot().active_jobs, 1)
            second[2].set()
            await second[0]

    async def test_close_then_cancel_active_owner_releases_without_reopening(self):
        controller = self.controller()
        task, entered, _, _ = await self.start_job(controller)
        await entered.wait()
        controller.close()
        self.assertEqual(controller.snapshot().active_jobs, 1)
        task.cancel()
        with self.assertRaises(AdmissionCancelled):
            await task
        state = controller.snapshot()
        self.assertEqual((state.active_jobs, state.waiting_jobs, state.closed), (0, 0, True))

    async def test_cancel_close_and_release_same_turn(self):
        controller = self.controller()
        async with controller.admit(self.parent()) as owner:
            task, _, _, _ = await self.start_job(controller)
            task.cancel()
            controller.close()
            owner.lease.release()
            with self.assertRaises(AdmissionCancelled):
                await task

    async def test_late_timeout_after_successful_handoff_is_harmless(self):
        controller = self.controller()
        patcher, handles = self.timers(controller)
        async with controller.admit(self.parent()) as owner:
            with patcher:
                task, entered, finish, _ = await self.start_job(controller)
                owner.lease.release()
                await entered.wait()
                handles[0].fire()  # simulate an already-dispatched cancelled callback
                self.assertEqual(controller.snapshot().active_jobs, 1)
                finish.set()
                await task

    async def test_snapshots_are_immutable_and_contain_only_capacity_state(self):
        controller = self.controller()
        state = controller.snapshot()
        self.assertEqual(set(state.__dataclass_fields__), {
            "active_jobs", "waiting_jobs", "available_active_capacity", "closed"})
        with self.assertRaises(AttributeError):
            state.active_jobs = 99

    async def test_different_event_loop_is_rejected_without_mutation(self):
        controller = self.controller()
        controller.snapshot()
        other_loop = object()
        with patch("mooby.api.admission.asyncio.get_running_loop", return_value=other_loop):
            with self.assertRaisesRegex(RuntimeError, "another event loop"):
                controller.close()
        self.assertFalse(controller.snapshot().closed)

    async def test_controller_creates_no_tasks_and_runtime_has_no_import(self):
        controller = self.controller()
        before = asyncio.all_tasks()
        async with controller.admit(self.parent()):
            self.assertEqual(asyncio.all_tasks(), before)
        root = Path(__file__).resolve().parents[1]
        for relative in ("src/mooby/api/app.py", "src/mooby/api/config.py",
                         "src/mooby/api/repository.py", "scripts/launchd_api.py"):
            tree = ast.parse((root/relative).read_text())
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom):
                    self.assertNotEqual(node.module, "mooby.api.admission")
                    self.assertNotIn("admission", [alias.name for alias in node.names])
                elif isinstance(node, ast.Import):
                    self.assertFalse(any(alias.name == "mooby.api.admission" for alias in node.names))
