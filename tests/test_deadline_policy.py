"""Pure inactive policy tests: no timers, execution workers or external services."""

from dataclasses import replace
import ast
import math
import os
from pathlib import Path
import unittest
from unittest.mock import patch

from mooby.api.config import ApiSettings
from mooby.api.deadline_policy import Deadline, DeadlinePolicy, DeadlinePolicyError


class Clock:
    def __init__(self, now=100.0):
        self.now = now

    def __call__(self):
        return self.now


class DeadlineTests(unittest.TestCase):
    def test_absolute_deadline_and_remaining_time(self):
        clock = Clock()
        deadline = Deadline.after(3, clock=clock)
        self.assertEqual(deadline.expires_at_seconds, 103)
        clock.now = 101.25
        self.assertEqual(deadline.remaining_seconds(), 1.75)
        self.assertFalse(deadline.expired())
        self.assertEqual(deadline.expires_at_seconds, 103)

    def test_exact_expiry_and_overrun_are_zero(self):
        clock = Clock()
        deadline = Deadline.after(1, clock=clock)
        for now in (101, 102, 10000):
            clock.now = now
            self.assertEqual(deadline.remaining_seconds(), 0)
            self.assertTrue(deadline.expired())

    def test_zero_duration_is_immediately_expired(self):
        self.assertTrue(Deadline.after(0, clock=Clock()).expired())

    def test_child_cannot_extend_parent(self):
        clock = Clock()
        parent = Deadline.after(5, clock=clock)
        clock.now = 104
        child = parent.child(10)
        self.assertEqual(child.expires_at_seconds, parent.expires_at_seconds)
        self.assertIs(child.clock, clock)
        self.assertEqual(child.remaining_seconds(), 1)

    def test_nested_stages_do_not_restart_overall_budget(self):
        clock = Clock()
        parent = Deadline.after(10, clock=clock)
        first = parent.child(3)
        clock.now = 102
        second = first.child(20)
        self.assertEqual(second.expires_at_seconds, 103)
        clock.now = 104
        self.assertTrue(second.child(100).expired())
        self.assertEqual(parent.remaining_seconds(), 6)

    def test_child_zero_and_expired_parent(self):
        clock = Clock()
        parent = Deadline.after(1, clock=clock)
        self.assertTrue(parent.child(0).expired())
        clock.now = 105
        self.assertEqual(parent.child(3).expires_at_seconds, 101)
        self.assertTrue(parent.child(3).expired())

    def test_clock_epoch_is_arbitrary(self):
        clock = Clock(-50)
        deadline = Deadline.after(2.5, clock=clock)
        clock.now = -48
        self.assertEqual(deadline.remaining_seconds(), .5)

    def test_invalid_durations_are_rejected(self):
        for value in (-1, math.nan, math.inf, -math.inf, True, "1", None, 10**400):
            with self.subTest(value_type=type(value).__name__):
                with self.assertRaises(DeadlinePolicyError):
                    Deadline.after(value, clock=Clock())
                with self.assertRaises(DeadlinePolicyError):
                    Deadline.after(1, clock=Clock()).child(value)

    def test_invalid_clock_or_absolute_timestamp_is_rejected(self):
        for value in (math.nan, math.inf, "private", True):
            with self.assertRaises(DeadlinePolicyError):
                Deadline(value)
            with self.assertRaises(DeadlinePolicyError):
                Deadline.after(1, clock=lambda: value)
        with self.assertRaises(DeadlinePolicyError):
            Deadline.after(1, clock=None)

    def test_nonfinite_clock_during_remaining_or_child_is_rejected(self):
        clock = Clock()
        deadline = Deadline.after(2, clock=clock)
        clock.now = math.nan
        with self.assertRaises(DeadlinePolicyError):
            deadline.remaining_seconds()
        with self.assertRaises(DeadlinePolicyError):
            deadline.child(1)

    def test_overflow_is_rejected(self):
        with self.assertRaises(DeadlinePolicyError):
            Deadline.after(1e308, clock=Clock(1e308))
        with self.assertRaises(DeadlinePolicyError):
            Deadline(1e308, Clock(1e308)).child(1e308)
        with self.assertRaises(DeadlinePolicyError):
            Deadline(1e308, Clock(-1e308)).remaining_seconds()


class PolicyTests(unittest.TestCase):
    def test_defaults_are_inactive_and_fit_current_database_defaults(self):
        policy = DeadlinePolicy.from_mapping({})
        self.assertFalse(policy.enabled)
        self.assertEqual((policy.max_active_jobs, policy.max_waiting_jobs), (2, 4))
        self.assertEqual(policy.minimum_preparation_ms(connection_count=1), 12000)
        self.assertEqual(policy.minimum_preparation_ms(connection_count=2), 16000)
        policy.validate_for_database(connect_timeout_seconds=3, statement_timeout_ms=3000)

    def test_canonical_mapping_and_no_mutation(self):
        values = {"enabled":"false", "feed_prepare_ms":"13000", "max_active_jobs":3}
        before = values.copy()
        policy = DeadlinePolicy.from_mapping(values)
        self.assertEqual((policy.feed_prepare_ms, policy.max_active_jobs), (13000, 3))
        self.assertEqual(values, before)

    def test_activation_is_rejected_even_when_requested(self):
        for value in (True, "true", 1, 0, "0", None):
            with self.assertRaises(DeadlinePolicyError):
                DeadlinePolicy.from_mapping({"enabled": value})
        with self.assertRaises(DeadlinePolicyError):
            DeadlinePolicy(enabled=True)

    def test_malformed_numbers_are_rejected(self):
        for value in ("", " 100", "100 ", "+100", "01", "1.0", "1e2", "1_00", "١٠٠",
                      "${PRIVATE}", "$(private)", "9"*10000, 1.5, True, None, math.nan):
            with self.assertRaises(DeadlinePolicyError):
                DeadlinePolicy.from_mapping({"admission_wait_ms": value})

    def test_unknown_fields_and_errors_do_not_echo_values(self):
        for values in ({"PRIVATE_SECRET":"secret-content"}, {"feed_prepare_ms":"secret-content"}):
            with self.assertRaises(DeadlinePolicyError) as error:
                DeadlinePolicy.from_mapping(values)
            self.assertNotIn("PRIVATE_SECRET", str(error.exception))
            self.assertNotIn("secret-content", str(error.exception))

    def test_invalid_integer_ranges_for_every_policy_allowance(self):
        policy = DeadlinePolicy()
        for name in policy.__dataclass_fields__:
            if name.endswith("_ms"):
                for value in (-1, 300001, True, 1.5):
                    with self.subTest(name=name, value=value):
                        with self.assertRaises(DeadlinePolicyError):
                            replace(policy, **{name:value})
        for name, values in (("max_active_jobs", (0, 65, True)),
                             ("max_waiting_jobs", (-1, 1025, True))):
            for value in values:
                with self.assertRaises(DeadlinePolicyError):
                    replace(policy, **{name:value})

    def test_zero_is_rejected_for_required_allowances(self):
        policy = DeadlinePolicy()
        for name in policy.__dataclass_fields__:
            if name.endswith("_ms") and name not in {"admission_wait_ms", "cancellation_grace_ms"}:
                with self.assertRaises(DeadlinePolicyError):
                    replace(policy, **{name:0})

    def test_no_waiting_mode_and_zero_cancellation_grace(self):
        policy = DeadlinePolicy(admission_wait_ms=0, max_waiting_jobs=0, cancellation_grace_ms=0)
        self.assertFalse(policy.enabled)
        for values in ({"admission_wait_ms":0}, {"max_waiting_jobs":0}):
            with self.assertRaises(DeadlinePolicyError):
                DeadlinePolicy(**values)

    def test_conflicting_stage_combinations_are_rejected(self):
        for values in ({"feed_prepare_ms":11999}, {"readiness_prepare_ms":15999},
                       {"database_operation_ms":250}, {"retirement_ms":1249},
                       {"aggregate_connect_ms":3001}):
            with self.subTest(values=values), self.assertRaises(DeadlinePolicyError):
                DeadlinePolicy(**values)

    def test_retirement_exact_boundary_and_separate_send_allowance(self):
        policy = DeadlinePolicy(retirement_ms=1250, response_send_ms=300000)
        self.assertEqual(policy.minimum_preparation_ms(connection_count=1), 12000)

    def test_database_compatibility_rejects_larger_timeouts(self):
        for connection, statement in ((4,3000), (3,3001)):
            with self.assertRaises(DeadlinePolicyError):
                DeadlinePolicy().validate_for_database(connect_timeout_seconds=connection,
                                                      statement_timeout_ms=statement)

    def test_minimum_driver_timeout_floor_and_maximum_compatibility(self):
        minimum = DeadlinePolicy(aggregate_connect_ms=2000, database_operation_ms=350,
                                 feed_prepare_ms=5200, readiness_prepare_ms=8200)
        minimum.validate_for_database(connect_timeout_seconds=1, statement_timeout_ms=100)
        with self.assertRaises(DeadlinePolicyError):
            replace(minimum, aggregate_connect_ms=1999).validate_for_database(
                connect_timeout_seconds=1, statement_timeout_ms=100)
        maximum = DeadlinePolicy(aggregate_connect_ms=15000, database_operation_ms=10250,
                                 feed_prepare_ms=38000, readiness_prepare_ms=54000)
        maximum.validate_for_database(connect_timeout_seconds=15, statement_timeout_ms=10000)

    def test_invalid_database_timeout_inputs(self):
        for connection, statement in ((0,3000), (16,3000), (3,99), (3,10001), (True,3000), (3,"3000")):
            with self.assertRaises(DeadlinePolicyError):
                DeadlinePolicy().validate_for_database(connect_timeout_seconds=connection,
                                                      statement_timeout_ms=statement)
        with self.assertRaises(DeadlinePolicyError):
            DeadlinePolicy().minimum_preparation_ms(connection_count=3)

    def test_mapping_ignores_parent_environment_and_existing_config_is_unchanged(self):
        values = {"APP_API_DATABASE_URL":"postgresql://example.invalid/test",
                  "APP_API_BEARER_TOKEN":"test-" + "x"*32}
        with patch.dict(os.environ, {"feed_prepare_ms":"1", "enabled":"true"}):
            before = dict(os.environ)
            self.assertEqual(DeadlinePolicy.from_mapping({}), DeadlinePolicy())
            settings = ApiSettings.from_mapping(values)
            self.assertEqual((settings.connect_timeout_seconds, settings.statement_timeout_ms), (3,3000))
            self.assertEqual(dict(os.environ), before)
        # Existing R04 integer grammar is preserved; new policy grammar is stricter.
        settings = ApiSettings.from_mapping({**values, "APP_API_CONNECT_TIMEOUT_SECONDS":" +5 ",
                                           "APP_API_STATEMENT_TIMEOUT_MS":" 04000 "})
        self.assertEqual((settings.connect_timeout_seconds, settings.statement_timeout_ms), (5,4000))

    def test_runtime_modules_do_not_import_inactive_policy(self):
        root = Path(__file__).resolve().parents[1]
        for relative in ("src/mooby/api/app.py", "src/mooby/api/repository.py",
                         "src/mooby/api/config.py", "scripts/launchd_api.py"):
            tree = ast.parse((root/relative).read_text())
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom):
                    self.assertNotIn("deadline_policy", node.module or "")
                    self.assertNotIn("deadline_policy", [alias.name for alias in node.names])
                elif isinstance(node, ast.Import):
                    self.assertFalse(any("deadline_policy" in alias.name for alias in node.names))
