from __future__ import annotations

import asyncio
import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fgpbot import cli
from fgpbot.app import run_bot
from fgpbot.config import ConfigError
from fgpbot.health import SingleInstance, read_status
from tests.helpers import StoreCase, cancel, config


class CLITests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.config=config(Path(self.temp.name))

    def test_missing_config_returns_permanent_error_code(self):
        with patch.object(cli,"load_config",side_effect=ConfigError("missing")),patch("sys.stderr",io.StringIO()):
            self.assertEqual(cli.main(["run"]),2)

    def test_crash_returns_nonzero_for_windows_launcher(self):
        with (patch.object(cli,"load_config",return_value=self.config),
              patch.object(cli,"setup_logging"),patch.object(cli.LOG,"exception"),
              patch.object(cli,"run_bot",AsyncMock(side_effect=RuntimeError("synthetic failure")))):
            self.assertEqual(cli.main(["run"]),1)

    def test_second_instance_returns_three_without_opening_rotating_log(self):
        with (SingleInstance(self.config.data_dir/"bot.lock"),
              patch.object(cli,"load_config",return_value=self.config),
              patch.object(cli,"setup_logging") as setup,patch.object(cli,"run_bot") as run,
              patch("sys.stdout",io.StringIO())):
            self.assertEqual(cli.main(["run"]),3)
            setup.assert_not_called()
            run.assert_not_called()

    def test_doctor_missing_database_is_failure_not_green(self):
        with patch.object(cli,"load_config",return_value=self.config),patch("sys.stdout",io.StringIO()):
            self.assertEqual(cli.main(["doctor","--offline"]),1)

    def test_selftest_without_tests_does_not_report_success(self):
        with patch.object(cli.unittest.defaultTestLoader,"discover",return_value=unittest.TestSuite()),patch("sys.stderr",io.StringIO()):
            self.assertEqual(cli.main(["selftest"]),1)

    def test_status_does_not_need_configuration_or_twitch(self):
        with patch.object(cli,"ROOT",self.config.root),patch.object(cli,"load_config") as load,patch("sys.stdout",io.StringIO()):
            self.assertEqual(cli.main(["status"]),1)
            load.assert_not_called()


class LifecycleTests(StoreCase):
    async def test_unhandled_failure_writes_failed_status_before_reraising(self):
        with patch("fgpbot.app.Application.run",AsyncMock(side_effect=RuntimeError("synthetic crash"))):
            with self.assertRaises(RuntimeError):
                await run_bot(self.config)
        status=read_status(self.config.status_file)
        self.assertEqual(status["status"],"FAILED")
        self.assertFalse(status["transport_ready"])

    async def test_clean_cancellation_writes_stopped_status(self):
        started=asyncio.Event()
        async def wait(_self):
            started.set()
            await asyncio.Future()
        with patch("fgpbot.app.Application.run",wait):
            task=asyncio.create_task(run_bot(self.config))
            await asyncio.wait_for(started.wait(),2)
            await cancel(task)
        status=read_status(self.config.status_file)
        self.assertEqual(status["status"],"STOPPED")
        self.assertFalse(status["transport_ready"])
