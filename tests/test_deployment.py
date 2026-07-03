"""
Deployment sanity tests: systemd unit assumptions and import-path checks.

These catch the failure mode where the bot works from an interactive shell
(`PYTHONPATH=src python -m bot.main`) but crash-loops under systemd because
the unit never sets PYTHONPATH for the src/ layout.
"""
import configparser
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SERVICE_FILE = REPO_ROOT / "deploy" / "rsi-pi-bot.service"
ENV_EXAMPLE = REPO_ROOT / "deploy" / "rsi-pi-bot.env.example"


def load_service():
    parser = configparser.ConfigParser(strict=False)
    parser.optionxform = str  # keep case
    parser.read_string(SERVICE_FILE.read_text())
    return parser


class TestSystemdUnit:
    def test_sets_pythonpath_for_src_layout(self):
        content = SERVICE_FILE.read_text()
        assert "Environment=PYTHONPATH=/opt/rsi-pi-bot/src" in content, (
            "The unit must set PYTHONPATH itself; imports cannot rely on an "
            "interactive shell or an optional env-file line"
        )

    def test_execstart_runs_bot_module(self):
        service = load_service()
        exec_start = service["Service"]["ExecStart"]
        assert exec_start.endswith("-m bot.main")
        assert "/opt/rsi-pi-bot/venv/bin/python" in exec_start

    def test_restart_and_env_file_configured(self):
        service = load_service()
        assert service["Service"]["Restart"] == "always"
        assert service["Service"]["EnvironmentFile"] == "/etc/rsi-pi-bot.env"

    def test_writable_dirs_provisioned(self):
        """State/log dirs must be created by systemd for the service user."""
        service = load_service()
        assert service["Service"]["StateDirectory"] == "rsi-pi-bot"
        assert service["Service"]["LogsDirectory"] == "rsi-pi-bot"
        # ProtectSystem=full keeps /opt writable (runtime/ + data/tickers.csv);
        # strict would break the default in-repo paths.
        assert service["Service"]["ProtectSystem"] == "full"

    def test_env_example_has_token_and_paths(self):
        content = ENV_EXAMPLE.read_text()
        assert "DISCORD_TOKEN=" in content
        for var in ("TICKERS_FILE=", "DB_PATH=", "LOG_PATH="):
            assert var in content


class TestImportPaths:
    def test_bot_package_importable_with_src_pythonpath(self, tmp_path):
        """Simulates what the systemd unit does: PYTHONPATH=src, import bot.main."""
        env = dict(os.environ)
        env["PYTHONPATH"] = str(REPO_ROOT / "src")
        env["DB_PATH"] = str(tmp_path / "import-check.db")
        env["LOG_PATH"] = str(tmp_path / "import-check.log")
        env.pop("DISCORD_TOKEN", None)

        result = subprocess.run(
            [sys.executable, "-c",
             "import bot.main, bot.services.scheduler, bot.repositories.database; print('ok')"],
            env=env, cwd=str(REPO_ROOT), capture_output=True, text=True, timeout=60,
        )
        assert result.returncode == 0, result.stderr
        assert "ok" in result.stdout

    def test_bot_package_not_importable_without_pythonpath(self, tmp_path):
        """Documents the failure mode the unit fix prevents."""
        env = dict(os.environ)
        env.pop("PYTHONPATH", None)
        env["DB_PATH"] = str(tmp_path / "x.db")
        env["LOG_PATH"] = str(tmp_path / "x.log")

        result = subprocess.run(
            [sys.executable, "-c", "import bot.main"],
            env=env, cwd=str(REPO_ROOT), capture_output=True, text=True, timeout=60,
        )
        assert result.returncode != 0
        assert "ModuleNotFoundError" in result.stderr
