#!/usr/bin/env python3
"""로컬 DAG 개발용 Airflow 스케줄러와 웹 UI를 실행한다."""

from __future__ import annotations

import argparse
import os
import re
import secrets
import shutil
import socket
import subprocess
import sys
from pathlib import Path

from src.common.utils.config import init_runtime_config, repo_root, resolve_repo_path

DEFAULT_ADMIN_USERNAME = "admin"
DEFAULT_WEBSERVER_PORT = 8080


def port_in_use(port: int, host: str = "127.0.0.1") -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.5)
        return sock.connect_ex((host, port)) == 0


def build_airflow_env(config_path: str) -> dict[str, str]:
    init_runtime_config(config_path)
    env = os.environ.copy()
    root = repo_root()

    airflow_home = resolve_repo_path(env.get("AIRFLOW_HOME", "data/airflow"))
    dags_folder = resolve_repo_path(env.get("AIRFLOW__CORE__DAGS_FOLDER", "src"))

    env["PYTHONPATH"] = str(root) + (f":{env['PYTHONPATH']}" if env.get("PYTHONPATH") else "")
    env["AIRFLOW_HOME"] = str(airflow_home)
    env["AIRFLOW__CORE__DAGS_FOLDER"] = str(dags_folder)
    env.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
    env.setdefault("AIRFLOW__CORE__DEFAULT_TIMEZONE", env.get("TZ", "Asia/Seoul"))
    env.setdefault(
        "AIRFLOW__WEBSERVER__DEFAULT_UI_TIMEZONE",
        env.get("AIRFLOW__CORE__DEFAULT_TIMEZONE", "Asia/Seoul"),
    )
    env.setdefault("AIRFLOW__WEBSERVER__WORKERS", "1")
    env.setdefault("AIRFLOW__WEBSERVER__SESSION_BACKEND", "database")
    env.setdefault("AIRFLOW__WEBSERVER__WEB_SERVER_PORT", str(DEFAULT_WEBSERVER_PORT))

    secret_file = airflow_home / ".webserver_secret_key"
    if secret_file.is_file():
        env.setdefault(
            "AIRFLOW__WEBSERVER__SECRET_KEY",
            secret_file.read_text(encoding="utf-8").strip(),
        )
    else:
        secret_key = secrets.token_urlsafe(32)
        airflow_home.mkdir(parents=True, exist_ok=True)
        secret_file.write_text(secret_key, encoding="utf-8")
        env.setdefault("AIRFLOW__WEBSERVER__SECRET_KEY", secret_key)

    return env


def _apply_dotenv(path: Path) -> None:
    """repo ``.env``를 읽어 아직 없는 키만 ``os.environ``에 넣는다."""
    if not path.is_file():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def admin_credentials() -> tuple[str, str]:
    username = os.environ.get("AIRFLOW_ADMIN_USERNAME", DEFAULT_ADMIN_USERNAME).strip() or (
        DEFAULT_ADMIN_USERNAME
    )
    password = os.environ.get("AIRFLOW_ADMIN_PASSWORD", "").strip()
    if not password:
        raise RuntimeError(
            "AIRFLOW_ADMIN_PASSWORD is required. "
            "Set it in .env or export AIRFLOW_ADMIN_PASSWORD."
        )
    return username, password


def verify_admin_password(env: dict[str, str], *, username: str, password: str) -> bool:
    prev = os.environ.copy()
    os.environ.update(env)
    try:
        from werkzeug.security import check_password_hash
        from airflow.www.app import cached_app

        app = cached_app()
        with app.app_context():
            user = app.appbuilder.sm.find_user(username)
            if user is None or not user.password:
                return False
            return check_password_hash(user.password, password)
    finally:
        os.environ.clear()
        os.environ.update(prev)


def ensure_admin_user(env: dict[str, str], *, username: str, password: str) -> None:
    root = repo_root()
    password_path = Path(env["AIRFLOW_HOME"]) / "standalone_admin_password.txt"
    password_path.parent.mkdir(parents=True, exist_ok=True)
    password_path.write_text(f"{password}\n", encoding="utf-8")

    listed = subprocess.run(
        ["airflow", "users", "list", "-o", "plain"],
        env=env,
        capture_output=True,
        text=True,
        cwd=root,
    )
    if re.search(rf"^\s*\d+\s+{re.escape(username)}\s", listed.stdout or "", re.MULTILINE):
        subprocess.run(
            ["airflow", "users", "delete", "-u", username],
            env=env,
            check=True,
            cwd=root,
        )
    subprocess.run(
        [
            "airflow",
            "users",
            "create",
            "-u",
            username,
            "-p",
            password,
            "-f",
            "Admin",
            "-l",
            "User",
            "-r",
            "Admin",
            "-e",
            f"{username}@example.com",
        ],
        env=env,
        check=True,
        cwd=root,
    )
    if not verify_admin_password(env, username=username, password=password):
        raise RuntimeError(
            f"Admin user {username!r} was created but password verification failed"
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="InvestBot DAG용 Airflow 웹 UI를 실행한다")
    parser.add_argument(
        "--config",
        default="configs/dev/debug.json",
        help="런타임 설정 파일 경로 (기본값: configs/dev/debug.json)",
    )
    args = parser.parse_args(argv)

    _apply_dotenv(repo_root() / ".env")

    if shutil.which("airflow") is None:
        print(
            "error: airflow를 찾을 수 없습니다. `pip install -r requirements.txt`로 설치하세요.",
            file=sys.stderr,
        )
        return 1

    env = build_airflow_env(args.config)
    airflow_home = Path(env["AIRFLOW_HOME"])
    dags_folder = Path(env["AIRFLOW__CORE__DAGS_FOLDER"])
    airflow_home.mkdir(parents=True, exist_ok=True)
    username, password = admin_credentials()
    web_port = int(env.get("AIRFLOW__WEBSERVER__WEB_SERVER_PORT", DEFAULT_WEBSERVER_PORT))

    if port_in_use(web_port):
        print(
            f"error: 포트 {web_port}가 이미 사용 중입니다. 기존 Airflow 프로세스를 먼저 종료하세요.",
            file=sys.stderr,
        )
        return 1

    root = repo_root()
    print("==> InvestBot Airflow UI")
    print(f"    config:       {args.config}")
    print(f"    repo:         {root}")
    print(f"    DAGs 폴더:    {dags_folder}")
    print(f"    AIRFLOW_HOME: {airflow_home}")
    print(f"    웹 UI:        http://localhost:{web_port}")
    print(f"    로그인:       {username} (AIRFLOW_ADMIN_PASSWORD)")
    print()

    print("==> Airflow 메타데이터 DB 마이그레이션 확인 중...")
    subprocess.run(["airflow", "db", "migrate"], env=env, check=True, cwd=root)

    print("==> Airflow 관리자 계정 확인 중...")
    ensure_admin_user(env, username=username, password=password)

    print("==> DAG import 확인 중...")
    listed = subprocess.run(
        ["airflow", "dags", "list"],
        env=env,
        capture_output=True,
        text=True,
        cwd=root,
    )
    if listed.returncode != 0:
        print(listed.stderr or listed.stdout, file=sys.stderr)
        return listed.returncode
    if "dag_market" not in (listed.stdout or ""):
        print(
            "warning: `airflow dags list` 출력에서 dag_market을 찾지 못했습니다.",
            file=sys.stderr,
        )
    else:
        print("    dag_market 확인됨")

    print("==> 스케줄러와 웹서버를 시작합니다 (airflow standalone)...")
    print()

    os.chdir(root)
    os.environ.update(env)
    os.execvp("airflow", ["airflow", "standalone"])


if __name__ == "__main__":
    raise SystemExit(main())
