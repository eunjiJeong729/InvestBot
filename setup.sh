#!/usr/bin/env bash
set -euo pipefail

PROFILE="${1:-}"
if [[ "$PROFILE" != "dev" && "$PROFILE" != "test" && "$PROFILE" != "prod" ]]; then
  echo "Usage: $0 {dev|test|prod}" >&2
  exit 1
fi

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO_DIR"
echo "[setup.sh] profile=${PROFILE} repo=${REPO_DIR}"

if [[ "$PROFILE" == "dev" ]]; then
  echo "[setup.sh] dev profile: devcontainer 환경 가정, Docker/Claude 설치 생략"
  export INVESTBOT_CONFIG="configs/dev/debug.json"
  export INVESTBOT_PROFILE="dev"
  pip install --no-cache-dir -r requirements-dev.txt
  echo "[setup.sh] dev 준비 완료. INVESTBOT_CONFIG=${INVESTBOT_CONFIG}"
  exit 0
fi

_install_docker_if_missing() {
  if command -v docker &>/dev/null; then
    echo "[setup.sh] Docker 이미 설치됨, 스킵"
    return
  fi
  # shellcheck disable=SC1091
  . /etc/os-release
  case "$ID" in
    amzn)
      sudo dnf update -y
      sudo dnf install -y docker
      sudo systemctl enable --now docker
      COMPOSE_DIR="/usr/local/lib/docker/cli-plugins"
      sudo mkdir -p "$COMPOSE_DIR"
      sudo curl -SL "https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64" \
        -o "${COMPOSE_DIR}/docker-compose"
      sudo chmod +x "${COMPOSE_DIR}/docker-compose"
      ;;
    ubuntu|debian)
      sudo apt-get update -y
      sudo apt-get install -y ca-certificates curl gnupg
      sudo install -m 0755 -d /etc/apt/keyrings
      curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
      sudo chmod a+r /etc/apt/keyrings/docker.gpg
      echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
        | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
      sudo apt-get update -y
      sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
      sudo systemctl enable --now docker
      ;;
    *)
      echo "[setup.sh] 지원하지 않는 OS(${ID})" >&2
      exit 1
      ;;
  esac
  sudo usermod -aG docker "$USER" || true
}

_install_claude_code_if_missing() {
  if command -v claude &>/dev/null; then
    echo "[setup.sh] Claude Code 이미 설치됨, 스킵"
    return
  fi
  if ! command -v npm &>/dev/null; then
    # shellcheck disable=SC1091
    . /etc/os-release
    if [[ "$ID" == "amzn" ]]; then
      curl -fsSL https://rpm.nodesource.com/setup_lts.x | sudo bash -
      sudo dnf install -y nodejs
    else
      curl -fsSL https://deb.nodesource.com/setup_lts.x | sudo -E bash -
      sudo apt-get install -y nodejs
    fi
  fi
  sudo npm install -g @anthropic-ai/claude-code
}

_install_docker_if_missing
_install_claude_code_if_missing

if [[ "$PROFILE" == "test" ]]; then
  export INVESTBOT_CONFIG="configs/test/ec2.json"
  export INVESTBOT_PROFILE="test"
else
  export INVESTBOT_CONFIG="configs/prod/ec2.json"
  export INVESTBOT_PROFILE="prod"
fi

echo "[setup.sh] docker compose 빌드 및 기동 (profile=${PROFILE})"
sudo docker compose -f docker/docker-compose.yml -f docker/docker-compose.prod.yml up --build -d
echo "[setup.sh] ${PROFILE} 환경 준비 완료. INVESTBOT_CONFIG=${INVESTBOT_CONFIG}"
