#!/usr/bin/env bash
# ============================================================
#  Apple Health Grafana — 一键部署脚本
#  用法:  curl -fsSL <raw_url>/deploy.sh | bash
#  或者:  bash deploy.sh
# ============================================================
set -euo pipefail

REPO_URL="https://github.com/cuijianzhuang/apple-health-grafana.git"
INSTALL_DIR="${INSTALL_DIR:-$HOME/apple-health-grafana}"

GREEN='\033[0;32m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

# ---------- 1. 检查依赖 ----------
info "检查运行环境 …"

command -v docker   >/dev/null 2>&1 || error "未检测到 docker，请先安装: https://docs.docker.com/engine/install/"
command -v docker   >/dev/null 2>&1 && docker compose version >/dev/null 2>&1 || \
  command -v docker-compose >/dev/null 2>&1 || error "未检测到 docker compose，请先安装 Docker Compose v2"

COMPOSE_CMD="docker compose"
if ! docker compose version >/dev/null 2>&1; then
  COMPOSE_CMD="docker-compose"
fi

# ---------- 2. 克隆/更新仓库 ----------
if [ -d "$INSTALL_DIR/.git" ]; then
  info "项目目录已存在，拉取最新代码 …"
  cd "$INSTALL_DIR"
  git pull --ff-only || warn "git pull 失败，将使用本地现有代码"
else
  info "克隆仓库到 $INSTALL_DIR …"
  git clone "$REPO_URL" "$INSTALL_DIR"
  cd "$INSTALL_DIR"
fi

# ---------- 3. 环境变量 ----------
if [ ! -f .env ]; then
  if [ -f .env.example ]; then
    cp .env.example .env
    info "已从 .env.example 生成 .env，如需自定义请编辑 $INSTALL_DIR/.env"
  fi
fi

# ---------- 4. 构建并启动 ----------
info "构建镜像并启动服务 …"
$COMPOSE_CMD build --pull
$COMPOSE_CMD up -d

# ---------- 5. 输出信息 ----------
GRAFANA_PORT=$(grep -E '^GRAFANA_PORT=' .env 2>/dev/null | cut -d= -f2 || echo "3000")
GRAFANA_PORT=${GRAFANA_PORT:-3000}
API_PORT=$(grep -E '^API_PORT=' .env 2>/dev/null | cut -d= -f2 || echo "5353")
API_PORT=${API_PORT:-5353}

LOCAL_IP=$(hostname -I 2>/dev/null | awk '{print $1}' || echo "localhost")

echo ""
info "========================================"
info "  部署完成！"
info "========================================"
echo ""
info "Grafana 仪表板:  http://${LOCAL_IP}:${GRAFANA_PORT}"
info "  默认账号: admin / health"
echo ""
info "Health Auto Export API:"
info "  POST http://${LOCAL_IP}:${API_PORT}/api/healthautoexport"
info "  健康检查: http://${LOCAL_IP}:${API_PORT}/health"
echo ""
info "在 Health Auto Export App 中配置:"
info "  1. 自动化 → 添加 → REST API"
info "  2. URL 填: http://${LOCAL_IP}:${API_PORT}/api/healthautoexport"
info "  3. 格式选 JSON"
echo ""
info "管理命令:"
info "  cd $INSTALL_DIR"
info "  $COMPOSE_CMD logs -f        # 查看日志"
info "  $COMPOSE_CMD restart        # 重启服务"
info "  $COMPOSE_CMD down           # 停止服务"
info "  $COMPOSE_CMD up -d --build  # 重新构建并启动"
echo ""
