# Smart Money Swap Monitor (RPC Edition)

这个版本不依赖 Dune，直接从链上 RPC 拉数据。

当前默认支持：

- `ethereum`
- `base`
- `bnb`
- `solana`

目标和旧版保持一致：读取 `smart_money_active.csv` 里的地址，定时扫描最近窗口内的新交易，识别 swap / swap-like 行为并发出告警。

## 现在怎么识别

### EVM

- 直接查询 `ERC20 Transfer` 日志
- 按交易哈希聚合一个钱包在同一笔交易里的 token 流入 / 流出
- 如果同一笔交易里同时出现至少一个流出 token 和一个流入 token，就判定为 `swap-like`

这是一种纯 RPC 的启发式判断，不依赖任何索引平台。优点是简单、便宜、可控；缺点是对复杂路由、原生币换币和某些聚合器场景不如专门索引准确。

### Solana

- 直接通过 `getSignaturesForAddress` 拉地址最近签名
- 再用 `getTransaction` 读取交易详情
- 根据 `preTokenBalances` / `postTokenBalances` 计算监控地址的 token 余额变化
- 如果同一笔交易里既有明显流出又有明显流入，就判定为 `swap-like`
- 日志里出现 `swap` / `route` / `raydium` / `jupiter` / `orca` 等关键字时，会提高信号可信度

## CSV 格式

```csv
address_type,address,label,enabled
evm,0x123...,wallet-a,true
sol,93Ny...,wallet-b,true
```

- `address`: 必填
- `address_type`: `evm` 或 `sol`
- `label`: 可选，告警展示名
- `enabled`: 可选，默认启用

兼容旧版导出里常见字段，比如 `name` / `alias` / `last_active`。

## 快速启动

1. 创建虚拟环境并安装依赖

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. 配环境变量

```bash
cp .env.example .env
```

至少要填：

- `ETHEREUM_RPC_URL`
- `BASE_RPC_URL`
- `BNB_RPC_URL`
- `SOLANA_RPC_URL`

3. 修改 `smart_money_active.csv`

4. 启动

```bash
source .venv/bin/activate
python smart_money_monitor.py
```

只跑一轮：

```bash
python smart_money_monitor.py --once
```

## 不在本地运行

这版更适合部署成一个常驻的云端 worker，而不是放在 GitHub Actions 这类无状态定时任务里。

原因很简单：

- 监控器依赖 `monitor_state.json` 记录上次扫描进度
- 如果运行环境每次都是全新的，容易重复告警，或者为了防漏单又不得不扩大回看窗口

### 推荐方式

推荐把它部署到支持 Docker 的平台上，例如：

- Railway
- Render
- Fly.io
- 你自己的云主机 / VPS

这个目录已经带了 [Dockerfile](/Users/wj/Documents/smart_money_tracker_rpc/Dockerfile:1)，可以直接作为一个后台 worker 跑起来。

### 部署时要点

1. 把这个目录推到 GitHub 仓库
2. 在部署平台创建一个基于 Docker 的 worker service
3. 配置环境变量
4. 给状态文件挂一个持久化目录

建议把下面两个变量改到持久卷里：

```env
STATE_FILE=/data/monitor_state.json
ALERT_LOG_FILE=/data/alerts.log
```

只要 `/data` 是持久化挂载，服务重启后就不会丢监控进度。

### Railway 部署

如果我们先用 Railway 验证稳定性，建议这样配：

1. 把这个目录推到 GitHub 仓库
2. 在 Railway 创建一个 `Persistent Service`
3. 让它从这个仓库部署，Railway 会直接识别 [Dockerfile](/Users/wj/Documents/smart_money_tracker_rpc/Dockerfile:1)
4. 给这个 service 挂一个 Volume
5. 把 Volume 的 mount path 设成 `/data`
6. 在 Variables 里填入 RPC 和告警相关环境变量

这个项目已经带了 [start.sh](/Users/wj/Documents/smart_money_tracker_rpc/start.sh:1)，会自动读取 Railway 提供的 `RAILWAY_VOLUME_MOUNT_PATH`。
如果挂了 Volume 而且没有手动设置 `STATE_FILE` / `ALERT_LOG_FILE`，它会自动落到：

- `.../monitor_state.json`
- `.../alerts.log`

也就是说，在 Railway 上通常不需要你自己再写这两个路径。

建议至少配置这些变量：

```env
ETHEREUM_RPC_URL=...
BASE_RPC_URL=...
BNB_RPC_URL=...
SOLANA_RPC_URL=...
SMART_MONEY_CSV=/app/smart_money_active.csv
POLL_INTERVAL_SECONDS=3600
```

如果你要告警，再额外配置：

```env
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
```

按 Railway 官方文档，Volume 会在你指定的 mount path 以目录形式提供给服务，并且运行时会自动注入 `RAILWAY_VOLUME_MOUNT_PATH` 变量；Variables 也会在构建和运行时提供给服务。

### Docker 本地构建命令

如果你只是想验证镜像能启动，可以用：

```bash
docker build -t smart-money-tracker-rpc .
docker run --env-file .env smart-money-tracker-rpc
```

如果是正式部署，记得把 `STATE_FILE` 和 `ALERT_LOG_FILE` 指到平台的持久卷路径。

## 输出

默认会：

- 在终端打印告警
- 追加写入 `alerts.log`

可选推送：

- Telegram
- Slack / Discord / 自定义 Webhook

## 状态文件

状态保存在 `monitor_state.json`，主要包括：

- EVM 每条链最后扫到的区块
- 已经告警过的交易 ID
- 最近一次全局检查时间

这样可以避免重复告警。

## 当前边界

- 这是一个“纯 RPC MVP”，重点是先脱离 Dune 跑起来
- EVM 目前主要依赖 ERC20 `Transfer` 事件做启发式识别
- Solana 目前主要依赖 token balance delta
- 原生币参与的 swap、复杂多跳路由、某些无标准事件协议，可能只会被识别成较粗的 `swap-like`

如果要继续提高精度，下一步最值得做的是：

1. 加入常见 Router / Aggregator 的方法签名识别
2. 给常见代币做 symbol / decimals 本地缓存
3. 为 Solana 接入 Jupiter / Raydium / Orca 的程序级解析
