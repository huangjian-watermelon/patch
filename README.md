# patch

## 配置方式

服务端和客户端都通过 JSON 文件读取运行参数，不再使用启动参数逐项传入：

- 推流程序默认读取 `stream_forwarder.json`
- 补包程序默认读取 `retrans_service.json`
- 客户端默认读取 `ts_request_client.json`

配置解析已统一使用共享 JSON 解析模块（`shared/json_config.*`），不再使用正则抽取字段。

也可以手动指定配置文件路径：

```bash
./patchStreamForwarder /path/to/stream_forwarder.json
./patchRetransServer /path/to/retrans_service.json
./ts_request_client /path/to/ts_request_client.json
```

## patchServer 拆分说明

现在 `patchServer` 被拆分为两个独立程序，可分别拉起：

1. `patchStreamForwarder`：接收 `238.1.1.130:1234` 并转发到 `238.1.1.127:5040`。  
2. `patchRetransServer`：接收 `0.0.0.0:9000` 补包请求，并发补包到 `0.0.0.0:9001`（目的IP沿用请求来源IP，端口为9001）。其缓存输入默认和 `patchStreamForwarder` 一样直接接 `238.1.1.130:1234`。

这样即使补包进程崩溃，推流进程仍可独立运行（反之亦然）。

## 会话切换（session_id）

- `patchStreamForwarder` 每次启动都会生成新的 `session_id`，并跟随每个主流包发送给客户端。
- `ts_request_client` 检测到 `session_id` 变化后，会重置重排和缺包状态并重新起播，避免卡在旧 `expected_seq`。

## 压测与KPI

仓库提供了最小化压测脚本和 KPI 提取脚本：

- `tools/run_chaos_test.sh`：启动服务端/客户端并运行指定秒数，输出日志和 KPI 报告。
- `tools/kpi_from_logs.py`：从日志提取 `missing/request/resent/recovered/timeout` 等指标。
- `tools/netem_profile.sh`：基于 `tc netem` 注入/清理基础丢包、延迟、乱序（需 root）。

示例：

```bash
# 注入网络扰动（需 root）
sudo tools/netem_profile.sh apply eth0

# 跑 120 秒压测
tools/run_chaos_test.sh 120

# 清理网络扰动
sudo tools/netem_profile.sh clear eth0
```
