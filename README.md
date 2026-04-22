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

说明：`patchStreamForwarder` 和 `patchRetransServer` 现在会忽略终端 `SIGINT/SIGHUP`，避免你在同一终端结束 `ts_request_client` 时把服务端一起停掉。需要停止服务端时请单独 `kill -TERM <pid>`。

跨机器部署（上游服务器运行 `patchStreamForwarder` + `patchRetransServer`，下游机器运行 `ts_request_client`）时，请至少确认：

1. `ts_request_client.json` 里的 `server_ip` 改为上游 `patchRetransServer` 的可达 IP（不要用 `127.0.0.1`）。
2. 下游机器能通过交换机加入主流组播 `stream_mcast_ip:stream_port`（默认 `238.1.1.127:5040`）。
3. 上游机器放行 UDP `9000/9001`，确保补包请求和补包数据可达。
4. 现在 `StreamPacket` 的 `session_id/seq` 已统一使用网络字节序发送，跨主机/跨端序系统也能正确解析。
5. 若同时有大量客户端请求补包，可在 `retrans_service.json` 调大 `req_rcvbuf_bytes`，减少 UDP 请求排队溢出。

`ts_request_client.json` 里还支持补包容错参数，避免 `patchRetransServer` 异常时卡住播放：

- `retrans_enabled`：是否启用补包（`0`=关闭，主流缺包直接跳过，`1`=启用补包）。
- `retrans_retry_interval_ms`：补包重试间隔。
- `retrans_total_timeout_ms`：单个缺包最多等待时间，到时后直接跳过继续播。
- `retrans_max_retry_count`：单个缺包最大重试次数。

## patchServer 拆分说明

现在 `patchServer` 被拆分为两个独立程序，可分别拉起：

1. `patchStreamForwarder`：接收 `238.1.1.130:1234` 并转发到 `238.1.1.127:5040`。  
2. `patchRetransServer`：接收 `0.0.0.0:9000` 补包请求，并发补包到 `0.0.0.0:9001`（目的IP沿用请求来源IP，端口为9001）。其缓存输入来自 `patchStreamForwarder` 的输出流（默认 `238.1.1.127:5040`，即 `StreamPacket` 流）。

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
