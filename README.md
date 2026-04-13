# patch

## 配置方式

服务端和客户端都通过 JSON 文件读取运行参数，不再使用启动参数逐项传入：

- 服务端默认读取 `patch_server.json`
- 客户端默认读取 `ts_request_client.json`

也可以手动指定配置文件路径：

```bash
./patchServer /path/to/patch_server.json
./ts_request_client /path/to/ts_request_client.json
```
