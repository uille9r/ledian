# ledian

团队局域网 AI 热点选题库。本分支提供 Python 后端、SQLite 存储、采集任务和 JSON API。

## 在 Windows 启动

需要 Python 3.12 或更新版本。用 PowerShell 在仓库目录运行：

```powershell
$env:LEDIAN_INVITE_CODE = Read-Host '输入团队邀请码（至少 8 个字符）'
python -m ledian --host 0.0.0.0 --port 3000
```

- 默认数据文件是 `data/ledian.sqlite3`。重启时继续使用同一个数据文件。
- 先在服务电脑打开 `http://127.0.0.1:3000/api/health`，再用同网设备访问 `http://<服务电脑内网 IP>:3000/api/health`。在路由器为服务电脑保留 DHCP 地址，并在 Windows 专用网络的防火墙中放行所选端口。仅在信任的局域网使用 HTTP。已有 HTTPS 反向代理时可加 `--secure-cookie`。
- 首页地址当前返回 JSON 健康状态。登录、热点卡的「推」按钮和通知弹窗由后续网页调用下面的 API。
- 服务启动即采集，之后每 10 分钟运行一轮。服务电脑关机或休眠时暂停采集，48 小时窗口仍按首次推荐的时间计算。

## 数据源

微博、知乎、抖音、今日头条、澎湃新闻和腾讯新闻使用自托管的 [DailyHotApi](https://github.com/imsyy/DailyHotApi)。默认地址为 `http://127.0.0.1:6688`，可设置 `LEDIAN_HOT_API` 或传 `--hot-api` 修改。若已有 Docker，DailyHotApi 的项目首页提供部署方式。只验证 API 时可先加 `--no-collect`。

微博主源出错时，读取 [hot-search-crawler 的公开微博榜单快照](https://github.com/solarhell/hot-search-crawler#输出结构)。仅使用 45 分钟内的成功快照，可通过 `--weibo-snapshot` 指定其他兼容快照地址。

OpenAI、Google AI、OpenAI／Claude／DeepSeek 状态页以及 Kimi Code 公开版本发布通过 RSS 或 Atom 读取。Grok、即梦和其余产品更新目前从热榜及已配置的新闻订阅发现。其他公开 RSS／Atom 可以通过 `--sources-file` 或 `LEDIAN_SOURCES_FILE` 添加。文件内容为 JSON 数组，每项包含唯一的 `id`、显示用的 `name`、订阅 `url`，以及可选的 `official`。只有团队核对过的一手源才设为官方。当前官方提醒要求原文与订阅源同域，且发布时间在最近 48 小时内。

```powershell
python -m ledian --collect-once --db data/probe.sqlite3
```

命令逐个输出来源的条数或失败原因。单个源失败时，页面仍可通过 API 读取其他来源及各自最近的成功时间。

## API

写入接口使用 JSON。登录后，浏览器通过会话 Cookie 识别成员。空正文的写入操作传 `{}`。

| 方法与路径 | 用途 |
| --- | --- |
| `GET /api/health` | 检查服务是否在线 |
| `POST /api/register` | 传 `invite`、`username`、`password` 进行注册 |
| `POST /api/login` | 传 `username`、`password` 登录，返回会话 Cookie |
| `GET /api/me`、`POST /api/logout` | 查看成员身份、退出登录 |
| `GET /api/sources` | 查看各来源的采集状态和最近成功时间 |
| `GET /api/watch`、`PUT /api/watch` | 查看或更新共享关注名单，值是「产品名 → 别名数组」 |
| `GET /api/events` | 搜索与筛选事件 |
| `GET /api/events/{id}` | 查看事件、来源链接、推荐人数及追踪状态 |
| `POST /api/events/{id}/push` | 推荐事件并在首次推荐时启动追踪 |
| `POST /api/events/{id}/merge` | 传另一条事件的 `event_id`，将其关联到当前卡 |
| `GET /api/notifications` | 获取当前成员的通知，可设 `after` 为上一条通知 ID、`unread=1` 只取未读、`wait=20` 等待新通知 |
| `POST /api/notifications/{id}/read` | 标记当前成员的通知为已读 |

事件查询支持 `q`（空格分隔多个词）、`scope=recent|qualified|saved`、`category=all|product|industry`、`source`、`since`、`until`、`tracking=active|ended`、`limit` 和 `offset`。时间以 Unix 秒或带时区的 ISO 时间表示，`until` 不含结束时刻。近期与新入围默认查最近 7 天，已入库默认查全部。

自动归并使用相同原文链接、相同规范化标题，或含相同模型版本和事件动作的相近标题。重复标题和共同原文不会单独构成两个独立信号。连续两轮升温的初版阈值为每轮至少上升 3 名。48 小时追踪在常规采集结果中优先关联同一事件的条目，覆盖程度取决于各来源进入榜单或订阅的内容。

## 验证

```powershell
python -m unittest discover -s tests -v
```

测试覆盖本机 HTTP 注册登录、并发推荐去重、未读状态持久化、多个来源的失败隔离、官方提醒去重、搜索和 48 小时到期。上线时用两台设备联调固定地址、登录和即时通知，再在实际网络中验证各数据源。
