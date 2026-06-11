# M8 — 会话生命周期、重连续传与重放去重（设计）

状态：**已实现**。

对应 spec `session-management`，设计决策 **D13**（诚实抖动处理：单连接内
TCP 有序，不做会话内重排窗口）。任务：8.1–8.4。

## 1. 核心模型：会话与连接解耦

M7 里"会话 = 连接"，断线即对话蒸发。M8 把二者拆开：

```
Session（持久，跨断线存活）            attachment（每连接，随连接生灭）
├─ pipeline（含对话历史！）            ├─ reader：去重 + 内联 VAD + 入管
├─ VAD controller                     ├─ writer：握手回执 + 下行帧 + 心跳
├─ wire 下行队列                       └─ epoch：握手时声明，接管即作废前任
├─ 下行采样时钟 / 下行 seq（连续）
└─ 上行去重水位 / 计数器
```

短暂断网后浏览器自动重连，**同一段对话原地继续**——对话历史在 pipeline 里，
pipeline 在 Session 里，Session 不死。`IdleTimeout`（默认 60s）同时充当
重连宽限窗：超时无任何帧往来才回收。

## 2. 握手协议（8.1 / 8.3）

复用 `CONTROL START` 帧作握手，`ControlPayload` 增加 `session_id`/`epoch`/
`last_seq` 三个字段（proto3 加字段向后兼容）：

```
客户端 → START { session_id: ""|旧id, epoch: 上次+1（新会话=1）, last_seq: 已收下行seq }
服务端 → START { session_id: 权威id, epoch: 采纳值, last_seq: 上行去重水位, detail: created|resumed }
服务端 → ERROR { detail } + 关连接        （陈旧 epoch 声明被拒）
```

- **epoch 单调递增，声明必须严格大于当前值**。等于或小于 = 陈旧重连
  （僵尸客户端 / 旧连接抢跑），回 ERROR 并计数（`StaleEpochClaims`）。
  接管成功时，前一个 attachment 被标记 stale：其在途残帧只计数不入管
  （`staleFrames`），连接随即关闭——旧会话残帧污染新会话在结构上不可能。
- **epoch 绑定在连接上，不在每帧上**。WS 下帧只能从某条连接到达，连接在
  握手时就定了 epoch——逐帧带 epoch 是为无连接传输（未来 WebTransport
  datagram）准备的，现在加只是虚胖（D13 诚实原则）。
- **过期 id 续连 = 静默开新会话**（ack 带新 id + detail 提示），不是报错：
  会话超时后用户重连，期望的是"重新开始对话"而非一条错误。

## 3. 续传与去重（8.2 / 8.3）

两个方向语义刻意不对称：

- **上行（麦克风）**：客户端重连后可重放未确认帧；服务端用**单调水位**
  `uplinkFloor`（已交付的最高 seq，CAS-max 推进）去重——`seq <= 水位` 即重复，
  计数丢弃（`dupFrames`）。ack 的 `last_seq` 告知水位，客户端可跳过已确认部分。
  **不需要去重窗口**：TCP 保证单连接内有序 + 客户端按序重放 ⇒ 水位即充分。
  原计划的 `dedup_window` 配置项因此删除——比计划更简单时，删掉计划的复杂度。
- **下行（合成音频）**：**不重传**。实时语音里重放断线期间的旧音频是反特性
  ——那个时刻已经过去了。下行 seq 跨重连连续递增（客户端可感知断档），
  断线期间 wire 队列满后由出口环 drop-oldest 吸收，重连后从"现在"继续。
  spec 的"不重复交付已确认帧"由上行水位去重满足。

## 4. 生命周期与回收（8.1 / 8.4）

- **创建**：首个 START；id = `crypto/rand` 128-bit hex，epoch 从声明采纳。
- **活跃**：任一方向有帧即 `touch()`（atomic 时间戳）。
- **空闲回收**：Manager 的 reaper 以 `IdleTimeout/4`（夹在 10ms–1s）周期扫描，
  `now - lastActive > IdleTimeout` 即回收；`CONTROL STOP` 立即回收整个会话
  （不只是连接）；服务器停机回收全部。
- **资源全释放**：回收 = 撤销会话级 ctx。pipeline 的全部 goroutine 退出
  （M5 已验证）、attachment 的 reader/writer 是该 ctx 的子 ctx 一并退出、
  环形缓冲随 Session 被 GC、连接显式关闭。锁纪律：Manager.mu 管映射表、
  Session.mu 管 attach/epoch，只允许 Manager→Session 方向嵌套，无反向取锁。

## 5. 浏览器侧配合

`web/app.js`：握手编码（手写 protobuf varint 编码器，~15 行）、START 回执
存 id/epoch、断线自动重连（600ms 间隔，最多 8 次，epoch+1 接管），采集链
（mic/AudioContext/worklet）在重连间隙保持存活，只重建 WS——断网几秒内
恢复对话无感。收到 ERROR 控制帧则终止。

## 6. 验收 / 测试（8.4）

全部 `-race` 三连干净（`session_test.go`，真实 WS 栈 + 合成客户端）：

- 握手建会话（id/epoch/计数正确）；M7 的对话+打断验收在握手协议上重跑通过。
- **重连续传**：突断（无 STOP）→ 同 id + epoch 2 重连 → ack `resumed`、
  `last_seq` = 上行水位、下行 seq 严格继续不重置、会话数仍为 1。
- **重放去重**：重连后重放 seq 4–8（≤ 水位 8）+ 新发 9–11 → `dupFrames == 5`
  精确命中，新帧正常交付。
- **陈旧 epoch**：僵尸连接声明 epoch 1（非递增）→ 收 ERROR、计数 +1、
  在线客户端完全不受扰动。
- **过期会话**：未知 id 续连 → 新 id、epoch 1、不报错。
- **长稳无泄漏**：25 个会话批量「握手→说话→完整一轮→突断」，reaper 以
  200ms 空闲超时全部回收 → 会话数归零、goroutine 数回落基线（±5）。
  RSS 维度依赖 goroutine/缓冲归零间接保证，M10 压测时用进程指标复核。

## 7. 已知取舍（如实记录）

- 接管瞬间旧 writer 手上可能持有一帧，写向已死连接即丢——实时音频语义下
  正确（那一刻已过去），字幕帧丢失影响可忽略。
- 客户端不维护上行重放缓冲：麦克风永远在产生更新鲜的帧，重放旧语音无意义；
  服务端去重逻辑独立完整（Go 测试合成重放验证），不依赖客户端行为。
- Redis 外置会话路由是可选外设（任务 12.1，默认关闭）：本里程碑会话状态
  全部进程内，热路径零外部存储调用——spec 的"关闭 Redis 仍可运行"天然满足。

## 参考

- Spec：`openspec/changes/streaming-multimodal-agent-engine/specs/session-management/spec.md`
- 设计决策：D13（单连接内不做重排窗口；去重仅限重连重放场景）。
- 消费：M5 pipeline（跨重连存活的对话本体）、M7 接线（握手嵌入其 reader/writer 模型）。
- 被消费：M9 指标（dup/stale/subtitleDrops 计数器现成可导出）、M10 压测
  （批量会话生命周期即负载发生器的会话模型）。
