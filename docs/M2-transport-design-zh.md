# M2 — 帧协议与 WebSocket 传输（设计）

状态：**已实现** · 决策：**方案B**（通过protoc工具链的真实protobuf负载）。

> 实现备注：下行步调在M5推迟（回环中没有真实下行）；`TCP_NODELAY` 被验证（Go默认），不手动设置。Windows上的竞态检测需要现代MinGW-w64（gcc ≥ 13；已在16.1.0上验证）——更早的8.x版本无法启动竞态运行时（`0xc0000139`）。

对应规格`streaming-transport`和设计决策**D1, D7, D12, D13**。
任务：2.1–2.7。

## 1. 目的与交付物

M2构建**线路层**（内核的底部）——分三个子层：

```
③ Conn抽象（传输无关）         ReadFrame / WriteFrame / Close   ← 上层仅使用此接口
② WebSocket端点                coder/websocket，二进制消息      ← v1的唯一实现
① 帧编解码                    固定二进制头 + 负载               ← 手写协议
```

**交付物 = 一个回环循环**：服务器接受WS连接，读一个Frame，原样写回。这涵盖传输的每个部分（升级、解码、编码、双向流）。一旦回环能字节对字节往返，传输层就被验证了；M3–M5后期用真实管道替换"回环"。M2后进程保持**驻留，监听:8080**（不像M1脚手架那样立即退出）。

## 2. 帧格式 — 固定二进制头+类型化负载（混合）

```
 偏移   大小  字段      类型        原理
 ──────────────────────────────────────────────────────────────────────
 0      2     magic     0x56 0x53   同步 / 完整性检查；快速失败垃圾数据
 2      1     version   uint8       拒绝不兼容的协议版本
 3      1     type      uint8       解复用：AUDIO / TEXT / CONTROL
 4      8     seq       uint64 BE   单调；重放去重 + (未来UDP)重排检测
 12     8     ts_us     int64  BE   采样时钟PTS — 音频/token对齐(D7)
 20     4     length    uint32 BE   负载大小；边界检查（拒绝>maxPayload，如64KB）
 ──────────────────────────────────────────────────────────────────────
 24     ...   payload   bytes       AUDIO=原始PCM；TEXT/CONTROL=protobuf
```

24字节固定头，**大端字节序**（网络字节序）。

**为什么是这样的形式：**

- **固定偏移 → 零分配解析。** 解码是 `binary.BigEndian.Uint64(buf[4:12])` 风格的索引读取；无对象分配，无反射。服务于零分配音频热路径目标。
- **magic + version + length = "不信任线路上的数据。"** 直接映射到规格场景：垃圾数据快速死亡（magic），不兼容版本被拒绝（version），残缺/超大头不会造成越界读（length边界检查）。
- **按类型分割负载。** 音频是高频（50fps）不透明字节——用protobuf包装它会为无结构收益而烧CPU/分配，所以音频负载是**原始PCM**。TEXT/CONTROL是低频且演进——它们使用**protobuf**（紧凑、模式演进、代码生成）。正确选择每一层。

### 为什么在WebSocket已经分帧的时候还要自己分帧？

WebSocket二进制消息已经携带长度和边界，所以在WS上我们的`length`字段看起来冗余。我们无论如何都保留自己的分帧：

1. **传输独立性(D12/D13)。** 头必须在WebTransport数据报和原始TCP上工作——**无消息边界**的字节流传输。依赖WS分帧会把我们锁定在WS。自分帧是便携的。
2. **多路复用。** 一条WS连接携带三条逻辑流（音频/文本/控制）；`type` + `seq` 解复用它们。WS不知道我们的逻辑流。

头为最坏情况（字节流）而设计，仍在简单情况（WS消息）上工作。v1使用**1 Frame = 1 WS二进制消息**（无批处理）。

## 3. 负载编码 — protobuf（方案B）

TEXT和CONTROL负载是用`protoc` + `protoc-gen-go`编译的protobuf消息。音频负载是原始PCM字节（头的`length`限定它）。`.proto`定义帧`type`枚举和TEXT/CONTROL消息体（如部分/最终转录、token、开始/停止/打断控制）。

> 工具链前提条件：`protoc`（编译器）+ `protoc-gen-go`（Go插件）必须安装；见"开放项"。

## 4. Conn 抽象（D12 接缝）

```go
// 上层(管道/会话)仅依赖此；它们永不导入websocket。
type Conn interface {
    ReadFrame(ctx context.Context) (Frame, error)
    WriteFrame(ctx context.Context, f Frame) error
    Close(code StatusCode, reason string) error
}
```

`wsConn`在coder/websocket上实现`Conn`；未来的`wtConn`在WebTransport上实现**相同**接口——交换传输不触及上层代码。`ctx`在读/写上启用取消（打断、关闭），这是为什么选择coder/websocket（基于context的API）而非gorilla。

## 5. 连接goroutine模型

coder/websocket允许一个并发读者和一个并发写者，所以每连接使用**1个读goroutine + 1个写goroutine**：

```
┌── 读goroutine ──┐                 ┌── 写goroutine ──┐
│ ReadFrame       │   ...管道       │ 排干出口        │
│  └→ (M4+)内联VAD│   (M2中回环)    │  └→ WriteFrame  │
│  └→ 入口环      │                 │                 │
└─────────────────┘                 └─────────────────┘
```

M2使用最简单的回环（读→写）。读/写分割是M3–M5中携带的形状，其中中间成为真实管道。

## 6. 心跳与低延迟旋钮

- **心跳(2.5)：** 使用WebSocket内置**ping/pong**检活；心跳超时 → 向会话层发送断开事件(M8)。我们的CONTROL帧通道保留用于应用语义（开始/停止/打断），不检活。
- **禁用压缩(2.6)：** 通过coder/websocket`AcceptOptions`关闭permessage-deflate（压缩增加小实时帧的延迟/CPU）。
- **TCP_NODELAY(2.6)：** Go的`net/http`已默认在接受连接上启用`TCP_NODELAY`——所以这是**验证而非实现**。别重做运行时已做的事。
- **下行步调(2.6)：** 推迟——回环中没有真实下行；步调在真实TTS下行到达时登陆(M5)。

## 7. 验收 / 测试(2.7)

- **单元：** 构建一个Frame(type/seq/ts_us/负载) → 编码 → 解码 → 字段相等+负载字节相等。负面测试：坏magic、不兼容版本、截断/超大头都被拒绝而无越界读。
- **回环：** 启动服务器；`wscat`/浏览器连接，发一个frame，收到相同的字节回送。进程保持驻留监听:8080。

## 8. 开放项

- 实现任务2.1前安装`protoc` + `protoc-gen-go`。
- 定义TEXT/CONTROL的`.proto`消息集（M2中保持最小；在M4/M6特性登陆时扩展）。
- `maxPayload`边界值（默认64KB）——确认vs预期帧大小。

## 参考

- 规格：`openspec/changes/streaming-multimodal-agent-engine/specs/streaming-transport/spec.md`
- 设计决策：D1 (WS优先), D7 (采样时钟PTS), D12 (定位), D13 (低延迟传输&诚实抖动)。
