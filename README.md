# 灾区空中通信任务控制器

盐边县泥石流灾害场景下的后端控制器：以**仅追加事件日志**为唯一事实来源，
管理灾害区域版本、无人机能力与油电状态、起飞许可、航线、空中基站覆盖、
地面队伍需求与救援优先级，支撑翼龙无人机在核心空域侦察与应急通信保障之间
受控切换，并在链路时断时续、中心重启、离线补传与重复遥测并存的环境下保持
**同一任务事实**。

## 核心保证

- **断链不造事实**：序号缺口先缓冲、补传按序释放；重复事件幂等；迟到旧序号拒绝。
- **阶段防虚假**：只有带设备序号与发生时间的显式阶段事件能推进阶段，遥测/重试/补传不能。
- **签署计划离线可用**：中心重启后从日志完整重建，现场继续执行最后一份已激活计划。
- **受控改派**：覆盖下降、空域冲突、人员位置更新、次生灾害自动生成改派提案并钉死航线，
  必须由对应岗位（NETWORK_OPS / AIRSPACE / GROUND_TEAM / COMMANDER）确认才生效。
- **抢占必留补偿**：高优先级通话可抢占普通容量，被抢占者进入补偿队列，恢复后优先回放。
- **人工指令优先**：FREEZE_CAPACITY 冻结自动抢占；FORCE 保护通话、豁免许可、强制接入，
  自动策略只能读取、永不覆盖。
- **事后可还原**：输入一次通话或侦察成果，还原当时飞行位置、覆盖能力、抢占决定与责任链。
- **防篡改**：日志链式 sha256 摘要，重放逐行校验。

## 目录

- `cmd/server/` 服务入口（`DATABASE_PATH` 配置事件日志路径）。
- `internal/domain/` 事件信封、受控词汇、载荷结构与规范化摘要。
- `internal/store/` 仅追加 JSONL 日志（链式摘要、fsync、重放校验）与内存实现。
- `internal/engine/` 摄入幂等、序号缓冲、阶段状态机、改派、抢占补偿、人工指令、
  事后还原与只读快照。
- `internal/command/` 事件信封构造辅助（自动计算摘要）。
- `internal/service/` HTTP 边界。
- `contracts/` 外部交换字段示例。
- `docs/domain.md` 完整领域约定。

## HTTP 接口

| 方法与路径 | 说明 |
| --- | --- |
| `POST /v1/events/ingest` | 提交事件信封（唯一写入入口）。200 生效/重复，202 缺口缓冲，4xx 校验或规则拒绝 |
| `GET /v1/state` | 当前全部稳定事实快照（区域/无人机/许可/计划/阶段/基站/改派/通话/队列/指令） |
| `GET /v1/events?limit=n` | 事件日志（含 offset 与链式摘要） |
| `GET /v1/tasks/{taskRef}` | 单任务阶段与阶段历史 |
| `GET /v1/plans/{planRef}` | 单份签署计划 |
| `GET /v1/retasks` | 全部受控改派及状态 |
| `GET /v1/queues` | 各基站补偿队列与等待队列 |
| `GET /v1/reconstruct/call/{callID}` | 还原某通话当时的位置、覆盖、抢占与责任链 |
| `GET /v1/reconstruct/recon/{reconID}` | 还原某侦察成果当时的计划、阶段、位置与责任链 |
| `GET /health` | 健康检查 |

## 运行

```sh
make test      # 全部行为检查（含 -race 建议手动执行：go test -race ./...）
make migrate   # 创建 data/events.jsonl（受 DATABASE_PATH 影响）
make run       # 启动服务，默认 :8080
```

```sh
DATABASE_PATH=data/events.jsonl PORT=8080 make run
```

未设置 `DATABASE_PATH` 时退化为内存日志，仅用于本地演示；应急部署必须配置
持久化路径。配置通过环境变量传入，敏感值与本地数据文件不得提交到仓库。

## 提交事件

载荷只需提供业务字段；`schema_version`、规范化后的 `payload` 与
`payload_digest` 可由 `internal/command.Build` 自动生成。示例见
`contracts/event.example.json`、`contracts/call.example.json`、
`contracts/retask.example.json`。

时间一律使用带偏移量的 ISO 8601 字符串；标识一律为不含真实身份信息的稳定引用，
材料只存受控引用与 `sha256` 摘要。
