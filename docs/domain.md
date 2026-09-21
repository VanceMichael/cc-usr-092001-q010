# 领域约定

灾区空中通信任务控制器围绕无人机任务、通信覆盖与现场指令保存可核对的业务记录。
外部主体使用不含真实身份信息的稳定引用编号，时间采用带偏移量的 ISO 8601 字符串，
材料只保存受控引用和 `sha256` 摘要。

## 1. 事件信封

系统中**每一条业务事实都是一个仅追加事件**，状态只能由事件重放得到，没有任何
直接改状态的接口。信封字段：

| 字段 | 含义 |
| --- | --- |
| `schema_version` | 契约版本，当前为 `"1"` |
| `event_id` | 全局稳定标识，重复摄入同一标识幂等忽略 |
| `subject_ref` | 任务/区域/无人机/计划/通话等稳定引用 |
| `type` | 受控事件类型，见第 3 节 |
| `occurred_at` | 事件在**现场发生**的时间，带偏移量；不得用到达时间覆盖 |
| `source` | 来源标识；`controller:` 前缀为系统裁决保留，外部不得使用 |
| `source_sequence` | 来源内从 1 开始**连续递增**的序号，仅在同一来源内有意义 |
| `device_serial` | 相关设备序号；阶段变化事件必须携带 |
| `operator` | 触发或签署事件的岗位稳定引用 |
| `payload` | 强类型载荷 |
| `payload_digest` | 对规范化（去字段顺序、去空白）载荷计算的 `sha256:<hex>` |

摘要的规范化形式等价于：把载荷按 JSON 解码后，以对象键有序、无多余空白的
UTF-8 JSON 重新编码（数值按 JSON number 编码：整数不带小数点，如 `2`
而非 `2.0`；字符串中的非 ASCII 字符不转义）。现场终端也可直接使用
`internal/command.Build` 由系统计算；提交时 `payload_digest` 留空则跳过校验，
一旦提供就必须与载荷一致。

## 2. 摄入语义：断链、补传、重试都不制造虚假事实

- **幂等**：同一 `event_id` 的重复遥测、中心重试返回 `duplicate=true`，不追加任何事实。
  因此现场终端必须为同一业务事实**稳定生成** `event_id`（如 `设备-序号` 或内容哈希），
  at-least-once 重发时原样重发同一信封；换新 `event_id` 的重发会被当作新事实，
  其旧序号仍会被水位规则拒绝。
- **序号缺口**：来源的下一个期望序号为 N，先收到 N+1 时只缓冲不落盘（HTTP 202
  `buffered=true`）；N 补传到后按 N→N+1 顺序释放。
- **迟到旧序号**：序号小于水位的事件被拒绝（409 `STALE_SEQUENCE`），即使换了
  `event_id` 也不能覆盖既有事实。
- **时间保真**：排序、许可有效性、事后还原全部使用 `occurred_at`，与处理时刻无关。
- **阶段防虚假**：`task.phase_changed` 是唯一能改变飞行阶段的事件，且必须
  - 携带 `device_serial` 与 `occurred_at`；
  - `from_phase` 与当前事实一致（旧阶段事实补传会被拒绝）；
  - `to_phase` 只能是生命周期中的下一阶段，禁止跳级；
  - 从待命进入转场时持有效起飞许可，或有人工 `OVERRIDE_CLEARANCE` 强制豁免；
  - 锚定一份已签署并激活的计划。

遥测、油电、覆盖上报都不会改变阶段。

## 3. 事件类型

| 类型 | 说明 |
| --- | --- |
| `area.versioned` | 灾害区域版本，版本只能递增；计划必须锚定当前版本 |
| `drone.registered` | 无人机能力（侦察/基站、续航、油电类型） |
| `drone.telemetry` | 位置姿态遥测，只更新位置 |
| `drone.fuel_battery` | 油电状态 |
| `clearance.granted` | 起飞/空域许可与有效时间窗 |
| `plan.signed` / `plan.activated` | 签署计划 / 激活计划（现场以此为准离线执行） |
| `task.phase_changed` | 任务阶段推进（唯一阶段来源） |
| `route.updated` / `route.pinned` | 航线更新 / 改派期间钉死航线 |
| `station.coverage` | 空中基站位置、覆盖半径、容量与在网状态 |
| `ground_team.demand` | 地面队伍通信/侦察需求 |
| `rescue_priority.set` | 救援优先级设定 |
| `person.sighted` | 失联人员位置更新（自动触发改派） |
| `airspace.notice` | 空域冲突/关闭通告（自动触发改派） |
| `disaster.secondary_observed` | 次生灾害观测（自动触发改派） |
| `retask.proposed` / `retask.confirmed` / `retask.rejected` | 受控改派生命周期 |
| `call.requested` / `call.queued` / `call.preempted` / `call.admitted` / `call.ended` | 应急通话裁决 |
| `call.compensation_replayed` | 补偿队列回放 |
| `manual.directive` | 人工指令（FREEZE_CAPACITY / RESUME_CAPACITY / FORCE） |
| `recon.result` | 侦察成果（受控引用 + 摘要） |

`call.queued`、`call.preempted`、`call.admitted`、`call.compensation_replayed`
以及系统生成的 `retask.proposed`、`route.pinned`、`plan.activated`（改派确认时）
均由控制器从父事件**确定性派生**：派生事件 ID 由父事件 ID 与裁决要素哈希得到，
中心重试不会产生第二条裁决。

## 4. 任务阶段与岗位

阶段只能按序推进：

```
STANDBY（待命） → RELOCATE（转场） → NETWORK_UP（建网） → SUSTAIN（保障） → WITHDRAW（撤收）
```

岗位受控词汇：`COMMANDER`、`AIRSPACE`、`FLIGHT_OPS`、`NETWORK_OPS`、
`GROUND_TEAM`、`SYSTEM`（仅系统裁决使用）。

## 5. 受控改派

覆盖下降、空域冲突、失联人员位置更新、次生灾害四类情形发生时，系统**自动生成
`retask.proposed` 并立即钉死航线**，但必须由对应岗位确认后新计划才激活：

| 触发类型 | 必须确认的岗位 |
| --- | --- |
| `COVERAGE_DROP` | `NETWORK_OPS` |
| `AIRSPACE_CONFLICT` | `AIRSPACE` |
| `PERSON_LOCATION_UPDATE` | `GROUND_TEAM` |
| `SECONDARY_DISASTER` | `COMMANDER` |

- 待确认期间任何自动航线策略都被拒绝；人工可凭 FORCE `UNPIN_ROUTE` 介入。
- 岗位确认时可指定重签的新计划 `plan_id`；确认后系统派生 `plan.activated`。
- 岗位拒绝后解除钉死、维持原计划。
- 同一任务同时只允许存在一个待确认改派。

## 6. 通话裁决、抢占与补偿

- 通话优先级 1 为高优先级，数字越大越低。
- 有空余容量：直接接入。
- 容量满且未冻结：高优先级通话可抢占在网**普通**通话；被抢占者进入该基站
  **补偿队列**，抢占事件记录双方通话、补偿条目与裁决方。
- 被人工 FORCE `PROTECT_CALL` 保护的通话、以及其他高优先级通话不可选为抢占对象。
- 容量恢复（通话结束、覆盖恢复、解除冻结）时：**补偿队列先于普通等待队列**，
  各自 FIFO；等待中的高优先级通话重新获得抢占资格。
- `FREEZE_CAPACITY` 期间自动策略禁止抢占，高优先级通话也只能排队（原因 `FROZEN`）。
- 人工 FORCE `ADMIT_CALL` 可无条件接通通话并留痕 `forced=true`，自动策略不得撤销。

## 7. 人工指令优先

人工指令只追加、不可被自动策略覆盖。自动策略对 FORCE 决定只读：冻结状态、
通话保护、许可豁免、强制接入均从指令流按时间推导。

## 8. 持久化与重启

- 事件以 JSONL 追加到 `DATABASE_PATH`，每条记录含 `offset` 与链式摘要
  `chain[n] = sha256(chain[n-1] ‖ canonical(event))`，写入后 fsync。
- 重放逐行校验链式摘要，任何篡改/截断都阻止启动。
- 中心重启后，区域、计划、阶段、许可、队列与序号水位全部从日志重建；
  现场继续执行最后一份已签署并激活的计划。

## 9. 事后还原

输入通话标识（`GET /v1/reconstruct/call/{id}`）或侦察成果标识
（`GET /v1/reconstruct/recon/{id}`），系统以该事实发生时刻为界重放日志，返回：

- 当时各无人机飞行位置与锚定计划、航点；
- 当时各基站覆盖半径、容量与在网状态；
- 截至当时的抢占裁决（双方通话、补偿条目、裁决方）；
- 完整责任链：签署/激活/阶段推进/改派/确认/排队/抢占/回放/人工指令，
  每一环都有 `event_id`、`occurred_at`、`operator`、`device_serial` 与 `offset`。

示例内容仅用于说明字段形状，不代表真实人员、机构或业务结论。
