# Gossip 协议（Protocole de bavardage）

## 1. 为什么需要 Gossip？

在项目的前三个阶段中，系统使用的是**单一 Broker（集中式 courtier）**——所有客户端都连接到同一个 Broker。但在第四阶段（cahier-des-charges §3.7 répartition），需要把 Broker **分布到多个组件**上，每个 Broker 只服务一部分客户端。

问题来了：客户端 A 在 Broker1 上发布消息到 `channel0`，但订阅 `channel0` 的客户端 B 注册在 Broker2 上。**Broker1 怎么把消息传给 Broker2？**

这就需要一个 Broker 之间的**信息同步协议**——即 **Gossip 协议（protocole de bavardage）**。

## 2. Gossip 协议的核心思想

Gossip 的名字来自"八卦/闲聊"——就像人们传播八卦一样：

> **每个节点把信息发给自己的所有邻居，邻居再发给它们的邻居，以此类推，直到所有人都知道。**

具体来说（参考 cahier-des-charges §3.7.1）：

1. Broker 们组成一个**逻辑网络**（graphe），每个 Broker 只和自己的**直接邻居**相连
2. 当一个事件发生时（如发布消息），本地 Broker 把这个事件**包装成 GossipMessage**，发送给所有邻居
3. 邻居收到后，**处理这个事件**（如将消息投递给本地订阅者），然后**继续转发给自己的邻居**
4. 消息没有特定路由，而是像洪水一样**逐级扩散**，最终到达所有节点

## 3. 防止无限循环——去重机制

Gossip 的最大挑战是：消息会**来回反弹**，造成无限循环。解决方案：

- 每条 GossipMessage 有一个**唯一 URI**（由 `brokerURI + "-gossip-" + UUID` 生成）
- 每个 Broker 维护一个 `processedGossipURIs` 表，记录已处理的消息 URI 及其时间戳
- 收到已经处理过的消息 → **直接丢弃**，不再转发

对应代码（`Broker.java` 中的 `update` 方法）：

```java
public void update(GossipMessageI[] fromSender) {
    List<GossipMessageI> toPropagate = new ArrayList<>();

    for (GossipMessageI gm : fromSender) {
        String uri = gm.gossipMessageURI();

        // 去重：putIfAbsent 保证原子性，多线程下也不会重复处理
        if (processedGossipURIs.putIfAbsent(uri, gm.timestamp()) != null) {
            continue;
        }

        // 整合到本地状态
        integrateGossipMessage(gm);
        toPropagate.add(gm);
    }

    // 继续向邻居传播
    if (!toPropagate.isEmpty()) {
        gossipToNeighbors(toPropagate);
    }
}
```

另外，`processedGossipURIs` 会通过定时任务定期清理过期条目（默认 60 秒过期，每 30 秒清理一次），防止内存无限增长。

## 4. Gossip 传播的事件类型

`GossipPayloadType` 枚举定义了 6 种需要在 Broker 之间同步的事件：

| 类型 | 含义 | 为什么需要传播 |
|------|------|--------------|
| `PUBLISH` | 发布消息 | 其他 Broker 上的订阅者也需要收到消息 |
| `REGISTER` | 客户端注册 | 其他 Broker 需要知道这个客户端的存在和服务等级 |
| `UNREGISTER` | 客户端注销 | 其他 Broker 需要同步删除该客户端信息 |
| `CREATE_CHANNEL` | 创建频道 | 所有 Broker 需要知道新频道的存在和权限 |
| `DESTROY_CHANNEL` | 销毁频道 | 所有 Broker 需要同步删除该频道 |
| `MODIFY_AUTH` | 修改频道权限 | 权限变更需全局同步 |

## 5. 完整数据流（以发布消息为例）

```
客户端A --> Broker1.publish("channel0", msg)
              |
              |-- 本地处理：propagateMessages --> 投递给 Broker1 的本地订阅者
              |
              +-- gossipEvent(PUBLISH, {channel, messages})
                    |
                    |-- 创建 GossipMessage（唯一 URI）
                    |-- 记录到 processedGossipURIs（防重复）
                    +-- gossipToNeighbors()
                          |
                          |-- Broker2.receive() --> update()
                          |     |-- 去重检查（新消息，通过）
                          |     |-- integrateGossipMessage
                          |     |     +-- propagateMessages --> 投递给 Broker2 的本地订阅者
                          |     +-- gossipToNeighbors --> 转发给 Broker2 的其他邻居
                          |
                          +-- Broker3.receive() --> update() --> 同理继续传播...
```

当 Gossip 消息回到 Broker1 时，因为 URI 已在 `processedGossipURIs` 中，直接被丢弃，传播终止。

## 6. GossipMessage 的结构

每条 Gossip 消息（`GossipMessage.java`）包含以下字段：

| 字段 | 类型 | 说明 |
|------|------|------|
| `uri` | `String` | 消息的唯一标识，用于去重 |
| `timestamp` | `Instant` | 创建时间戳，用于过期清理 |
| `emitterURI` | `String` | 最近一次发送者的 Broker URI |
| `type` | `GossipPayloadType` | 事件类型（PUBLISH、REGISTER 等） |
| `payload` | `Map<String, Serializable>` | 事件的具体数据 |

`copyWithNewEmitterURI()` 方法用于转发时更新发送者 URI（浅拷贝，payload 共享同一引用），使接收方知道消息是从哪个邻居转发过来的。

## 7. BCM4Java 中的组件架构

Gossip 协议通过 BCM4Java 的端口-连接器（port-connector）机制实现：

### 接口层（由框架提供）

- **`GossipMessageI`**：Gossip 消息的接口，定义 URI、时间戳和拷贝方法
- **`GossipReceiverI` / `GossipReceiverCI`**：接收方接口，提供 `receive()` 方法
- **`GossipSenderCI`**：发送方接口，提供 `send()` 方法
- **`GossipImplementationI`**：实现接口，继承 `GossipReceiverI` 并添加 `update()` 方法

### 实现层

- **`GossipMessage`**：`GossipMessageI` 的具体实现，携带事件类型和 payload
- **`GossipPayloadType`**：事件类型枚举
- **`GossipReceiverInbound`**（offered port）：每个 Broker 暴露一个入站端口，接收邻居发来的 gossip 消息。接收是**异步**的（通过 `runTask` 执行），避免阻塞发送方
- **`GossipSenderOutbound`**（required port）：每个 Broker 为每个邻居创建一个出站端口
- **`GossipConnector`**：连接器把 `send()` 调用翻译成对方的 `receive()` 调用

### 连接拓扑

```
Broker1                          Broker2
+-----------------------------+  +-----------------------------+
| GossipSenderOutbound -------|--|--> GossipReceiverInbound    |
|                             |  |                             |
| GossipReceiverInbound <-----|--|------- GossipSenderOutbound |
+-----------------------------+  +-----------------------------+
```

每对邻居之间有**双向连接**：每个 Broker 既有发送端口也有接收端口。

### Broker 中的 Gossip 相关字段

```java
// 接收 gossip 消息的入站端口
protected GossipReceiverInbound gossipReceiverInbound;

// 发送 gossip 消息到各邻居的出站端口（key = 邻居的 inbound port URI）
protected final ConcurrentMap<String, GossipSenderOutbound> gossipSenderOutbounds;

// 已处理消息的 URI 记录（key = gossip URI, value = timestamp），用于去重
protected final ConcurrentMap<String, Instant> processedGossipURIs;

// 邻居的 gossip 入站端口 URI 列表，在 execute() 中建立连接
protected final String[] neighborGossipInboundURIs;
```

### Broker 中的 Gossip 专用线程池

```java
public static final String GOSSIP_HANDLER_URI = "gossip-pool";
// 在构造函数中创建，包含 2 个线程
this.createNewExecutorService(GOSSIP_HANDLER_URI, 2, false);
```

Gossip 的接收和转发都在此线程池中异步执行，与消息发布和投递的线程池隔离。

## 8. 生命周期

### 执行阶段（`execute()`）

1. 在 `super.execute()` 之前，为每个邻居创建 `GossipSenderOutbound` 端口并建立连接，确保 gossip 网络在客户端活动开始前就绑定完毕
2. 启动定时任务，定期清理过期的 `processedGossipURIs` 条目

### 运行阶段

- 本地事件发生 → `gossipEvent()` → 创建消息 → `gossipToNeighbors()` → 发送给所有邻居
- 收到邻居消息 → `receive()` → `update()` → 去重 + 整合 + 继续传播

### 关闭阶段（`finalise()` / `shutdown()`）

1. 断开所有 gossip 出站端口的连接
2. 取消发布并销毁 gossip 入站端口

## 9. 总结

**Gossip 就是分布式 Broker 之间的"洪水广播"同步协议：每个事件包装成带唯一 ID 的消息，向邻居扩散，邻居再向它们的邻居扩散，通过去重防止无限循环，最终所有 Broker 的状态保持一致。**

这种方式的优势是：
- **简单**：不需要复杂的路由算法，每个节点只需要知道自己的邻居
- **可扩展**：pair-a-pair 架构天然支持大规模部署
- **容错**：只要网络连通，消息最终都能到达所有节点

代价是：
- **消息冗余**：同一条消息会被多次传输（但通过去重不会被多次处理）
- **延迟**：多跳传播意味着非直接邻居会有更高延迟
