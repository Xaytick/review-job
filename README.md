# review-job

`review-job` 是一个**异步数据处理服务**，它的核心功能是**将数据库中的评价（review）数据同步到 Elasticsearch 中**。

具体来说，它的工作流程是：

1. **消费 Kafka 消息**：

   * 它会连接到 Kafka，并订阅一个名为 `reviewdb_review_info` 的主题（Topic）。
   * 这个主题中的消息，实际上是来自数据库 `review_info` 表的数据变更事件（比如 `INSERT` 新增评价、`UPDATE` 更新评价等）。这通常是通过 Canal 这样的工具实现的 Change Data Capture (CDC) 机制。
2. **处理数据**：

   * 服务会解析 Kafka 中的消息，提取出具体的评价数据。
3. **写入 Elasticsearch**：

   * 根据消息的类型（是新增还是更新），它会将评价数据**索引（写入或更新）**到 Elasticsearch 的 `review` 索引中。

**一句话总结：**

`review-job` 扮演了一个数据管道的角色，它实时地监听评价数据的变化，并将其同步到一个专门用于搜索的 Elasticsearch 引擎中，从而为整个系统提供高效、强大的评价搜索功能。
