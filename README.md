# kubernetes-resource-quota-allocator
按命名空间、优先级和突发请求分配 Kubernetes 资源配额

> **注意：本模块只模拟准入（admission）决策，不调用 Kubernetes API。**
> 它在内存中维护命名空间配额与用量，用于验证配额分配策略，
> 不会创建、修改或删除任何真实的 Kubernetes 资源。

## 功能

- 四维资源配额：`cpu` / `memory` / `storage` / `pods`，多资源请求必须**同时**满足才准入
- 每个命名空间独立记账，已准入未释放的额度不会被重复分配
- 优先级分层调度：高优先级（如关键服务）先准入
- 关键服务下限（`guaranteed`）：共享集群容量时为命名空间保留保底资源
- 突发额度（`burst`）：`kind="burst"` 的工作负载可超出基础配额至 `quota + burst`
- 公平性：同优先级层内按加权主导份额（weighted dominant share）轮流准入，
  小命名空间不会因到达顺序靠后而饿死；准入结果与输入顺序无关
- 回收：`release` 删除工作负载、`scale_down` 缩容，额度归还到正确命名空间
- 配额超配：各命名空间配额之和允许超过 `cluster_capacity`，但准入受物理容量约束
- 字段缺失处理：请求缺失的资源字段按 0 计；配额缺失的字段表示该资源不受限

## 用法

```python
from quota_allocator import QuotaAllocator, Workload

alloc = QuotaAllocator(cluster_capacity={"cpu": 100, "memory": 200})
alloc.register_namespace(
    "payments",
    quota={"cpu": 20, "memory": 40, "storage": 100, "pods": 50},
    used={"cpu": 4, "memory": 8},      # 当前用量
    burst={"cpu": 5},                  # 突发额度
    weight=2.0,                        # 公平权重
    guaranteed={"cpu": 4},             # 关键服务下限
)

result = alloc.admit([
    Workload("web-1", "payments", {"cpu": 2, "memory": 4, "pods": 1},
             priority=10, kind="critical"),
    Workload("batch-1", "payments", {"cpu": 8, "memory": 8, "pods": 1},
             priority=0),
])
print(result.admitted_ids)           # 已准入
print(result.reason_for("batch-1"))  # 拒绝原因（若被拒绝）

alloc.release("payments", "web-1")               # 删除，归还额度
alloc.scale_down("payments", "batch-1", {"cpu": 4})  # 缩容，部分归还
```

## 测试

```bash
python -m unittest
```

覆盖：多资源约束、命名空间公平、突发额度、优先级保护、回收与额度不足等场景。
兼容 Python 3.8+。
