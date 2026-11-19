"""Kubernetes 资源配额准入分配器（纯模拟，不调用 Kubernetes API）。

核心语义：
- 多资源请求（cpu/memory/storage/pods）必须同时满足才准入。
- 每个命名空间的额度独立记账，不会把其他命名空间未释放的额度重复分配。
- 准入顺序与输入顺序无关：按优先级分层，层内按加权主导份额（weighted
  dominant share）做公平调度，同优先级同份额时按名称字典序打破平局。
- 关键服务下限（guaranteed）在集群容量共享时为对应命名空间保留。
- 突发工作负载（kind="burst"）可以使用 quota + burst 的扩展额度，
  普通/关键工作负载只使用基础 quota。
- 删除或缩容工作负载后，额度归还到其所属命名空间。
"""

RESOURCES = ("cpu", "memory", "storage", "pods")

KIND_CRITICAL = "critical"
KIND_BURST = "burst"
KIND_NORMAL = "normal"
_VALID_KINDS = (KIND_CRITICAL, KIND_BURST, KIND_NORMAL)


def _normalize(spec, missing_value):
    """把资源字典规范化为四种资源齐全；缺失字段填 missing_value。"""
    if spec is None:
        spec = {}
    if not isinstance(spec, dict):
        raise TypeError("资源规格必须是 dict，得到 %r" % type(spec).__name__)
    unknown = set(spec) - set(RESOURCES)
    if unknown:
        raise ValueError("未知资源字段: %s" % sorted(unknown))
    out = {}
    for name in RESOURCES:
        value = spec.get(name, missing_value)
        if value is None:
            out[name] = None
            continue
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise TypeError("资源 %r 的值必须是数字，得到 %r" % (name, value))
        if value < 0:
            raise ValueError("资源 %r 的值不能为负: %r" % (name, value))
        out[name] = value
    return out


def _fits(request, used, limit):
    """检查 request 中所有资源在 used 基础上都不超过 limit。

    limit 为 None 的字段表示该资源不受约束。
    返回第一个不满足的资源名，全部满足返回 None。
    """
    for name in RESOURCES:
        cap = limit[name]
        if cap is None:
            continue
        if used[name] + request[name] > cap:
            return name
    return None


def _add(target, delta):
    for name in RESOURCES:
        target[name] += delta[name]


def _sub(target, delta):
    for name in RESOURCES:
        target[name] -= delta[name]


class Workload(object):
    """一个待批准或已准入的工作负载。"""

    def __init__(self, workload_id, namespace, resources,
                 priority=0, kind=KIND_NORMAL):
        if not workload_id:
            raise ValueError("workload_id 不能为空")
        if kind not in _VALID_KINDS:
            raise ValueError("未知工作负载类型: %r" % (kind,))
        self.id = str(workload_id)
        self.namespace = namespace
        self.resources = _normalize(resources, 0)
        self.priority = priority
        self.kind = kind

    def __repr__(self):
        return "Workload(id=%r, ns=%r, priority=%r, kind=%r)" % (
            self.id, self.namespace, self.priority, self.kind)


class Namespace(object):
    """命名空间的配额、突发额度、公平权重与实时用量。"""

    def __init__(self, name, quota, used=None, burst=None,
                 weight=1.0, guaranteed=None):
        if not name:
            raise ValueError("命名空间名称不能为空")
        if not isinstance(weight, (int, float)) or weight <= 0:
            raise ValueError("公平权重必须为正数: %r" % (weight,))
        self.name = name
        # 配额字段缺失（None）表示该资源不受限制。
        self.quota = _normalize(quota, None)
        self.burst = _normalize(burst, 0)
        self.used = _normalize(used, 0)
        self.guaranteed = _normalize(guaranteed, 0)
        self.weight = float(weight)
        self.allocated = {}  # workload_id -> 资源字典副本

    def limit_for(self, kind):
        """该类型工作负载适用的有效额度（突发类型含突发额度）。"""
        limit = {}
        for name in RESOURCES:
            base = self.quota[name]
            if base is None:
                limit[name] = None
            elif kind == KIND_BURST:
                limit[name] = base + self.burst[name]
            else:
                limit[name] = base
        return limit

    def share_with(self, used):
        """加权主导份额：max(used / (weight * quota))，用于公平调度。
        None 或 0 的配额不参与份额计算。"""
        share = 0.0
        for name in RESOURCES:
            cap = self.quota[name]
            if not cap:
                continue
            share = max(share, used[name] / (self.weight * cap))
        return share


class Rejection(object):
    def __init__(self, workload, reason):
        self.workload = workload
        self.reason = reason

    def __repr__(self):
        return "Rejection(%s, %r)" % (self.workload.id, self.reason)


class AdmissionResult(object):
    """一次准入批处理的结果。"""

    def __init__(self):
        self.admitted = []   # 按准入顺序排列的 Workload
        self.rejected = []   # Rejection 列表

    @property
    def admitted_ids(self):
        return [w.id for w in self.admitted]

    @property
    def rejected_ids(self):
        return [r.workload.id for r in self.rejected]

    def reason_for(self, workload_id):
        for rej in self.rejected:
            if rej.workload.id == workload_id:
                return rej.reason
        return None


class QuotaAllocator(object):
    """配额分配器。可选的 cluster_capacity 表示集群物理容量上限，
    各命名空间配额之和允许超过它（超配），但准入时受实际容量约束。"""

    def __init__(self, cluster_capacity=None):
        if cluster_capacity is None:
            self.cluster_capacity = {name: None for name in RESOURCES}
        else:
            self.cluster_capacity = _normalize(cluster_capacity, None)
        self.namespaces = {}

    # ------------------------------------------------------------------
    # 命名空间管理
    # ------------------------------------------------------------------
    def register_namespace(self, name, quota, used=None, burst=None,
                           weight=1.0, guaranteed=None):
        if name in self.namespaces:
            raise ValueError("命名空间已存在: %r" % (name,))
        ns = Namespace(name, quota, used=used, burst=burst,
                       weight=weight, guaranteed=guaranteed)
        self._check_used_within_limit(ns)
        self.namespaces[name] = ns
        return ns

    def _check_used_within_limit(self, ns):
        for name in RESOURCES:
            cap = ns.quota[name]
            if cap is None:
                continue
            if ns.used[name] > cap + ns.burst[name]:
                raise ValueError(
                    "命名空间 %r 的当前用量 %s=%s 超出配额+突发额度 %s"
                    % (ns.name, name, ns.used[name], cap + ns.burst[name]))

    def usage(self, name):
        """返回命名空间当前用量快照（基础用量 + 已准入工作负载）。"""
        ns = self._ns(name)
        used = dict(ns.used)
        for res in ns.allocated.values():
            _add(used, res)
        return used

    def _ns(self, name):
        try:
            return self.namespaces[name]
        except KeyError:
            raise KeyError("未知命名空间: %r" % (name,))

    # ------------------------------------------------------------------
    # 准入
    # ------------------------------------------------------------------
    def admit(self, workloads):
        """对一批待批准工作负载做准入决策。

        决策只依赖各命名空间状态与权重，与 workloads 的传入顺序无关。
        已准入的工作负载会占用对应命名空间的额度，直到 release/scale_down。
        """
        result = AdmissionResult()
        pending = {}  # ns_name -> [Workload]
        seen = set()
        for wl in workloads:
            ns = self._ns(wl.namespace)
            key = (wl.namespace, wl.id)
            if key in seen or wl.id in ns.allocated:
                raise ValueError("工作负载 id 重复: %r/%r"
                                 % (wl.namespace, wl.id))
            seen.add(key)
            pending.setdefault(wl.namespace, []).append(wl)

        # 按优先级从高到低分层；层内做加权公平调度。
        priorities = sorted({wl.priority for wl in workloads}, reverse=True)
        for prio in priorities:
            tiers = {}
            for ns_name, wls in pending.items():
                tier = sorted((w for w in wls if w.priority == prio),
                              key=lambda w: w.id)
                if tier:
                    tiers[ns_name] = tier
            self._admit_tier(tiers, result)
        return result

    def _admit_tier(self, tiers, result):
        """同一优先级层内：每轮选出加权主导份额最小的命名空间，
        尝试准入其下一个工作负载，直到本层没有待批准请求。"""
        while tiers:
            # 候选命名空间按 (份额, 名称) 排序，保证结果与输入顺序无关。
            # 份额基于实时用量（含本轮已准入的工作负载）。
            ns_name = min(
                tiers,
                key=lambda n: (
                    self.namespaces[n].share_with(self.usage(n)), n))
            ns = self.namespaces[ns_name]
            wl = tiers[ns_name].pop(0)
            if not tiers[ns_name]:
                del tiers[ns_name]
            reason = self._try_admit(ns, wl)
            if reason is None:
                result.admitted.append(wl)
            else:
                result.rejected.append(Rejection(wl, reason))

    def _try_admit(self, ns, wl):
        """返回 None 表示准入成功，否则返回拒绝原因字符串。"""
        used = self.usage(ns.name)
        limit = ns.limit_for(wl.kind)
        lacking = _fits(wl.resources, used, limit)
        if lacking is not None:
            return ("命名空间 %r 的 %s 额度不足: 需要 %s, 已用 %s, 上限 %s"
                    % (ns.name, lacking, wl.resources[lacking],
                       used[lacking], limit[lacking]))

        lacking = self._check_cluster_capacity(ns, wl)
        if lacking is not None:
            return "集群容量不足: %s (命名空间 %r)" % (lacking, ns.name)

        ns.allocated[wl.id] = dict(wl.resources)
        return None

    def _check_cluster_capacity(self, ns, wl):
        """集群容量约束：为准入命名空间之外的命名空间保留其关键服务下限。"""
        totals = {name: 0 for name in RESOURCES}
        per_ns_used = {}
        for other in self.namespaces.values():
            other_used = self.usage(other.name)
            per_ns_used[other.name] = other_used
            for name in RESOURCES:
                totals[name] += other_used[name]
        for name in RESOURCES:
            cap = self.cluster_capacity[name]
            if cap is None:
                continue
            reserved = 0
            for other in self.namespaces.values():
                if other is ns:
                    continue
                reserved += max(
                    0, other.guaranteed[name] - per_ns_used[other.name][name])
            if totals[name] + wl.resources[name] + reserved > cap:
                return name
        return None

    # ------------------------------------------------------------------
    # 回收
    # ------------------------------------------------------------------
    def release(self, namespace, workload_id):
        """删除工作负载：把其占用额度归还给所属命名空间。
        返回被释放的资源字典。"""
        ns = self._ns(namespace)
        try:
            res = ns.allocated.pop(workload_id)
        except KeyError:
            raise KeyError("命名空间 %r 中不存在工作负载 %r"
                           % (namespace, workload_id))
        return res

    def scale_down(self, namespace, workload_id, resources):
        """缩容工作负载：部分归还额度。归还量不能超过当前占用。"""
        ns = self._ns(namespace)
        if workload_id not in ns.allocated:
            raise KeyError("命名空间 %r 中不存在工作负载 %r"
                           % (namespace, workload_id))
        delta = _normalize(resources, 0)
        current = ns.allocated[workload_id]
        for name in RESOURCES:
            if delta[name] > current[name]:
                raise ValueError(
                    "缩容量超过占用量: %s 归还 %s > 占用 %s"
                    % (name, delta[name], current[name]))
        _sub(current, delta)
        return delta
