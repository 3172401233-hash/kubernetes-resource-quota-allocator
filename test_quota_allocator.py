import unittest

from quota_allocator import (
    KIND_BURST,
    KIND_CRITICAL,
    KIND_NORMAL,
    QuotaAllocator,
    Workload,
)


def wl(wid, ns, cpu=0, memory=0, storage=0, pods=0,
       priority=0, kind=KIND_NORMAL):
    return Workload(wid, ns,
                    {"cpu": cpu, "memory": memory,
                     "storage": storage, "pods": pods},
                    priority=priority, kind=kind)


class MultiResourceConstraintTest(unittest.TestCase):
    """多资源请求必须同时满足。"""

    def setUp(self):
        self.alloc = QuotaAllocator()
        self.alloc.register_namespace(
            "ns", quota={"cpu": 8, "memory": 16, "storage": 100, "pods": 10})

    def test_memory_shortage_rejects_even_if_cpu_fits(self):
        result = self.alloc.admit([wl("w1", "ns", cpu=1, memory=20, pods=1)])
        self.assertEqual(result.admitted_ids, [])
        self.assertEqual(result.rejected_ids, ["w1"])
        self.assertIn("memory", result.reason_for("w1"))

    def test_all_resources_must_fit_simultaneously(self):
        # cpu/memory/storage 都够，只有 pods 不够，也必须拒绝。
        result = self.alloc.admit([wl("w1", "ns", cpu=1, memory=1,
                                      storage=1, pods=11)])
        self.assertEqual(result.rejected_ids, ["w1"])
        self.assertIn("pods", result.reason_for("w1"))

    def test_rejection_does_not_consume_quota(self):
        self.alloc.admit([wl("big", "ns", cpu=9, memory=1, pods=1)])
        result = self.alloc.admit([wl("ok", "ns", cpu=8, memory=1, pods=1)])
        self.assertEqual(result.admitted_ids, ["ok"])


class NamespaceFairnessTest(unittest.TestCase):
    """公平性：小命名空间不应因到达顺序靠后而长期拿不到资源。"""

    def test_small_namespace_not_starved(self):
        alloc = QuotaAllocator()
        for name in ("big", "small"):
            alloc.register_namespace(
                name, quota={"cpu": 4, "memory": 4, "pods": 100})
        # big 先到达且请求很多；small 最后才到达。
        pending = [wl("big-%d" % i, "big", cpu=1, memory=1, pods=1)
                   for i in range(6)]
        pending += [wl("small-%d" % i, "small", cpu=1, memory=1, pods=1)
                    for i in range(2)]
        result = alloc.admit(pending)
        # 两个命名空间额度独立，small 全部准入，big 超出自身配额的被拒绝。
        self.assertEqual(sorted(result.admitted_ids),
                         ["big-0", "big-1", "big-2", "big-3",
                          "small-0", "small-1"])
        self.assertEqual(sorted(result.rejected_ids), ["big-4", "big-5"])

    def test_shared_cluster_capacity_weighted_fair_share(self):
        # 集群容量是共享瓶颈时，按权重分配，与提交顺序无关。
        alloc = QuotaAllocator(cluster_capacity={"cpu": 6})
        alloc.register_namespace("a", quota={"cpu": 100}, weight=2.0)
        alloc.register_namespace("b", quota={"cpu": 100}, weight=1.0)
        pending = [wl("b-%d" % i, "b", cpu=1) for i in range(6)]
        pending += [wl("a-%d" % i, "a", cpu=1) for i in range(6)]
        result = alloc.admit(pending)
        admitted_a = len([i for i in result.admitted_ids
                          if i.startswith("a")])
        admitted_b = len([i for i in result.admitted_ids
                          if i.startswith("b")])
        # 权重 2:1 -> 份额 4:2
        self.assertEqual((admitted_a, admitted_b), (4, 2))

    def test_same_priority_alternates_between_namespaces(self):
        alloc = QuotaAllocator(cluster_capacity={"cpu": 4})
        for name in ("a", "b"):
            alloc.register_namespace(name, quota={"cpu": 100})
        pending = [wl("a-%d" % i, "a", cpu=1) for i in range(4)]
        pending += [wl("b-%d" % i, "b", cpu=1) for i in range(4)]
        result = alloc.admit(pending)
        # 同优先级同权重：轮流准入，前 4 个里两个命名空间各占一半。
        first_four = result.admitted_ids[:4]
        self.assertEqual(sum(i.startswith("a") for i in first_four), 2)
        self.assertEqual(sum(i.startswith("b") for i in first_four), 2)

    def test_input_order_does_not_change_decision(self):
        def run(order):
            alloc = QuotaAllocator(cluster_capacity={"cpu": 5})
            for name in ("a", "b"):
                alloc.register_namespace(name, quota={"cpu": 100})
            pending = [wl("a-%d" % i, "a", cpu=1) for i in range(4)]
            pending += [wl("b-%d" % i, "b", cpu=1) for i in range(4)]
            result = alloc.admit(order(pending))
            return (sorted(result.admitted_ids),
                    sorted(result.rejected_ids))

        forward = run(list)
        backward = run(lambda p: list(reversed(p)))
        self.assertEqual(forward, backward)


class BurstAllowanceTest(unittest.TestCase):
    """突发额度：burst 类型可超出基础配额，普通类型不行。"""

    def setUp(self):
        self.alloc = QuotaAllocator()
        self.alloc.register_namespace(
            "ns", quota={"cpu": 4, "pods": 10}, burst={"cpu": 2, "pods": 2})

    def test_burst_workload_uses_extended_limit(self):
        result = self.alloc.admit([
            wl("base", "ns", cpu=4, pods=1),
            wl("spike", "ns", cpu=2, pods=1, kind=KIND_BURST),
        ])
        self.assertEqual(sorted(result.admitted_ids), ["base", "spike"])

    def test_normal_workload_cannot_use_burst(self):
        result = self.alloc.admit([
            wl("base", "ns", cpu=4, pods=1),
            wl("extra", "ns", cpu=1, pods=1),
        ])
        self.assertEqual(result.admitted_ids, ["base"])
        self.assertIn("cpu", result.reason_for("extra"))

    def test_burst_beyond_burst_allowance_rejected(self):
        result = self.alloc.admit([
            wl("spike", "ns", cpu=7, pods=1, kind=KIND_BURST),
        ])
        self.assertEqual(result.rejected_ids, ["spike"])


class PriorityProtectionTest(unittest.TestCase):
    """优先级保护与关键服务下限。"""

    def test_higher_priority_admitted_first(self):
        alloc = QuotaAllocator()
        alloc.register_namespace("ns", quota={"cpu": 2, "pods": 10})
        result = alloc.admit([
            wl("low", "ns", cpu=2, pods=1, priority=0),
            wl("high", "ns", cpu=2, pods=1, priority=10,
               kind=KIND_CRITICAL),
        ])
        self.assertEqual(result.admitted_ids, ["high"])
        self.assertEqual(result.rejected_ids, ["low"])

    def test_same_priority_deterministic_tie_break(self):
        # 相同优先级时按工作负载 id 字典序，结果与输入顺序无关。
        def run(reverse):
            alloc = QuotaAllocator()
            alloc.register_namespace("ns", quota={"cpu": 1, "pods": 10})
            pending = [wl("x", "ns", cpu=1, pods=1, priority=5),
                       wl("a", "ns", cpu=1, pods=1, priority=5)]
            if reverse:
                pending.reverse()
            return alloc.admit(pending).admitted_ids

        self.assertEqual(run(False), ["a"])
        self.assertEqual(run(True), ["a"])

    def test_guaranteed_floor_reserved_for_critical_service(self):
        # 集群容量 10，ns-b 保留 4 CPU 下限；ns-a 最多只能用 6。
        alloc = QuotaAllocator(cluster_capacity={"cpu": 10})
        alloc.register_namespace("ns-a", quota={"cpu": 10})
        alloc.register_namespace("ns-b", quota={"cpu": 10},
                                 guaranteed={"cpu": 4})
        result = alloc.admit([wl("a-%d" % i, "ns-a", cpu=1)
                              for i in range(8)])
        self.assertEqual(len(result.admitted), 6)
        # ns-b 的关键服务仍然能拿到保低下限。
        result_b = alloc.admit([
            wl("crit", "ns-b", cpu=4, priority=100, kind=KIND_CRITICAL)])
        self.assertEqual(result_b.admitted_ids, ["crit"])


class ReclaimTest(unittest.TestCase):
    """删除/缩容后额度归还到正确命名空间。"""

    def setUp(self):
        self.alloc = QuotaAllocator()
        self.alloc.register_namespace("a", quota={"cpu": 2, "pods": 10})
        self.alloc.register_namespace("b", quota={"cpu": 2, "pods": 10})

    def test_release_returns_quota_to_owner(self):
        self.alloc.admit([wl("w1", "a", cpu=2, pods=1)])
        self.assertEqual(self.alloc.usage("a")["cpu"], 2)
        released = self.alloc.release("a", "w1")
        self.assertEqual(released["cpu"], 2)
        self.assertEqual(self.alloc.usage("a")["cpu"], 0)
        # 归还后本命名空间可以再次准入。
        result = self.alloc.admit([wl("w2", "a", cpu=2, pods=1)])
        self.assertEqual(result.admitted_ids, ["w2"])

    def test_release_does_not_leak_across_namespaces(self):
        self.alloc.admit([wl("wa", "a", cpu=2, pods=1),
                          wl("wb", "b", cpu=2, pods=1)])
        self.alloc.release("a", "wa")
        # a 的释放不影响 b 的用量。
        self.assertEqual(self.alloc.usage("b")["cpu"], 2)
        result = self.alloc.admit([wl("wb2", "b", cpu=1, pods=1)])
        self.assertEqual(result.rejected_ids, ["wb2"])

    def test_release_unknown_workload_raises(self):
        with self.assertRaises(KeyError):
            self.alloc.release("a", "nope")

    def test_scale_down_partial_release(self):
        self.alloc.admit([wl("w1", "a", cpu=2, pods=1)])
        self.alloc.scale_down("a", "w1", {"cpu": 1})
        self.assertEqual(self.alloc.usage("a")["cpu"], 1)
        with self.assertRaises(ValueError):
            self.alloc.scale_down("a", "w1", {"cpu": 5})


class InsufficientQuotaTest(unittest.TestCase):
    def test_quota_exhaustion(self):
        alloc = QuotaAllocator()
        alloc.register_namespace("ns", quota={"cpu": 2, "pods": 10},
                                 used={"cpu": 1})
        result = alloc.admit([wl("w1", "ns", cpu=2, pods=1)])
        self.assertEqual(result.rejected_ids, ["w1"])
        reason = result.reason_for("w1")
        self.assertIn("cpu", reason)
        self.assertIn("ns", reason)

    def test_no_double_allocation_of_unreleased_quota(self):
        # 已准入但未释放的额度不能被后续批次重复分配。
        alloc = QuotaAllocator()
        alloc.register_namespace("ns", quota={"cpu": 4, "pods": 10})
        first = alloc.admit([wl("w1", "ns", cpu=3, pods=1)])
        self.assertEqual(first.admitted_ids, ["w1"])
        second = alloc.admit([wl("w2", "ns", cpu=3, pods=1)])
        self.assertEqual(second.rejected_ids, ["w2"])


class EdgeCaseInputTest(unittest.TestCase):
    """字段缺失、超配与非法输入。"""

    def test_missing_resource_fields_default_to_zero(self):
        alloc = QuotaAllocator()
        alloc.register_namespace("ns", quota={"cpu": 1, "pods": 5})
        # 请求只声明 cpu，其余字段按 0 处理。
        result = alloc.admit([Workload("w1", "ns", {"cpu": 1})])
        self.assertEqual(result.admitted_ids, ["w1"])

    def test_missing_quota_field_means_unlimited(self):
        alloc = QuotaAllocator()
        alloc.register_namespace("ns", quota={"cpu": 1})
        # quota 未声明 pods -> pods 不受限。
        result = alloc.admit([wl("w1", "ns", cpu=1, pods=1000)])
        self.assertEqual(result.admitted_ids, ["w1"])

    def test_overcommitted_quotas_bounded_by_cluster_capacity(self):
        # 两个命名空间配额之和 (8+8) 超过集群容量 10：允许超配，
        # 但准入总量不能超过物理容量。
        alloc = QuotaAllocator(cluster_capacity={"cpu": 10})
        alloc.register_namespace("a", quota={"cpu": 8})
        alloc.register_namespace("b", quota={"cpu": 8})
        result = alloc.admit([wl("a1", "a", cpu=8), wl("b1", "b", cpu=8)])
        self.assertEqual(len(result.admitted), 1)
        self.assertEqual(len(result.rejected), 1)
        self.assertIn("集群容量不足",
                      result.reason_for(result.rejected_ids[0]))

    def test_used_exceeding_quota_rejected_at_registration(self):
        alloc = QuotaAllocator()
        with self.assertRaises(ValueError):
            alloc.register_namespace("ns", quota={"cpu": 1},
                                     used={"cpu": 5})

    def test_invalid_inputs_raise(self):
        alloc = QuotaAllocator()
        with self.assertRaises(ValueError):
            alloc.register_namespace("ns", quota={"cpu": 1}, weight=0)
        with self.assertRaises(ValueError):
            alloc.register_namespace("ns", quota={"gpu": 1})
        alloc.register_namespace("ns", quota={"cpu": 1})
        with self.assertRaises(ValueError):
            alloc.register_namespace("ns", quota={"cpu": 1})
        with self.assertRaises(KeyError):
            alloc.admit([wl("w1", "ghost", cpu=1)])
        with self.assertRaises(ValueError):
            Workload("w", "ns", {"cpu": -1})
        with self.assertRaises(ValueError):
            Workload("w", "ns", {"cpu": 1}, kind="weird")

    def test_duplicate_workload_id_rejected(self):
        alloc = QuotaAllocator()
        alloc.register_namespace("ns", quota={"cpu": 10})
        alloc.admit([wl("w1", "ns", cpu=1)])
        with self.assertRaises(ValueError):
            alloc.admit([wl("w1", "ns", cpu=1)])


if __name__ == "__main__":
    unittest.main()
