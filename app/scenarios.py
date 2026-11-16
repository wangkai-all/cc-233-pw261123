"""Scenario fixtures: fully invented data and deterministic demo situations."""

from __future__ import annotations

import json

from .collector import Collector
from .db import Store, content_hash
from .sources import SourceManager


CUSTOMERS = [
    ("C-1001", {"name": "青禾咖啡", "tier": "gold", "city": "杭州", "balance": 1280}),
    ("C-1002", {"name": "北岸书店", "tier": "silver", "city": "青岛", "balance": 640}),
    ("C-1003", {"name": "拾光旅馆", "tier": "gold", "city": "成都", "balance": 2210}),
    ("C-1004", {"name": "远山茶行", "tier": "bronze", "city": "昆明", "balance": 95}),
    ("C-1005", {"name": "琥珀烘焙", "tier": "silver", "city": "苏州", "balance": 780}),
    ("C-1006", {"name": "潮汐唱片", "tier": "bronze", "city": "厦门", "balance": 310}),
    ("C-1007", {"name": "松间民宿", "tier": "gold", "city": "大理", "balance": 1560}),
    ("C-1008", {"name": "砾石车行", "tier": "silver", "city": "西安", "balance": 460}),
]


def ensure_sources(store: Store, sources: SourceManager) -> None:
    """Create the two independent local demo feeds once."""
    sources.create("orders", "订单中心 · 本地虚构源", page_size=3)
    sources.create("inventory", "库存中心 · 本地虚构源", page_size=3)
    if store.get_kv("seeded"):
        return
    for key, fields in CUSTOMERS:
        # The "orders" feed and the "inventory" feed are independent: the same
        # business key space is deliberately not coordinated between them.
        sources.write("orders", key, {**fields, "orders": 3, "source": "orders"})
        sources.write(
            "inventory", key,
            {"name": fields["name"], "stock": 20 + hash(key) % 300,
             "warehouse": "WH-" + key[-2:], "source": "inventory"},
        )
    store.set_kv("seeded", True)
    store.set_kv("seed_keys", [k for k, _ in CUSTOMERS])


def scenario(
    store: Store, sources: SourceManager, collector: Collector,
    source_id: str, name: str,
) -> dict:
    """Run one canned demonstration.  Returns what was arranged, not results."""
    keys = store.get_kv("seed_keys", [k for k, _ in CUSTOMERS])
    if name == "baseline":
        collector.drain_push(sources, source_id)
        return {"scenario": name,
                "arranged": "推送跑完全部待投递批次，未制造断档"}

    if name == "push_gap":
        # Start from a clean checkpoint, then lose three push events and write
        # four more.  The delivered batch is left pending so the operator's own
        # "接收推送" click is what exposes the sequence jump.
        collector.drain_push(sources, source_id)
        sources.set_outage(source_id, 3)
        for i, key in enumerate(keys[1:5], start=1):
            sources.write(source_id, key,
                          {"name": f"已改·{key}", "tier": "gold",
                           "revision_note": f"gap-demo-{i}", "balance": 100 + i})
        return {
            "scenario": name,
            "arranged": "丢失 3 个推送事件后又写入 4 条，事件仍在队列中；"
                        "点击“接收推送”将暴露序号跳变",
            "expect": "出现未排除缺口与疑点，完整性不能宣称",
        }

    if name == "tail_stall":
        collector.drain_push(sources, source_id)
        sources.set_outage(source_id, 2)
        for i, key in enumerate(keys[:2]):
            sources.write(source_id, key,
                          {"name": f"尾部·{key}", "note": f"tail-{i}"})
        return {
            "scenario": name,
            "arranged": "上游接受 2 条写入但推送事件全部丢失，随后不产生新事件",
            "expect": "仅凭推送无法发现丢失；需要对照推送头部或全量核对",
        }

    if name == "conflict":
        collector.drain_push(sources, source_id)
        key = keys[0]
        info = sources.inject_conflict(source_id, key)
        return {
            "scenario": name,
            "arranged": f"对 {key} 注入与现有修订相同、内容不同的推送事件；"
                        "点击“接收推送”后进入裁决队列",
            "expect": "保留两条版本、记录冲突；不得按到达先后覆盖",
            "detail": info,
        }

    if name == "high_revision":
        # Arrange an out-of-order delivery: a higher revision arrives after the
        # pull scan already saw the lower one.
        collector.drain_push(sources, source_id)
        key = keys[2]
        info = sources.write(source_id, key,
                             {"name": f"高版本·{key}", "note": "late-high-rev",
                              "balance": 9999}, suppress_push=True)
        return {
            "scenario": name,
            "arranged": f"{key} 在本地只有较低修订时，远端已产生修订 "
                        f"{info['revision']}，但推送事件被扣住",
            "expect": "定向补采或全量核对取回更高修订；补采期间再次修订也不得覆盖",
            "detail": info,
        }

    if name == "pull_lag":
        sources.update(source_id, pull_delay_ms=1500)
        for i, key in enumerate(keys[:3]):
            sources.write(source_id, key,
                          {"name": f"刚写入·{key}", "note": "replication-lag"})
        return {
            "scenario": name,
            "arranged": "拉取延迟设为 1500ms 并写入 3 条：立即拉取看不到它们",
            "expect": "立即全量核对得到的快照水位低于写入头部，"
                      "证明范围不得延伸到这些写入",
        }

    raise ValueError(f"unknown scenario: {name}")
