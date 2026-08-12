"""定时群消息推送（Cron）AstrBot 插件入口。

管理员/群友用 cron 表达式或快捷语法（每日/每周）设置定时任务，
到点后由插件自动向群发送固定消息。任务持久化（重启不丢）。

核心逻辑见 cron.py（cron 解析与匹配），调度与命令见本文件。
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import MessageChain, filter, AstrMessageEvent
from astrbot.api.star import Context, Star

from .cron import CronExpr, parse_schedule

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None

HELP_TEXT = """⏰ 定时群消息推送 · 使用帮助
【新增任务】
  /定时 新增 <时间>|<消息内容>
    时间支持 3 种写法：
      · cron 表达式：0 8 * * *（每天 8:00）
      · 每日 HH:MM：每日 08:30
      · 每周X HH:MM：每周一 09:00 / 每周日 10:00
    例：/定时 新增 每日 08:00|早上好，记得喝水哦
    例：/定时 新增 0 9 * * 1|周一晨会提醒
【管理】
  /定时 列表            查看所有任务
  /定时 立即 <编号>     手动触发一次（测试用）
  /定时 暂停 <编号>     暂停任务
  /定时 启用 <编号>     恢复任务
  /定时 删除 <编号>     删除任务
  /定时 帮助            查看帮助"""


@dataclass
class PushTask:
    """一条定时推送任务。"""

    id: int
    cron: str
    message: str
    umo: str              # 目标会话（群的 unified_msg_origin）
    creator: str = ""
    enabled: bool = True
    last_fired: str = ""  # 最近一次触发的分钟标识，避免重复发送

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "cron": self.cron,
            "message": self.message,
            "umo": self.umo,
            "creator": self.creator,
            "enabled": self.enabled,
            "last_fired": self.last_fired,
        }


class CronPushPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self.tasks: dict[int, PushTask] = {}
        self._next_id = 1
        self._loaded = False
        self._sched_task: Optional[asyncio.Task] = None
        self._ensure_scheduler()

    # ---------------- 基础工具 ----------------

    def _cfg(self, key: str, default):
        try:
            return self.config.get(key, default)
        except Exception:
            return default

    def _now(self) -> datetime:
        tz = self._cfg("timezone", "")
        if tz and ZoneInfo is not None:
            try:
                return datetime.now(ZoneInfo(tz))
            except Exception:
                pass
        return datetime.now()

    @staticmethod
    def _cmd_text(event: AstrMessageEvent, n: int) -> str:
        """去掉开头的 n 个指令 token，返回剩余文本。"""
        s = event.message_str.strip()
        if s.startswith("/"):
            s = s[1:]
        parts = s.split()
        if len(parts) <= n:
            return ""
        return " ".join(parts[n:]).strip()

    # ---------------- 持久化（KV 存储） ----------------

    async def _ensure_loaded(self) -> None:
        if self._loaded:
            return
        self._loaded = True
        try:
            data = await self.get_kv_data("tasks", [])
            for item in data:
                try:
                    t = PushTask(**item)
                    self.tasks[t.id] = t
                    self._next_id = max(self._next_id, t.id + 1)
                except Exception:
                    logger.warning(f"跳过无法解析的定时任务记录：{item}")
        except Exception:
            logger.exception("加载定时任务失败")

    async def _save_tasks(self) -> None:
        try:
            await self.put_kv_data("tasks", [t.to_dict() for t in self.tasks.values()])
        except Exception:
            logger.exception("保存定时任务失败")

    # ---------------- 调度循环 ----------------

    def _ensure_scheduler(self) -> None:
        if self._sched_task is None or self._sched_task.done():
            try:
                self._sched_task = asyncio.create_task(self._scheduler_loop())
            except RuntimeError:
                logger.warning("当前没有可用的事件循环，调度器将在首次收到指令时启动")

    async def _scheduler_loop(self) -> None:
        while True:
            try:
                await self._ensure_loaded()
                now = self._now()
                key = now.strftime("%Y-%m-%d %H:%M")
                for t in list(self.tasks.values()):
                    if not t.enabled or t.last_fired == key:
                        continue
                    try:
                        if CronExpr(t.cron).match(now):
                            await self.context.send_message(t.umo, MessageChain().message(t.message))
                            t.last_fired = key
                            await self._save_tasks()
                            logger.info(f"定时任务 #{t.id} 已发送到 {t.umo}")
                    except Exception as e:
                        logger.warning(f"定时任务 #{t.id} 执行失败：{e}")
            except Exception:
                logger.exception("定时任务调度循环异常")
            await asyncio.sleep(max(5, int(self._cfg("check_interval_seconds", 20))))

    async def terminate(self) -> None:
        """插件停用/卸载时取消调度任务。"""
        if self._sched_task:
            self._sched_task.cancel()

    # ---------------- 命令组：定时 ----------------

    @filter.command_group("定时", alias={"cron", "定时任务", "timer"})
    def timer_group(self):
        """定时群消息推送"""
        pass

    @timer_group.command("新增", alias={"添加", "add", "create"})
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def add_task(self, event: AstrMessageEvent):
        """新增定时任务：/定时 新增 <时间>|<消息>"""
        self._ensure_scheduler()
        await self._ensure_loaded()
        arg = self._cmd_text(event, 2)
        if "|" not in arg:
            yield event.plain_result("❌ 格式：/定时 新增 <时间>|<消息>，例：/定时 新增 每日 08:00|早上好，记得喝水哦")
            return
        sched, msg = arg.split("|", 1)
        sched, msg = sched.strip(), msg.strip()
        if not msg:
            yield event.plain_result("❌ 消息内容不能为空。")
            return
        try:
            cron_str = parse_schedule(sched)
        except ValueError as e:
            yield event.plain_result(f"❌ {e}")
            return

        per_group = int(self._cfg("max_tasks_per_group", 10))
        gid_count = sum(1 for t in self.tasks.values() if t.umo == event.unified_msg_origin)
        if gid_count >= per_group:
            yield event.plain_result(f"❌ 本群定时任务已达上限（{per_group} 个）。")
            return
        if len(self.tasks) >= int(self._cfg("max_total_tasks", 50)):
            yield event.plain_result("❌ 全局定时任务已达上限。")
            return

        t = PushTask(
            id=self._next_id,
            cron=cron_str,
            message=msg,
            umo=event.unified_msg_origin,
            creator=event.get_sender_name(),
        )
        self._next_id += 1
        self.tasks[t.id] = t
        await self._save_tasks()
        yield event.plain_result(
            f"✅ 已创建定时任务 #{t.id}\n时间：{sched}（cron：{cron_str}）\n内容：{msg}\n"
            f"发送 /定时 立即 {t.id} 可手动测试，/定时 列表 查看全部。"
        )

    @timer_group.command("列表", alias={"list", "查看"})
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def list_tasks(self, event: AstrMessageEvent):
        """查看所有定时任务"""
        self._ensure_scheduler()
        await self._ensure_loaded()
        if not self.tasks:
            yield event.plain_result("📭 暂无定时任务。发送 /定时 新增 <时间>|<消息> 创建。")
            return
        lines = ["⏰ 定时任务列表："]
        for t in sorted(self.tasks.values(), key=lambda x: x.id):
            status = "▶️" if t.enabled else "⏸"
            msg = t.message if len(t.message) <= 30 else t.message[:30] + "…"
            lines.append(f"{status} #{t.id} {t.cron} | {msg}（{t.creator}）")
        lines.append("发送 /定时 帮助 查看操作说明。")
        yield event.plain_result("\n".join(lines))

    def _task_id(self, event: AstrMessageEvent) -> int:
        """解析任务编号（无效返回 -1）。"""
        tid = self._cmd_text(event, 2).strip()
        if not tid.isdigit():
            return -1
        return int(tid)

    @timer_group.command("立即", alias={"run", "now"})
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def run_now(self, event: AstrMessageEvent):
        """手动触发一次任务：/定时 立即 <编号>"""
        await self._ensure_loaded()
        tid = self._task_id(event)
        if tid < 0 or tid not in self.tasks:
            yield event.plain_result(f"❌ 格式：/定时 立即 <编号>，用 /定时 列表 查看编号。")
            return
        try:
            await self.context.send_message(self.tasks[tid].umo, MessageChain().message(self.tasks[tid].message))
        except Exception as e:
            yield event.plain_result(f"❌ 发送失败：{e}")
            return
        yield event.plain_result(f"✅ 已手动发送任务 #{tid} 的内容。")

    @timer_group.command("暂停", alias={"pause", "stop"})
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def pause_task(self, event: AstrMessageEvent):
        """暂停任务：/定时 暂停 <编号>"""
        await self._ensure_loaded()
        tid = self._task_id(event)
        if tid < 0 or tid not in self.tasks:
            yield event.plain_result("❌ 格式：/定时 暂停 <编号>，用 /定时 列表 查看编号。")
            return
        self.tasks[tid].enabled = False
        await self._save_tasks()
        yield event.plain_result(f"⏸ 已暂停任务 #{tid}，发送 /定时 启用 {tid} 恢复。")

    @timer_group.command("启用", alias={"resume", "start"})
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def resume_task(self, event: AstrMessageEvent):
        """恢复任务：/定时 启用 <编号>"""
        await self._ensure_loaded()
        tid = self._task_id(event)
        if tid < 0 or tid not in self.tasks:
            yield event.plain_result("❌ 格式：/定时 启用 <编号>，用 /定时 列表 查看编号。")
            return
        self.tasks[tid].enabled = True
        await self._save_tasks()
        yield event.plain_result(f"▶️ 已启用任务 #{tid}。")

    @timer_group.command("删除", alias={"del", "remove"})
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def delete_task(self, event: AstrMessageEvent):
        """删除任务：/定时 删除 <编号>"""
        await self._ensure_loaded()
        tid = self._cmd_text(event, 2).strip()
        if not tid.isdigit() or int(tid) not in self.tasks:
            yield event.plain_result(f"❌ 没有编号为 {tid or '空'} 的任务。")
            return
        self.tasks.pop(int(tid))
        await self._save_tasks()
        yield event.plain_result(f"🗑 已删除任务 #{tid}。")

    @timer_group.command("帮助", alias={"help"})
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def help_cmd(self, event: AstrMessageEvent):
        """查看帮助"""
        yield event.plain_result(HELP_TEXT)
