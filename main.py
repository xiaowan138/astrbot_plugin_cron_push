"""定时群消息推送（Cron）AstrBot 插件入口。

管理员/群友用 cron 表达式或快捷语法（每日/每周）设置定时任务，
到点后由插件自动向群发送固定消息。任务持久化（重启不丢）。

核心逻辑见 cron.py（cron 解析与匹配），调度与命令见本文件。
"""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import MessageChain, filter, AstrMessageEvent
from astrbot.api.message_components import At, Plain
from astrbot.api.star import Context, Star

from .cron import CronExpr, parse_schedule

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None

HELP_TEXT = """⏰ 定时消息推送 · 使用帮助
【新增任务】
  /定时 新增 <时间>|<消息内容>
    时间支持 3 种写法：
      · cron 表达式：0 8 * * *（每天 8:00）
      · 每日 HH:MM：每日 08:30
      · 每周X HH:MM：每周一 09:00 / 每周日 10:00
    例：/定时 新增 每日 08:00|早上好，记得喝水哦
    例：/定时 新增 0 9 * * 1|周一晨会提醒
    消息里可用占位符：{{time}} 当前 HH:MM、{{date}} 今日日期、{{datetime}} 日期+时间
                     {{at:QQ号}} 在消息中 @某人（需平台支持）
【管理】（仅管理员可用，且只能操作本会话的任务）
  /定时 列表            查看本会话任务（含上次/下次触发与累计次数）
  /定时 编辑 <编号>|<新内容>   修改任务的消息内容
  /定时 改时 <编号>|<新时间>   修改任务的触发时间（支持同样的时间写法）
  /定时 立即 <编号>     手动触发一次（测试用）
  /定时 暂停 <编号>     暂停任务
  /定时 启用 <编号>     恢复任务
  /定时 删除 <编号>     删除任务
  /定时 帮助            查看帮助
群聊与私聊均可使用；任务会推送到创建它的那个会话。"""

_AT_PATTERN = re.compile(r"\{\{at:(\d+)\}\}")


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
    fire_count: int = 0  # 累计触发次数
    consecutive_failures: int = 0  # 连续发送失败次数

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "cron": self.cron,
            "message": self.message,
            "umo": self.umo,
            "creator": self.creator,
            "enabled": self.enabled,
            "last_fired": self.last_fired,
            "fire_count": self.fire_count,
            "consecutive_failures": self.consecutive_failures,
        }


class CronPushPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self.tasks: dict[int, PushTask] = {}
        self._next_id = 1
        self._loaded = False
        self._caught_up = False
        self._load_lock = asyncio.Lock()
        self._save_lock = asyncio.Lock()
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
        """去掉开头的 n 个指令 token，返回剩余原文（保留内容中的空格与换行）。"""
        s = event.message_str.lstrip()
        if s.startswith("/"):
            s = s[1:]
        for _ in range(n):
            s = s.lstrip()
            idx = 0
            while idx < len(s) and not s[idx].isspace():
                idx += 1
            s = s[idx:]
        return s.strip()

    # ---------------- 持久化（KV 存储） ----------------

    async def _ensure_loaded(self) -> None:
        if self._loaded:
            return
        async with self._load_lock:
            if self._loaded:
                return
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
            finally:
                self._loaded = True

    async def _save_tasks(self) -> None:
        async with self._save_lock:
            try:
                await self.put_kv_data("tasks", [t.to_dict() for t in self.tasks.values()])
            except Exception:
                logger.exception("保存定时任务失败")

    @staticmethod
    def _render(tpl: str, now: datetime) -> str:
        """替换消息模板中的占位符。"""
        return (
            tpl.replace("{{datetime}}", now.strftime("%Y-%m-%d %H:%M"))
            .replace("{{date}}", now.strftime("%Y-%m-%d"))
            .replace("{{time}}", now.strftime("%H:%M"))
        )

    def _build_chain(self, tpl: str, now: datetime) -> MessageChain:
        """构建待发送消息链；含 {{at:QQ}} 时拆分为文本与 @ 消息段。"""
        text = self._render(tpl, now)
        if not _AT_PATTERN.search(text):
            return MessageChain().message(text)
        try:
            chain = MessageChain()
            pos = 0
            for m in _AT_PATTERN.finditer(text):
                if m.start() > pos:
                    chain.append(Plain(text[pos : m.start()]))
                chain.append(At(qq=m.group(1)))
                pos = m.end()
            if pos < len(text):
                chain.append(Plain(text[pos:]))
            return chain
        except Exception as e:
            logger.warning(f"构建 @ 消息段失败，降级为纯文本：{e}")
            return MessageChain().message(text)

    async def _notify(self, umo: str, text: str) -> None:
        """向会话发送提示信息，失败仅记录日志。"""
        try:
            await self.context.send_message(umo, MessageChain().message(text))
        except Exception as e:
            logger.warning(f"发送通知失败：{e}")

    def _parse_last_fired(self, value: str, now: datetime):
        """把持久化的 last_fired 还原为与 now 同时区的时间。"""
        if not value:
            return None
        try:
            return datetime.strptime(value, "%Y-%m-%d %H:%M").replace(tzinfo=now.tzinfo)
        except ValueError:
            return None

    def _resolve_task(self, event: AstrMessageEvent, tid: int):
        """按编号取任务并校验会话归属，返回 (任务, 错误信息)。"""
        task = self.tasks.get(tid)
        if task is None:
            return None, f"❌ 没有编号为 {tid} 的任务。"
        if not self._cfg("allow_cross_session", False) and task.umo != event.unified_msg_origin:
            return None, f"❌ 任务 #{tid} 属于其它会话，无法在此操作。"
        return task, ""

    def _next_fire_str(self, cron: str) -> str:
        try:
            nxt = CronExpr(cron).find_next(self._now())
        except Exception:
            return "—"
        return nxt.strftime("%Y-%m-%d %H:%M") if nxt else "—"

    def _max_failures(self) -> int:
        """连续发送失败达到该次数自动暂停任务；0 表示不自动暂停。"""
        try:
            return max(0, int(self._cfg("max_consecutive_failures", 3)))
        except Exception:
            return 3

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
                if not self._caught_up:
                    self._caught_up = True
                    await self._catch_up(now)
                key = now.strftime("%Y-%m-%d %H:%M")
                for t in list(self.tasks.values()):
                    if not t.enabled or t.last_fired == key:
                        continue
                    try:
                        matched = CronExpr(t.cron).match(now)
                    except Exception as e:
                        logger.warning(f"定时任务 #{t.id} cron 无效：{e}")
                        continue
                    if not matched:
                        continue
                    try:
                        await self.context.send_message(t.umo, self._build_chain(t.message, now))
                        t.fire_count += 1
                        t.consecutive_failures = 0
                        t.last_fired = key
                        await self._save_tasks()
                        logger.info(f"定时任务 #{t.id} 已发送到 {t.umo}")
                    except Exception as e:
                        t.consecutive_failures += 1
                        limit = self._max_failures()
                        if limit and t.consecutive_failures >= limit:
                            t.enabled = False
                            await self._save_tasks()
                            logger.warning(f"定时任务 #{t.id} 连续失败 {t.consecutive_failures} 次，已自动暂停")
                            await self._notify(
                                t.umo,
                                f"⚠️ 定时任务 #{t.id} 已连续发送失败 {t.consecutive_failures} 次，"
                                f"为免继续无效重试已自动暂停。\n内容：{t.message[:50]}\n"
                                f"排查后可发送 /定时 启用 {t.id} 恢复。",
                            )
                        else:
                            logger.warning(f"定时任务 #{t.id} 执行失败：{e}")
            except Exception:
                logger.exception("定时任务调度循环异常")
            await asyncio.sleep(max(5, int(self._cfg("check_interval_seconds", 20))))

    async def _catch_up(self, now: datetime) -> None:
        """补发离线期间错过的任务；每个任务最多补发一次。"""
        if not self._cfg("catch_up", False):
            return
        try:
            max_hours = max(1, int(self._cfg("catch_up_max_hours", 6)))
        except Exception:
            max_hours = 6
        window_start = now - timedelta(hours=max_hours)
        for t in list(self.tasks.values()):
            if not t.enabled:
                continue
            start = self._parse_last_fired(t.last_fired, now) or window_start
            if start < window_start:
                start = window_start
            try:
                missed = CronExpr(t.cron).find_next(start)
            except Exception:
                continue
            if missed is None or missed > now:
                continue
            try:
                await self.context.send_message(t.umo, self._build_chain(t.message, now))
                t.fire_count += 1
                t.consecutive_failures = 0
                t.last_fired = now.strftime("%Y-%m-%d %H:%M")
                await self._save_tasks()
                logger.info(f"定时任务 #{t.id} 已补发（错过 {missed:%Y-%m-%d %H:%M}）")
            except Exception as e:
                logger.warning(f"定时任务 #{t.id} 补发失败：{e}")

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
    @filter.event_message_type(filter.EventMessageType.ALL)
    @filter.permission_type(filter.PermissionType.ADMIN)
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
    @filter.event_message_type(filter.EventMessageType.ALL)
    async def list_tasks(self, event: AstrMessageEvent):
        """查看定时任务"""
        self._ensure_scheduler()
        await self._ensure_loaded()
        show_all = bool(self._cfg("list_global", False))
        if show_all:
            items = list(self.tasks.values())
        else:
            items = [t for t in self.tasks.values() if t.umo == event.unified_msg_origin]
        if not items:
            hint = "" if show_all else "（或可在插件配置开启 `list_global` 查看全部会话任务）"
            yield event.plain_result(f"📭 当前会话暂无定时任务。发送 /定时 新增 <时间>|<消息> 创建任务{hint}。")
            return
        lines = ["⏰ 定时任务列表："]
        for t in sorted(items, key=lambda x: x.id):
            status = "▶️" if t.enabled else "⏸"
            msg = t.message if len(t.message) <= 24 else t.message[:24] + "…"
            last = t.last_fired.split(" ", 1)[-1] if t.last_fired else "—"
            when = f"下次 {self._next_fire_str(t.cron)}" if t.enabled else "已暂停"
            session = f" · {t.umo.split(':', 1)[-1]}" if show_all else ""
            lines.append(f"{status} #{t.id} {t.cron} | {msg}")
            lines.append(f"    上次 {last} · {when} · 已触发 {t.fire_count} 次 · 创建者 {t.creator or '—'}{session}")
        lines.append("发送 /定时 帮助 查看操作说明。")
        yield event.plain_result("\n".join(lines))

    def _task_id(self, event: AstrMessageEvent) -> int:
        """解析任务编号（无效返回 -1）。"""
        parts = self._cmd_text(event, 2).split(maxsplit=1)
        if not parts or not parts[0].isdigit():
            return -1
        return int(parts[0])

    @timer_group.command("立即", alias={"run", "now"})
    @filter.event_message_type(filter.EventMessageType.ALL)
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def run_now(self, event: AstrMessageEvent):
        """手动触发一次任务：/定时 立即 <编号>"""
        await self._ensure_loaded()
        tid = self._task_id(event)
        if tid < 0:
            yield event.plain_result("❌ 格式：/定时 立即 <编号>，用 /定时 列表 查看编号。")
            return
        t, err = self._resolve_task(event, tid)
        if t is None:
            yield event.plain_result(err)
            return
        try:
            await self.context.send_message(t.umo, self._build_chain(t.message, self._now()))
        except Exception as e:
            yield event.plain_result(f"❌ 发送失败：{e}")
            return
        # 置位 last_fired，避免手动触发与本分钟调度触发重复发送
        t.last_fired = self._now().strftime("%Y-%m-%d %H:%M")
        t.fire_count += 1
        t.consecutive_failures = 0
        await self._save_tasks()
        yield event.plain_result(f"✅ 已手动发送任务 #{tid} 的内容。")

    @timer_group.command("暂停", alias={"pause", "stop"})
    @filter.event_message_type(filter.EventMessageType.ALL)
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def pause_task(self, event: AstrMessageEvent):
        """暂停任务：/定时 暂停 <编号>"""
        await self._ensure_loaded()
        tid = self._task_id(event)
        if tid < 0:
            yield event.plain_result("❌ 格式：/定时 暂停 <编号>，用 /定时 列表 查看编号。")
            return
        t, err = self._resolve_task(event, tid)
        if t is None:
            yield event.plain_result(err)
            return
        t.enabled = False
        await self._save_tasks()
        yield event.plain_result(f"⏸ 已暂停任务 #{tid}，发送 /定时 启用 {tid} 恢复。")

    @timer_group.command("启用", alias={"resume", "start"})
    @filter.event_message_type(filter.EventMessageType.ALL)
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def resume_task(self, event: AstrMessageEvent):
        """恢复任务：/定时 启用 <编号>"""
        await self._ensure_loaded()
        tid = self._task_id(event)
        if tid < 0:
            yield event.plain_result("❌ 格式：/定时 启用 <编号>，用 /定时 列表 查看编号。")
            return
        t, err = self._resolve_task(event, tid)
        if t is None:
            yield event.plain_result(err)
            return
        t.enabled = True
        t.consecutive_failures = 0
        await self._save_tasks()
        yield event.plain_result(f"▶️ 已启用任务 #{tid}。")

    @timer_group.command("删除", alias={"del", "remove"})
    @filter.event_message_type(filter.EventMessageType.ALL)
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def delete_task(self, event: AstrMessageEvent):
        """删除任务：/定时 删除 <编号>"""
        await self._ensure_loaded()
        tid = self._task_id(event)
        if tid < 0:
            yield event.plain_result("❌ 格式：/定时 删除 <编号>，用 /定时 列表 查看编号。")
            return
        t, err = self._resolve_task(event, tid)
        if t is None:
            yield event.plain_result(err)
            return
        self.tasks.pop(tid)
        await self._save_tasks()
        yield event.plain_result(f"🗑 已删除任务 #{tid}。")

    @timer_group.command("编辑", alias={"edit", "改内容"})
    @filter.event_message_type(filter.EventMessageType.ALL)
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def edit_task(self, event: AstrMessageEvent):
        """修改任务的消息内容：/定时 编辑 <编号>|<新内容>"""
        await self._ensure_loaded()
        arg = self._cmd_text(event, 2)
        id_part, _, payload = arg.partition("|")
        if not id_part.strip().isdigit() or not payload.strip():
            yield event.plain_result("❌ 格式：/定时 编辑 <编号>|<新内容>，用 /定时 列表 查看编号，新内容不能为空。")
            return
        t, err = self._resolve_task(event, int(id_part.strip()))
        if t is None:
            yield event.plain_result(err)
            return
        t.message = payload.strip()
        await self._save_tasks()
        yield event.plain_result(f"✅ 已更新任务 #{t.id} 的内容。")

    @timer_group.command("改时", alias={"resched", "改时间"})
    @filter.event_message_type(filter.EventMessageType.ALL)
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def resched_task(self, event: AstrMessageEvent):
        """修改任务的触发时间：/定时 改时 <编号>|<新时间>"""
        await self._ensure_loaded()
        arg = self._cmd_text(event, 2)
        id_part, _, payload = arg.partition("|")
        if not id_part.strip().isdigit() or not payload.strip():
            yield event.plain_result("❌ 格式：/定时 改时 <编号>|<新时间>，例如：/定时 改时 3|每日 09:00")
            return
        t, err = self._resolve_task(event, int(id_part.strip()))
        if t is None:
            yield event.plain_result(err)
            return
        try:
            new_cron = parse_schedule(payload.strip())
        except ValueError as e:
            yield event.plain_result(f"❌ {e}")
            return
        t.cron = new_cron
        await self._save_tasks()
        yield event.plain_result(f"✅ 已更新任务 #{t.id} 的触发时间（cron：{new_cron}）。")

    @timer_group.command("帮助", alias={"help"})
    @filter.event_message_type(filter.EventMessageType.ALL)
    async def help_cmd(self, event: AstrMessageEvent):
        """查看帮助"""
        yield event.plain_result(HELP_TEXT)
