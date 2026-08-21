"""5 段 cron 表达式解析与匹配 + 常用快捷语法。纯逻辑，可独立测试。"""

from __future__ import annotations

from datetime import datetime, timedelta

WEEKDAY_MAP = {"一": 1, "二": 2, "三": 3, "四": 4, "五": 5, "六": 6, "日": 0, "天": 0}


class CronExpr:
    """5 段 cron：分 时 日 月 周。

    周字段：0 或 7 表示周日，1-6 表示周一~周六。
    支持 *、n、a-b、*/n、a-b/n、a,b,c 语法。
    """

    def __init__(self, expr: str):
        fields = expr.split()
        if len(fields) != 5:
            raise ValueError("cron 表达式需为 5 段（分 时 日 月 周），例如：0 8 * * *")
        self.expr = expr
        self.minute = self._parse(fields[0], 0, 59)
        self.hour = self._parse(fields[1], 0, 23)
        self.day = self._parse(fields[2], 1, 31)
        self.month = self._parse(fields[3], 1, 12)
        self.week = {0 if v == 7 else v for v in self._parse(fields[4], 0, 7)}

    @staticmethod
    def _parse(field: str, lo: int, hi: int) -> set[int]:
        result: set[int] = set()
        for part in field.split(","):
            part = part.strip()
            if not part:
                continue
            if part == "*":
                result.update(range(lo, hi + 1))
                continue
            if "/" in part:
                base, step = part.split("/", 1)
                step = int(step)
                if step <= 0:
                    raise ValueError(f"无效的步长：{part}")
                if base == "*":
                    result.update(range(lo, hi + 1, step))
                elif "-" in base:
                    a, b = base.split("-", 1)
                    result.update(range(int(a), int(b) + 1, step))
                else:
                    result.update(range(int(base), hi + 1, step))
            elif "-" in part:
                a, b = part.split("-", 1)
                result.update(range(int(a), int(b) + 1))
            else:
                result.add(int(part))
        return {v for v in result if lo <= v <= hi}

    def match(self, dt: datetime) -> bool:
        if dt.minute not in self.minute:
            return False
        if dt.hour not in self.hour:
            return False
        if dt.month not in self.month:
            return False
        day_all = len(self.day) == 31  # 日字段为 *
        week_all = len(self.week) == 8  # 周字段为 *（0..7）
        wd = (dt.weekday() + 1) % 7  # 周一=1 ... 周日=0
        day_match = day_all or (dt.day in self.day)
        week_match = week_all or (wd in self.week)
        if not day_all and not week_all:
            # 标准 cron：日与周同时受限时，命中其一即算匹配
            return day_match or week_match
        return day_match and week_match

    def find_next(self, dt: datetime, limit_days: int = 366):
        """返回从 dt（含）之后、最接近的下一个触发时刻；找不到返回 None。"""
        d = dt.replace(second=0, microsecond=0)
        end = d + timedelta(days=limit_days)
        d += timedelta(minutes=1)
        while d <= end:
            if self.match(d):
                return d
            d += timedelta(minutes=1)
        return None


def _parse_hhmm(s: str) -> tuple[int, int]:
    s = s.strip().replace("：", ":")
    if ":" in s:
        hh, mm = s.split(":", 1)
        hh, mm = hh.strip(), mm.strip()
    else:
        if len(s) != 4 or not s.isdigit():
            raise ValueError("时间格式需为 HH:MM 或 HHMM，例如 08:30")
        hh, mm = s[:2], s[2:]
    if not (hh.isdigit() and mm.isdigit()):
        raise ValueError("时间需为数字")
    h, m = int(hh), int(mm)
    if not (0 <= h <= 23 and 0 <= m <= 59):
        raise ValueError("时间超出范围")
    return h, m


def parse_schedule(s: str) -> str:
    """把用户输入的时间语法解析为标准 5 段 cron；无法识别时抛 ValueError。

    支持：
      - 5 段 cron 表达式，如 "0 8 * * *"、"*/30 * * * *"
      - "每日 HH:MM"，如 "每日 08:30"
      - "每周X HH:MM"，如 "每周一 09:00"、"每周日 10:00"
    """
    s = s.strip()
    if not s:
        raise ValueError("定时时间不能为空。")
    parts = s.split()
    if len(parts) == 5:
        CronExpr(s)  # 校验合法性
        return s
    if s.startswith("每日") or s.startswith("每天"):
        h, m = _parse_hhmm(s[2:])
        return f"{m} {h} * * *"
    if s.startswith("每周"):
        rest = s[2:].strip()
        wd = WEEKDAY_MAP.get(rest[:1])
        if wd is None:
            raise ValueError("每周后需跟：一、二、三、四、五、六、日")
        h, m = _parse_hhmm(rest[1:])
        return f"{m} {h} * * {wd}"
    raise ValueError(
        "无法识别的定时格式。支持：5 段 cron（如 0 8 * * *）、每日 HH:MM（如 每日 08:30）、每周X HH:MM（如 每周一 09:00）"
    )
