"""5 段 cron 表达式解析与匹配 + 常用快捷语法。纯逻辑，可独立测试。"""

from __future__ import annotations

from datetime import datetime, timedelta

WEEKDAY_MAP = {"一": 1, "二": 2, "三": 3, "四": 4, "五": 5, "六": 6, "日": 0, "天": 0}


class CronExpr:
    """5 段 cron：分 时 日 月 周。

    周字段：0 或 7 表示周日，1-6 表示周一~周六。
    支持 *、n、a-b、*/n、a-b/n、a,b,c 语法。
    越界或非数字取值会抛 ValueError，避免产生「永不触发」的静默任务。
    """

    def __init__(self, expr: str):
        fields = expr.split()
        if len(fields) != 5:
            raise ValueError("cron 表达式需为 5 段（分 时 日 月 周），例如：0 8 * * *")
        self.expr = expr
        self.minute = self._parse(fields[0], 0, 59, "分")
        self.hour = self._parse(fields[1], 0, 23, "时")
        self.day = self._parse(fields[2], 1, 31, "日")
        self.month = self._parse(fields[3], 1, 12, "月")
        # 归一化：7 与 0 同为周日，因此 * 展开后落在 0-6 共 7 个取值
        self.week = {0 if v == 7 else v for v in self._parse(fields[4], 0, 7, "周")}

    @staticmethod
    def _int(token: str, label: str) -> int:
        try:
            return int(token.strip())
        except ValueError:
            raise ValueError(f"cron 的{label}字段包含非数字内容：{token.strip()!r}") from None

    @classmethod
    def _parse(cls, field: str, lo: int, hi: int, label: str) -> set[int]:
        result: set[int] = set()

        def add(value: int) -> None:
            if not lo <= value <= hi:
                raise ValueError(f"cron 的{label}字段数值 {value} 超出范围（{lo}-{hi}）")
            result.add(value)

        def add_range(start: int, stop: int, step: int = 1) -> None:
            if start > stop:
                raise ValueError(f"cron 的{label}字段区间起点大于终点：{start}-{stop}")
            for value in range(start, stop + 1, step):
                add(value)

        for part in field.split(","):
            part = part.strip()
            if not part:
                continue
            if part == "*":
                result.update(range(lo, hi + 1))
                continue
            if "/" in part:
                base, _, step_text = part.partition("/")
                step = cls._int(step_text, label)
                if step <= 0:
                    raise ValueError(f"cron 的{label}字段步长需大于 0：{part}")
                if base.strip() == "*":
                    result.update(range(lo, hi + 1, step))
                elif "-" in base:
                    a, _, b = base.partition("-")
                    add_range(cls._int(a, label), cls._int(b, label), step)
                else:
                    add_range(cls._int(base, label), hi, step)
            elif "-" in part:
                a, _, b = part.partition("-")
                add_range(cls._int(a, label), cls._int(b, label))
            else:
                add(cls._int(part, label))

        if not result:
            raise ValueError(f"cron 的{label}字段没有任何有效取值：{field!r}")
        return result

    def _day_matches(self, dt: datetime) -> bool:
        """判断日/月/周是否命中（不含时分）。"""
        if dt.month not in self.month:
            return False
        day_all = len(self.day) == 31
        week_all = len(self.week) == 7
        wd = (dt.weekday() + 1) % 7  # 周一=1 ... 周日=0
        day_match = day_all or (dt.day in self.day)
        week_match = week_all or (wd in self.week)
        if not day_all and not week_all:
            # 标准 cron：日与周同时受限时，命中其一即算匹配
            return day_match or week_match
        return day_match and week_match

    def match(self, dt: datetime) -> bool:
        if dt.minute not in self.minute:
            return False
        if dt.hour not in self.hour:
            return False
        return self._day_matches(dt)

    def find_next(self, dt: datetime, limit_days: int = 2923):
        """返回 dt 所在分钟之后、最接近的触发时刻；找不到返回 None。

        按天跳过不可能命中的日期，避免逐分钟扫描整年。
        搜索上限取 8 年，足以覆盖闰日（2/29）这类最长 4 年周期。
        """
        cur = dt.replace(second=0, microsecond=0) + timedelta(minutes=1)
        end = cur + timedelta(days=limit_days)
        while cur <= end:
            if not self._day_matches(cur):
                cur = (cur + timedelta(days=1)).replace(hour=0, minute=0)
                continue
            if cur.hour in self.hour and cur.minute in self.minute:
                return cur
            cur += timedelta(minutes=1)
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