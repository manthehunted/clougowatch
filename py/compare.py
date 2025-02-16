import logging
import json
import sys
import re
from contextlib import contextmanager
from itertools import tee
import sqlite3

from typing import NewType

from dataclasses import dataclass


class JsonFormatter(logging.Formatter):
    default_time_format = "%Y-%m-%d %H:%M:%S"
    default_msec_format = "%s,%03d"

    @staticmethod
    def parse(fmt) -> list:
        patterns = re.findall("%\((?P<tag>\w+)\)(?P<dtype>\w)", fmt)
        assert len(patterns), f"supported format is `%(<name>)`. cannot parse {fmt}"
        return patterns

    @staticmethod
    def _new_format(lst: list[tuple[str, str]]) -> str:
        """
        format into json serialized string without the outermost curly brackets, {}.
        The outermost curly brackets will be added in function:`format`.
        """
        formats = []
        for tag, dtype in lst:
            match dtype:
                case "d" | "f":
                    fmt = "%({tag}){dtype}"
                case "s":
                    fmt = '"%({tag}){dtype}"'
                case _:
                    raise ValueError(dtype)
            formats.append(":".join(['"{tag}"', fmt]).format(tag=tag, dtype=dtype))
        return ",".join(formats)

    def __init__(
        self, fmt=None, datefmt=None, style="%", validate=True, *, defaults=None
    ):
        super().__init__(
            fmt=self._new_format(self.parse(fmt)),
            datefmt=datefmt,
            style=style,
            validate=validate,
            defaults=defaults,
        )

    def format(self, record) -> str:
        record.message = record.getMessage()
        if self.usesTime():
            record.asctime = self.formatTime(record, self.datefmt)
        output = self.formatMessage(record)

        s = ""
        if record.exc_info:
            # Cache the traceback text to avoid converting it multiple times
            # (it's constant anyway)
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            if s[-1:] != "\n":
                s = s + "\n"
            s = s + record.exc_text
        if record.stack_info:
            if s[-1:] != "\n":
                s = s + "\n"
            s = s + self.formatStack(record.stack_info)
        if s:
            output = ",".join([output, '"exc_info": "{s}"'.format(s=s)])
        return "{" + output + "}"


ch = logging.StreamHandler(sys.stdout)
json_formatter = JsonFormatter(
    "%(asctime)s %(name)s %(lineno)d %(levelname)s %(message)s"
)
ch.setFormatter(json_formatter)
ch.setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)
logger.addHandler(ch)
logger.setLevel(logging.DEBUG)


class DB:
    def __init__(self, name: str):
        self.session_maker = lambda: sqlite3.connect(name)
        self._conn = None

    @property
    def conn(self):
        if self._conn is None:
            _conn = self.session_maker()
            self._conn = _conn
        return self._conn

    @contextmanager
    def cursor(self):
        cur = self.conn.cursor()
        yield cur
        cur.close()

    def query(self, query) -> list:
        try:
            with self.cursor() as cur:
                cur.execute(query)
                return cur.fetchall()
        except Exception as e:
            logger.warning(e, exc_info=True)
            return []


@dataclass(frozen=True, slots=True)
class Patterned:
    PatternId: str
    logSamples: str
    pattern: str
    ratio: float
    regexString: str
    relatedPattern: str
    sampleCount: int
    severityLabel: str
    tokens: list[json]
    visualization: str

    def __str__(self):
        return self.pattern

    def to_pattern(self) -> str:
        return self.pattern.replace("<*>", "*")

    @classmethod
    def from_entry(cls, data: dict):
        kwargs = {}
        for k in [
            "@PatternId",
            "@logSamples",
            "@pattern",
            "@ratio",
            "@regexString",
            "@relatedPattern",
            "@sampleCount",
            "@severityLabel",
            "@tokens",
            "@visualization",
        ]:
            key = k.replace("@", "")
            v = data[k]
            match key:
                case "ratio":
                    v = float(v)
                case "sampleCount":
                    v = int(v)
                case "tokens":
                    v = json.loads(v)
                case _:
                    pass
            kwargs[key] = v
        return cls(**kwargs)


def get(cur, qid: str) -> dict:
    iter = map(
        Patterned.from_entry,
        json.loads(
            cur.execute(
                f"select result from results where queryid='{qid}';"
            ).fetchone()[0]
        ),
    )
    return {str(k): v for k, v in zip(*tee(iter, 2))}


Patterns = NewType("Patterns", tuple[list[Patterned], int])


def compare(r1: Patterns, r2: Patterns) -> set:
    rs1, t1 = r1
    rs2, t2 = r2
    if t1 > t2:
        diff = set(rs1) - set(rs2)
    else:
        diff = set(rs2) - set(rs1)
    return diff


def main(
    queryid: str,
    fs: str = "data.db",
):
    db = DB(fs)
    query = f"""
        select queryid, time
        from (
            with query AS (select query as this_query, groups as this_groups, time as this_time from queries where queryid = '{queryid}')
            select queryid, query, groups, time, rank() over (partition by query, groups order by time DESC) num_rank
            from queries
            inner join query on (query.this_query = query) AND (query.this_groups = groups)
            where time <= this_time)
        where num_rank < 3
    """
    res = db.query(query)

    assert len(res) < 3, "selected at most 2 entries, but not. check query above"

    if len(res) != 2:
        logger.debug("not enough data to proceed")
        return

    col = ["queryid", "time"]
    res = [dict(zip(col, entry)) for entry in res]
    res = [Patterns((get(db.conn, d["queryid"]), d["time"])) for d in res]
    for k in compare(*res):
        logger.info(f"{k}")

    return True


if __name__ == "__main__":
    logger.debug("run main")
    if len(sys.argv) == 2:
        # to load into ipython kernel
        logger.debug(f"invoke with {sys.argv[1]}")
        main(queryid=sys.argv[1])
    logger.debug("done main")
