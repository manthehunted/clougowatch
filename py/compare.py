import logging
from pathlib import Path
import argparse
import json
import sys
import re
from contextlib import contextmanager
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


BCOLORS = dict(
    HEADER="\033[95m",
    OKBLUE="\033[94m",
    OKCYAN="\033[96m",
    OKGREEN="\033[92m",
    WARNING="\033[93m",
    FAIL="\033[91m",
    ENDC="\033[0m",
    BOLD="\033[1m",
    UNDERLINE="\033[4m",
)


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
            logger.warning(f"while executing {query}: {e}", exc_info=True)
            return []


## AWS CloudWatch related
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


Patterns = NewType("Patterns", tuple[list[Patterned], int])


@dataclass(frozen=True, slots=True)
class AuditEntries:
    log_group: str
    queryid: str
    patterns: list[str]

    @classmethod
    def from_db_result(cls, queryid: str, db: DB):
        q = """
        WITH tmp AS (
            select id, queryid, result from results where queryid='{qid}'
        )
        select queries.groups as log_group, tmp.queryid, tmp.result
        from tmp
        inner join queries on queries.queryid = tmp.queryid;
        """.format(qid=queryid)
        data = db.query(q)
        assert len(data) == 1, len(data)
        data = data[0]
        output = dict(
            zip(
                [
                    "log_group",
                    "queryid",
                ],
                data[:-1],
            )
        )
        res = list(
            map(
                Patterned.from_entry,
                (json.loads(data[-1])),
            )
        )
        output["patterns"] = res
        return cls(**output)

    def __iter__(self):
        for entry in self.patterns:
            yield {
                "log_group": self.log_group,
                "queryid": self.queryid,
                "pattern": entry,
            }


def setup_db_audits_vetted(db: DB, delete_existing: bool = False):
    delete_audits = """
    drop table if exists audits;
    """

    create_audits = """
    create table if not exists audits (
        id integer not null,
        pattern_str blob not null,
        log_group text not null,
        queryid text not null,
        vetted integer default 0,
        PRIMARY KEY (pattern_str, log_group)
        -- FOREIGN KEY(log_group) REFERENCES queries(groups)
    );
    """

    delete_vetted = """
    drop table if exists vetted;
    """

    create_vetted = """
    create table if not exists vetted (
        id integer not null,
        pattern_str text not null,
        time_auditted timestamp,
        time_last_seen timestamp,
        log_group text not null,
        PRIMARY KEY (pattern_str, log_group)
        -- FOREIGN KEY(id, pattern_str, log_group) REFERENCES audits(id, pattern_str, log_group)
        -- FOREIGN KEY (log_group) REFERENCES audits(log_group) -- commented out since log_group is not unique
        -- on delete cascade
    );
    """

    delete_trigger1 = """
    drop TRIGGER if exists vetted_pattern;
    """

    create_trigger1 = """
    CREATE TRIGGER if not exists vetted_pattern
    AFTER UPDATE ON audits
    when old.vetted <> new.vetted
    begin
    insert into vetted (
        id,
        pattern_str,
        time_auditted,
        time_last_seen,
        log_group
    )
    values(
            old.id,
            old.pattern_str,
            DATETIME('NOW'),
            DATETIME('NOW'), -- FIXME:
            old.log_group
    );
    END
    ;
    """

    # delete_trigger2 = """
    # drop TRIGGER if exists delete_audit;
    # """

    # create_trigger2 = """
    # CREATE TRIGGER if not exists delete_audit
    # AFTER insert ON vetted
    # BEGIN
    #     delete from audits where id in (select id from vetted);
    # END
    # ;
    # """

    scripts = [
        delete_audits,
        delete_vetted,
        delete_trigger1,
        # delete_trigger2,
        create_audits,
        create_vetted,
        create_trigger1,
        # create_trigger2,
    ]

    if not delete_existing:
        scripts = scripts[int(len(scripts) / 2) :]

    conn = db.conn
    cur = conn.cursor()
    cur.execute("PRAGMA foreign_keys = ON;")
    for script in scripts:
        try:
            cur.execute(script)
        except Exception as e:
            logger.warning(f"while executing {script}: {e}", exc_info=True)
            return False
    conn.commit()
    return True


def audit_list(db, group_names: None | list[str] = None) -> None:
    s = ",".join(map(lambda x: f"'{x}'", group_names))
    for qid in db.query(
        f"select queryid from queries where groups in ({s}) and query like '%pattern%';"
    ):
        qid = qid[0]
        data = AuditEntries.from_db_result(qid, db)
        logger.debug(f"insert queryid={qid} with len(data)={len(data.patterns)}")
        create_or_insert_to_audits(db, data)

    _audit_list(db, group_names)


def _audit_list(db, group_names: None | list[str] = None) -> None:
    condition = " where vetted = 0"
    if group_names:
        condition += " and log_group in ({})".format(
            ",".join(map(lambda x: "'{}'".format(x), group_names))
        )
    else:
        condition += ""

    results = db.query("select id, pattern_str from audits" + condition)

    if len(results) == 0:
        sys.stdout.write("no entry to audit\n")
    else:
        for i in results:
            sys.stdout.write(
                f"{BCOLORS['BOLD']}Audit id={BCOLORS['OKBLUE']}{i[0]}{BCOLORS['ENDC']}, pattern={BCOLORS['OKGREEN']}{i[1]}{BCOLORS['ENDC']}\n"
            )


# raise sqlite3
def audit_mark(db, ids: list[int]) -> bool:
    logger.debug("mark")
    conn = db.conn
    try:
        conn.executemany(
            "update audits set vetted = 1 where id in (?)", [[x] for x in ids]
        )
    except sqlite3.ProgrammingError as err:
        conn.rollback()
        logger.warning(f"rollback due to {err}", exc_info=True)
        return False

    conn.commit()
    return True


def create_or_insert_to_audits(db: DB, data: AuditEntries):
    idx = db.query("select max(rowid) from audits;")
    s = 0
    if idx:
        if idx[0][0]:
            s = idx[0][0]
    conn = db.conn
    for idx, res in enumerate(data):
        res: Patterned = res
        to_insert = {
            "id": s + idx,
            "pattern_str": json.dumps(str(res["pattern"])).encode(),
            "log_group": res["log_group"],
            "queryid": res["queryid"],
            "vetted": 0,
        }

        try:
            conn.execute(
                "insert into audits values(:id, :pattern_str, :log_group, :queryid, :vetted)",
                to_insert,
            )
        except sqlite3.IntegrityError as err:
            if "UNIQUE constraint failed" in str(err):
                logger.debug("the same entry already exists.")
            else:
                logger.info("rollback", exc_info=True)
            conn.rollback()
        except Exception as e:
            logger.warning(f"while insert into audits: {e}", exc_info=True)

    conn.commit()
    return True


if __name__ == "__main__":
    """
    Motivation
    ==========
    Audit and vet CloudWatch pattern, queried by keyword=`pattern` to
    - find any new pattern(s) from a time period to another
        - if auditted, the pattern is vetted and will not be present in future
        - otherwise, analyze and resolve the pattern and its logs
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("db", default="data.db", type=Path)
    subparser = parser.add_subparsers()
    parse_audit = subparser.add_parser("audit")
    parse_audit.add_argument("--list", nargs="+", type=str, default=None)
    parse_audit.add_argument("--mark", nargs="+", type=int, default=None)

    args = parser.parse_args()
    db = DB(args.db)

    setup_db_audits_vetted(db)
    if args.list is not None:
        audit_list(db, args.list)
    elif args.mark is not None:
        audit_mark(db, args.mark)
    else:
        raise RuntimeError("unreachable")
