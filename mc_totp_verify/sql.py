from logging import Logger
import sqlite3
from concurrent.futures import Future
from queue import Queue
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from mcdreforged import PluginServerInterface

@dataclass(frozen=True)
class StopQueue:
    pass

@dataclass(slots=True)
class SQLQuery:
    execute: str
    params: tuple[Any, ...]
    mode: Literal["exec", "one", "all"]
    fut: Future[Any]


class SQLi:
    def __init__(
            self,
            server: PluginServerInterface,
            sql_file_path: Path
        ) -> None:
        self.server: PluginServerInterface = server
        self.log: Logger = server.logger
        self.sql_file_path: Path = sql_file_path
        self.sql_queue: Queue[StopQueue | SQLQuery] = Queue()

    def _sqlite_worker(self, conn: sqlite3.Connection):
        self.log.info("SQLite Worker 队列已启动")
        while True:
            try:
                sql_query = self.sql_queue.get()
                if isinstance(sql_query, StopQueue):
                    self.sql_queue.task_done()
                    break
            except Exception:
                self.log.exception("SQLite Worker 队列发生错误")
                continue

            try:
                cur = conn.cursor()
                cur.execute(
                    sql_query.execute,
                    sql_query.params
                )
                try:
                    match sql_query.mode:
                        case "exec":
                            conn.commit()
                            result = cur.rowcount
                        case "one":
                            result = cur.fetchone()
                        case "all":
                            result = cur.fetchall()
                        case _:
                            raise ValueError(f"未知 mode: {sql_query.mode}")
                finally:
                    cur.close()
                if not sql_query.fut.done():
                    sql_query.fut.set_result(result)
            except Exception as e:
                conn.rollback()
                if not sql_query.fut.done():
                    sql_query.fut.set_exception(e)
            finally:
                self.sql_queue.task_done()

    def start(self):
