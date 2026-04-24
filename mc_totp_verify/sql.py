from logging import Logger
import sqlite3
from concurrent.futures import Future
from queue import Queue
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Literal

from mcdreforged import ServerInterface

SQL_MODE = Literal["exec", "one", "all"]

@dataclass(frozen=True)
class StopQueue:
    pass

@dataclass(slots=True)
class SQLQuery:
    execute: str
    parameters: tuple[Any, ...]
    mode: SQL_MODE
    future: Future[Any]


class SQLi:
    def __init__(
            self,
            server: ServerInterface,
            sql_file_path: Path
        ) -> None:
        self.server: ServerInterface = server
        self.log: Logger = server.logger
        self.sql_file_path: Path = sql_file_path
        self.sql_queue: Queue[StopQueue | SQLQuery] = Queue()

    def _process_query(self, conn: sqlite3.Connection, sql_query: SQLQuery) -> None:
        try:
            cur = conn.cursor()
            try:
                cur.execute(sql_query.execute, sql_query.parameters)
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

                if not sql_query.future.done():
                    sql_query.future.set_result(result)
            finally:
                cur.close()
        except Exception as e:
            if sql_query.mode == "exec":
                conn.rollback()
            if not sql_query.future.done():
                sql_query.future.set_exception(e)

    def _sqlite_worker(self, conn: sqlite3.Connection) -> None:
        self.log.info("SQLite Worker 队列已启动")
        try:
            while True:
                item = self.sql_queue.get()
                try:
                    if isinstance(item, StopQueue):
                        break
                    self._process_query(conn, item)
                finally:
                    self.sql_queue.task_done()
        finally:
            conn.close()
            self.log.info("SQLite Worker 队列已关闭")

    def start(self) -> None:
        conn = sqlite3.connect(self.sql_file_path)
        self._sqlite_worker(conn)

    def execute(
            self,
            sql: str,
            parameters: tuple[Any, ...] = (),
            /,
            *,
            mode: SQL_MODE,
            future_callback: Callable[[Future[Any]], None] | None = None,
        ) -> Future:
        fut = Future()
        self.sql_queue.put_nowait(
            SQLQuery(
                execute=sql,
                parameters=parameters,
                mode=mode,
                future=fut
            )
        )
        if future_callback is not None:
            fut.add_done_callback(future_callback)
        return fut

    def stop(self) -> None:
        self.sql_queue.put_nowait(StopQueue())