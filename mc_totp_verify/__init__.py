import threading
from pathlib import Path

from mcdreforged import PluginServerInterface

from mc_totp_verify.sql import SQLi

sqlite = None

sql_thread = None
sql_thread_name = "mc_totp_verify_sqlite"


def on_load(server: PluginServerInterface, prev):
    """
    插件加载
    """
    sqlite_path = Path(server.get_data_folder(), "mtv.db")

    global sqlite, sql_thread
    sqlite = SQLi(
        server=server.as_basic_server_interface(),
        sql_file_path=sqlite_path
    )

    sql_thread = threading.Thread(
        target=sqlite.start,
        name=sql_thread_name
    )
    sql_thread.start()
    server.logger.info("插件已加载")

def on_unload(server: PluginServerInterface):
    global sqlite, sql_thread
    if sqlite is not None:
        sqlite.stop()
    
    if sql_thread is not None:
        sql_thread.join(timeout=10)
        if sql_thread.is_alive():
            server.logger.warning("SQLite 未关闭")
    
    sqlite = None
    sql_thread = None

    server.logger.info("插件已卸载")