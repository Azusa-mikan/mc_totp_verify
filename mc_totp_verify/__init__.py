import threading

from mcdreforged import PluginServerInterface

from mc_totp_verify.sql import SQLi


def on_load(server: PluginServerInterface, prev):
    """
    插件加载
    """