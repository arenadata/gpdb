import os
from typing import Optional
from gppylib.db import dbconn
from gppylib.commands.gp import *
from gppylib.commands.unix import *


def create_pid_file(coordinator_data_directory: str):
    with open(f"{coordinator_data_directory}/gprebalance.pid", "w") as fp:
        fp.write(str(os.getpid()))


def remove_pid_file(coordinator_data_directory: str):
    try:
        os.unlink(f"{coordinator_data_directory}/gprebalance.pid")
    except FileNotFoundError:
        pass


def check_running_gputils(dburl: dbconn.DbURL, coordinator_data_directory: str):
    """
    Checks if there are any running instances of
    gprebalance/gpbackup/gpexpand/gpshrink/gpresize
    """
    for util in ('gprebalance', 'gpexpand', 'gpshrink', 'gpresize'):
        try:
            with open(f'{coordinator_data_directory}/{util}.pid', 'r') as fp:
                pid = int(fp.readline().strip())
                if check_pid(pid):
                    raise Exception(f'{util} is already running.')
        except IOError:
            pass

    with closing(dbconn.connect(dburl, encoding='UTF8')) as conn:
        cursor = dbconn.query(conn, '''SELECT datid
                    FROM pg_stat_activity
                    WHERE application_name LIKE 'gpbackup%' OR
                    application_name LIKE 'gprestore%' ''')
        if cursor.rowcount > 0:
            raise Exception(
                '''gpbackup/gprestore utility is already running.''')

    if is_gprecoverseg_running():
        raise Exception(
            '''gprecoverseg is already running.''')
