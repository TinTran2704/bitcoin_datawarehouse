# Version: v1.2 (add pandas_to_sql_extended)


# Standard imports
import os
import typing
import re


# Library imports
import pandas
import sqlalchemy
from autologging import logged
from sqlalchemy.orm.session import Session as SqlalchemySession


# Local imports


def get_abspath(relpath, _file__):
    """
    Execute *.sql file using relative path. This function fastens how we execute sql file using relative path.
        relpath: Relative path to the sql file.
        _file__: This must be __file__ all times. We don't set default parameter because we want current dir of the file being called (not this file).
    """
    folder_path = os.path.dirname(os.path.abspath(_file__))
    file_path = os.path.join(folder_path, relpath)
    return file_path


def read_file(relpath, _file__):
    """
    Read file using relative path
        relpath: Relative path to the file.
        _file__: This must be __file__ all times. We don't set default parameter because we want current dir of the file being called (not this file).
    """
    abspath = get_abspath(relpath, _file__)
    with open(abspath) as file_to_read:
        content = file_to_read.read()
    return content