# Standard imports
import os
from contextlib import contextmanager
import logging


# Library imports
from autologging import logged
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker


# Local imports

@logged
class database_connection_abstract(object):
    @classmethod
    @contextmanager
    def session_scope(cls):
        cls.__log.debug('START session to %s', cls.url.__repr__())
        session = cls.session()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()
            cls.__log.debug('CLOSED session to %s', cls.url.__repr__())


@logged
class pg_data_lake(object):
    url = URL(
        drivername = 'postgresql+psycopg2',
        username = os.environ.get('PG_DATA_LAKE_USERNAME'),
        password = os.environ.get('PG_DATA_LAKE_PASSWORD'),
        host = os.environ.get('PG_DATA_WAREHOUSE_HOST'),
        port = os.environ.get('PG_DATA_WAREHOUSE_PORT'),
        database = os.environ.get('PG_DATA_WAREHOUSE_DATABASE'),
    )
    engine = create_engine(url)
    session = sessionmaker(bind = engine)

    @classmethod
    @contextmanager
    def session_scope(cls):
        cls.__log.debug('START session to %s', cls.url.__repr__())
        session = cls.session()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()
            cls.__log.debug('CLOSED session to %s', cls.url.__repr__())


@logged
class pg_data_warehouse(object):
    url = URL(
        drivername = 'postgresql+psycopg2',
        username = os.environ.get('PG_DATA_WAREHOUSE_USERNAME'),
        password = os.environ.get('PG_DATA_WAREHOUSE_PASSWORD'),
        host = os.environ.get('PG_DATA_WAREHOUSE_HOST'),
        port = os.environ.get('PG_DATA_WAREHOUSE_PORT'),
        database = os.environ.get('PG_DATA_WAREHOUSE_DATABASE'),
    )
    engine = create_engine(url)
    session = sessionmaker(bind = engine)
    
    @classmethod
    @contextmanager
    def session_scope(cls):
        cls.__log.debug('START session to %s', cls.url.__repr__())
        session = cls.session()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()
            cls.__log.debug('CLOSED session to %s', cls.url.__repr__())