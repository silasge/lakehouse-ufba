import os

from prefect import flow, task
from sqlalchemy import create_engine

from lakehouse_ufba.db import ufba_bronze


@task
def get_sqlalchemy_engine():
    engine = create_engine(os.environ["MARIADB_CONN_STRING"])
    return engine


@flow
def create_tables():
    engine = get_sqlalchemy_engine()
    ufba_bronze.UfbaBronze.metadata.create_all(engine)


if __name__ == "__main__":
    create_tables()
    