import time
from functools import partial, wraps
from pathlib import Path
from typing import Callable

import duckdb
import pandas as pd
from loguru import logger


def read_sql_text_file(path: str | Path) -> str:
    with open(path) as f:
        sql = f.read()
    return sql


def insert_pandas_df_into_duckdb(
    df: pd.DataFrame,
    duckdb_path: str | Path,
    schema: str,
    table: str
) -> None:
    # init duckdb connection 
    conn = duckdb.connect(duckdb_path)
    
    # because the df is given as a argument to a function, we need to register it
    # so duckdb can find it inside the function
    duckdb.register("df", df)
    
    # now, we can define our sql statement, which will inser the df into duckdb
    sql = f"""
    INSERT OR IGNORE INTO {schema}.{table}
    SELECT * FROM df;
    """
    
    # finally, we can just execute the sql statement defined above 
    # and close the connection
    conn.sql(sql)
    conn.commit()
    conn.close()
    
    
def log(func: Callable = None, log_file: str = None):
    if func is None:
        return partial(log, log_file=log_file)
    
    @wraps(func)
    def decorator(*args, **kwargs):
        time_started = time.perf_counter()
        logger.add(log_file, level="INFO")
        logger.info(f"Function '{func.__name__}' logging started at '{log_file}' ...")
        try:
            obj = func(*args, **kwargs)
        except:
            time_finished = time.perf_counter()
            logger.opt(exception=True).info(
                f"Function {func.__name__} failed within {round(time_finished-time_started, 1)} seconds."
            )
        else:
            time_finished = time.perf_counter()
            logger.info(f"Function {func.__name__} finished within {round(time_finished-time_started, 1)}  seconds.")
            return obj
        
    return decorator
    
    