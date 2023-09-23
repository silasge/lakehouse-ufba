import duckdb

from lakehouse_ufba.params import DuckDBFile, SQLFiles
from lakehouse_ufba.utils import log, read_sql_text_file


def read_and_execute_sql_files(
    sql_file_path: str, 
    duckdb_path: str = DuckDBFile.DUCKDB
)-> None:
    # init duckdb connection
    conn = duckdb.connect(str(duckdb_path))
    
    # read sql file 
    sql = read_sql_text_file(sql_file_path)
    
    # execute sql_file_path and close connection
    conn.sql(sql)
    conn.close()
    
@log(log_file="logs/config_db.log")
def run_duckdb_configs() -> None:
    # create schemas
    read_and_execute_sql_files(sql_file_path=SQLFiles.SCHEMA)
    
    #create ufba bronze tables
    read_and_execute_sql_files(sql_file_path=SQLFiles.UFBA_BRONZE)    
    

if __name__ == "__main__":
    run_duckdb_configs()
