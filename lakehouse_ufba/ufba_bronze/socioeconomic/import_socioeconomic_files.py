from pathlib import Path

import pandas as pd

from lakehouse_ufba.params import DuckDBFile, RawFiles
from lakehouse_ufba.utils import insert_pandas_df_into_duckdb, log


def read_socioeconomic(file_path: str | Path) -> pd.DataFrame:
    return pd.read_spss(file_path)


def pre_clean_socioeconomic_df(df_socioeconomic: pd.DataFrame) -> pd.DataFrame:
    # drop "filter_$" column
    if "filter_$" in df_socioeconomic.columns:
        df_socioeconomic = df_socioeconomic.drop(columns="filter_$")
    
    # all columns to lowercase
    df_socioeconomic = df_socioeconomic.rename(columns=lambda x: x.lower())
    
    # convert "insrica" and "cpf" to str
    for column in ["inscrica", "cpf"]:
        if column in df_socioeconomic.columns:
            df_socioeconomic[column] = df_socioeconomic[column].astype(str)
            
    columns_to_test_for_dups = ["ano", "inscrica", "area", "curso"]
    
    if "cpf" in df_socioeconomic.columns:
        columns_to_test_for_dups = columns_to_test_for_dups + ['cpf']
        
    df_socioeconomic = df_socioeconomic.drop_duplicates(subset=columns_to_test_for_dups)
            
    return df_socioeconomic


@log(log_file="logs/import_ufba_bronze_socioeco.log")
def import_socioeconomic_to_ufba_bronze(
    duckdb_path: Path,
    socioeconomic_path: Path
) -> None:
    # looping over socieconomic files 
    for socioeconomic_file in socioeconomic_path:
        # getting table names
        table_name = socioeconomic_file.split("/")[-1].split(".")[0].lower()
        
        # read the file
        df = read_socioeconomic(file_path=socioeconomic_file)
        df = pre_clean_socioeconomic_df(df_socioeconomic=df)
        insert_pandas_df_into_duckdb(
            df=df,
            duckdb_path=duckdb_path,
            schema="ufba_bronze",
            table=table_name
        )
        

if __name__ == "__main__":
    import_socioeconomic_to_ufba_bronze(
        duckdb_path=DuckDBFile.DUCKDB,
        socioeconomic_path=RawFiles.SOCIOECONOMIC
    )

