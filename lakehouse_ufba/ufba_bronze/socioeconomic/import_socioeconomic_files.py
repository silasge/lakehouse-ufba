from pathlib import Path

import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash

from lakehouse_ufba.params import DuckDBFile, RawFiles
from lakehouse_ufba.utils import generate_hash_id


@task(cache_key_fn=task_input_hash)
def read_socioeconomic(file_path: str | Path) -> pd.DataFrame:
    return pd.read_spss(file_path)


@task(cache_key_fn=task_input_hash)
def pre_clean_socioeconomic_df(df_socioeconomic: pd.DataFrame) -> pd.DataFrame:
    # drop "filter_$" column
    if "filter_$" in df_socioeconomic.columns:
        df_socioeconomic = df_socioeconomic.drop(columns="filter_$")
    
    # all columns to lowercase
    df_socioeconomic = df_socioeconomic.rename(columns=lambda x: x.lower())
    
    df_cols_pre_hash_id = df_socioeconomic.columns
    
    # convert "insrica" and "cpf" to str
    for column in ["inscrica", "cpf"]:
        if column in df_socioeconomic.columns:
            df_socioeconomic[column] = df_socioeconomic[column].astype(str)
            
    id_cols = ["ano", "inscrica", "area", "curso"]
    
    if "cpf" in df_socioeconomic.columns:
        id_cols = id_cols + ["cpf"]
        
    # drop any duplicated rows
    df_socioeconomic = df_socioeconomic.drop_duplicates(subset=id_cols)
    
    # create hash_id
    df_socioeconomic["hash_id"] = generate_hash_id(df=df_socioeconomic, keys=id_cols)
    
    # reorder df to put hash_id in the beggining
    df_socioeconomic = (
        df_socioeconomic.loc[
            :, 
            ["hash_id"] + [col for col in df_socioeconomic.columns if col != "hash_id"]
        ])
            
    return df_socioeconomic


@task(cache_key_fn=task_input_hash)
def teste():
    pass


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

