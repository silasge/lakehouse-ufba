from etl_ufba.params import DuckDBFile, RawFiles
from etl_ufba.ufba_bronze.academic.import_academic_file import \
    import_academic_to_ufba_bronze
from etl_ufba.ufba_bronze.socioeconomic.import_socioeconomic_files import \
    import_socioeconomic_to_ufba_bronze
from etl_ufba.utils import log


@log(log_file="logs/import_ufba_bronze.log")
def import_ufba_files_to_ufba_bronze(
    duckdb_path: str = DuckDBFile.DUCKDB,
    socioeconomic_path: str = RawFiles.SOCIOECONOMIC,
    academic_path: str = RawFiles.ACADEMIC
) -> None:
    import_socioeconomic_to_ufba_bronze(
        duckdb_path=duckdb_path,
        socioeconomic_path=socioeconomic_path
    )
    import_academic_to_ufba_bronze(
        duckdb_path=duckdb_path,
        academic_path=academic_path
    )
    
    
if __name__ == "__main__":
    import_ufba_files_to_ufba_bronze()
    