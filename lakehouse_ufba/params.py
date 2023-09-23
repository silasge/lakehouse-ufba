class DuckDBFile:
    # database file
    DUCKDB = "data/lakehouse/ufba.duckdb"


class SQLFiles:    
    # schema file
    SCHEMA = "sql/schemas.sql"
    
    # ufba_bronze tables file
    UFBA_BRONZE = "sql/ufba_bronze_tables.sql"
    
    
class RawFiles:
    # socioeconomic files
    SOCIOECONOMIC = [
        'data/raw/socioeconomico/BI_Vestibular_2009_Inscritos.sav',
        'data/raw/socioeconomico/BI_Vestibular_2010_Inscritos.sav',
        'data/raw/socioeconomico/BI_Vestibular_2011_Inscritos.sav',
        'data/raw/socioeconomico/BI_Vestibular_2012_Inscritos.sav',
        'data/raw/socioeconomico/BI_Vestibular_2013_Inscritos.sav',
        'data/raw/socioeconomico/Vestibular_2005_inscritos.sav',
        'data/raw/socioeconomico/Vestibular_2006_inscritos.sav',
        'data/raw/socioeconomico/Vestibular_2007_inscritos.sav',
        'data/raw/socioeconomico/Vestibular_2008_Inscritos.sav',
        'data/raw/socioeconomico/Vestibular_2009_Inscritos.sav',
        'data/raw/socioeconomico/Vestibular_2010_Inscritos.sav',
        'data/raw/socioeconomico/Vestibular_2011_Inscritos.sav',
        'data/raw/socioeconomico/Vestibular_2012_Inscritos.sav',
        'data/raw/socioeconomico/Vestibular_2013_Inscritos.sav'
    ]
    
    # academic file
    ACADEMIC = "data/raw/academico/ufba_academica_0321.csv"
    