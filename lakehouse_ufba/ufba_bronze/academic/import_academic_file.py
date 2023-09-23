import duckdb


def import_academic_to_ufba_bronze(
    duckdb_path: str,
    academic_path: str
) -> None:
    conn = duckdb.connect(duckdb_path)
    sql = f"""
    INSERT OR IGNORE INTO ufba_bronze.ufba_academica_0321
    SELECT
        cpf,
        mtr,
        inscrica,
        nome,
        per_ingr,
        cd_forma_ingr,
        descr_forma_ingr,
        per_saida,
        cd_forma_saida,
        descr_forma_saida,
        cr,
        escore,
        class_geral,
        categoria_class,
        cod_curso,
        per_crs_ini,
        nome_curso,
        colegiado,
        col_nm_colegiado,
        per_let_disc,
        disc,
        ch_disc,
        nat_disc,
        tur,
        nota,
        resultado,
        doc_nu_matricula_docente,
        doc_nm_docente,
        doc_vinculo,
        doc_titulacao,
        doc_nivel,
        doc_regime_trab
        nascimento,
        aln_cd_estado_civil,
        ecv_ds_estado_civil,
        sexo,
        dtnasc,
        aln_sg_estado_nascimento,
        aln_nm_pai,
        aln_nm_mae,
        aln_cd_cor,
        cor_nm_cor,
        aln_nm_cidade_nascimento,
        eda_nm_email,
        concat_ws(
            '_',
            coalesce(cpf::VARCHAR, '9999'),
            coalesce(mtr::VARCHAR, '9999'),
            coalesce(inscrica::VARCHAR, '9999'),
            per_ingr::VARCHAR,
            per_let_disc::VARCHAR,
            disc,
            coalesce(nota::VARCHAR, 9999),
            coalesce(resultado::VARCHAR, 9999),
            coalesce(doc_nu_matricula_docente, '9999'),
            coalesce(doc_nm_docente, '9999'),
            coalesce(doc_vinculo, '9999'),
            coalesce(doc_titulacao, '9999') ,
            coalesce(doc_nivel, '9999'),
            coalesce(doc_regime_trab, '9999')
        ) AS pk_id_academic
    FROM read_csv_auto('{academic_path}', delim=',', header=True)
    """
    conn.sql(sql)
    conn.commit()
    conn.close()
    