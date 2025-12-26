# -*- coding: utf-8 -*-
"""
# ==============================================================
# SCRIPT ID         : AR_CRIPTOGRAFIA_LEGADO.py
# AUTHOR            : Alexandre Alvino
# VERSION           : 1.1
# Parametros        :
# CHANGE LOG :
# VER       DATE            WHO                     COMMENTS
# ==============================================================
# 1.0       23/05/2025      Alexandre Alvino 
# ==============================================================
# DAG responsável pela leitura de dados de uma base, onde é feita a criptografia
# com chave assimetrica + salt value. As chaves são recuperadas através do cofre de senhas.
# ==============================================================
@author: Alexandre Alvino
"""

import logging
import sys
import requests
import json
import tempfile
import base64
import pandas as pd
from datetime import datetime, date, timedelta
from multiprocessing.dummy import Pool as ThreadPool
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models.baseoperator import chain
from cryptography.hazmat.primitives import hashes, padding
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from google.cloud import storage, bigquery
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


logging.basicConfig(stream=sys.stdout, level=logging.INFO)

DAG_NAME = 'AR_CRIPTOGRAFIA_LEGADO'
default_args = {
    'owner': 'Squad-DA',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes = 2)
}

parm = Variable.get("PAR_AR_CRIPTOGRAFIA_LEGADO", deserialize_json=True)
table_id = parm['conf']['table_id']
KEY_SIZE = parm['conf']['key_size']
ITERATIONS = parm['conf']['iterations']
key_cript = parm['cofre']['key_cript']
salt_cript = parm['cofre']['salt_cript']
project_id = parm['conf']['project_id']
bucket_temp = parm['conf']['bucket']
control_table_id = f"{project_id}.log.log_carga_dados"


keys = {}
IV = bytes([0] * 16)

def acessa_cofre():
    logging.info("Acessando o cofre de senhas da aplicacao")
    global keys

    response = requests.post(
        url=parm['cofre']['post_url'],
        headers={'Accept-Encoding': 'base64'},
        data=parm['cofre']['key'],
        verify=False
    )

    response.raise_for_status()
    token = response.text

    variable_ids = (parm['cofre']['identifier'].replace("/", "%2F") + '%2Fusername' + "," +
                    parm['cofre']['identifier'].replace("/", "%2F") + '%2Fpassword')

    url = parm['cofre']['get_url'] + variable_ids
    response = requests.get(url=url, headers={'Authorization': 'Token token="' + token + '"'}, verify=False)
    response.raise_for_status()

    keys['cofre'] = response.json()
    logging.info("Chaves recuperadas com sucesso")


def derive_key(secret_key, salt):
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=KEY_SIZE,
        salt=salt.encode(),
        iterations=ITERATIONS,
        backend=default_backend()
    )
    return kdf.derive(secret_key.encode())


def processa_chunks(data, chunk_idx, secret_key, salt):

    df = pd.DataFrame(data, columns=["CD_CPF_CRIPTOGRAFADO", "ID_GPON", "DT_FONTE", "DS_BASE_FONTE"])
    key = derive_key(secret_key, salt)

    def doEncrypt(value):
        if not value:
            return ''
        padder = padding.PKCS7(128).padder()
        padded_data = padder.update(value.encode('utf-8')) + padder.finalize()
        cipher = Cipher(algorithms.AES(key), modes.CBC(IV), backend=default_backend())
        encryptor = cipher.encryptor()
        encrypted = encryptor.update(padded_data) + encryptor.finalize()
        return base64.b64encode(encrypted).decode('utf-8')

    with ThreadPool(processes=4) as pool:
        df['CD_CPF_CRIPTOGRAFADO'] = pool.map(doEncrypt, df['CD_CPF_CRIPTOGRAFADO'])

    tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    df.to_csv(tmp_file.name, index=False, header=False)

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_temp)
    blob_path = f"legado_tmp/cripto_chunk_{chunk_idx}.csv"
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(tmp_file.name)

    return f"gs://{bucket_temp}/{blob_path}", tmp_file.name

def check_max_dt(**kwargs):
    bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default', use_legacy_sql=False)
    client = bq_hook.get_client()
    ti = kwargs['ti']
    
    ti.xcom_push(key='execution_date', value=kwargs['dag_run'].execution_date.isoformat())

    source_max_dt_sql = f"""
    SELECT MAX(dt_carga) as max_dt
    FROM (
        SELECT max(FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%Y%m%d', CAST(DT_BASE AS STRING))) ) AS dt_carga FROM `{project_id}.landzone.et_base_legado`
        UNION ALL
        SELECT max(FORMAT_DATE('%Y-%m-%d', DATE(DATA_ATUALIZACAO))) AS dt_carga FROM `{project_id}.trusted.tb_base_legado`
    )
    """

    source_max_dt_job = client.query(source_max_dt_sql)
    source_max_dt_result = source_max_dt_job.result()
    source_max_dt = None

    for row in source_max_dt_result:
        source_max_dt = row.max_dt
    
    if source_max_dt:
        if isinstance(source_max_dt, pd.Timestamp):
            source_max_dt = source_max_dt.to_pydatetime()
        elif isinstance(source_max_dt, date) and not isinstance(source_max_dt, datetime):
            source_max_dt = datetime.combine(source_max_dt, datetime.min.time())
    
    logging.info(f"Maior data nas tabelas de origem bov: {source_max_dt}")

    final_max_dt_sql = f"SELECT MAX(dt_fonte) as max_dt FROM `{table_id}`"

    final_max_dt_job = client.query(final_max_dt_sql)
    final_max_dt_result = final_max_dt_job.result()
    final_max_dt = None

    for row in final_max_dt_result:
        final_max_dt = row.max_dt

    if final_max_dt:
        if isinstance(final_max_dt, pd.Timestamp):
            final_max_dt = final_max_dt.to_pydatetime()
        elif isinstance(final_max_dt, date) and not isinstance(final_max_dt, datetime):
            final_max_dt = datetime.combine(final_max_dt, datetime.min.time())
    
    logging.info(f"Maior data na tabela tb_base_legado_cripto: {final_max_dt}")

    if source_max_dt is None:
        logging.warning("Sem data nas tabelas de origem.")
        ti.xcom_push(key='exec_carga_tb', value=False)
        ti.xcom_push(key='message', value='Nenhuma data de carga encontrada nas tabelas de origem.')
    elif final_max_dt is None or source_max_dt > final_max_dt:
        logging.info(f"Dados novos encontrados (Origem: {source_max_dt}, Destino: {final_max_dt}). A task 'carga_tabela' sera executada!")
        ti.xcom_push(key='exec_carga_tb', value=True)
        ti.xcom_push(key='message', value='Novos dados para carga. (Max dt da origem: {source_max_dt}, Max dt da tabela final antes da carga: {final_max_dt})')
    else:
        logging.info(f"Nenhum dado mais recente encontrado (Origem: {source_max_dt}, Destino: {final_max_dt}). A task 'carga_tabela' sera pulada.")
        ti.xcom_push(key='exec_carga_tb', value=False)
        ti.xcom_push(key='message', value='Nenhum dado mais recente para carga. (Maior data na origem: {source_max_dt}, Maior data no destino: {final_max_dt})')


def carga_tabela(**kwargs):
    client = None
    temp_table_id = None
    tmp_files = []
    gcs_uris = []
    
    ti = kwargs['ti']
    ti.xcom_push(key='carga_status', value='FAILED') 

    exec_carga_tb = ti.xcom_pull(task_ids='check_max_dt', key='exec_carga_tb')

    if not exec_carga_tb:
        logging.info("A carga da tabela foi pulada, conforme determinado pela task 'check_max_dt'.")
        ti.xcom_push(key='carga_status', value='SKIPPED')
        return 

    try:
        logging.info("Iniciando carga da tabela com criptografia")

        bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default', use_legacy_sql=False)
        client = bq_hook.get_client()

        session_id = ti.run_id.replace(':', '_').replace('-', '_').replace('+', '_').replace('.', '_')
        temp_table_name = f"temp_base_legado_cripto_{session_id}"
        temp_table_id = f"{project_id}.domain.{temp_table_name}"

        
        sql_tmp = f"""
        CREATE OR REPLACE TABLE `{temp_table_id}` AS
        SELECT DISTINCT numero_documento AS CD_CPF_CRIPTOGRAFADO, acesso_gpon AS ID_GPON, FORMAT_DATE('%Y-%m-%d', CURRENT_DATE()) AS DT_FONTE , '{project_id}.landzone.et_base_legado' AS DS_BASE_FONTE
        FROM `{project_id}.landzone.et_base_legado`
        UNION ALL
        SELECT DISTINCT CPF AS CD_CPF_CRIPTOGRAFADO, ACESSO_ID AS ID_GPON, FORMAT_DATE('%Y-%m-%d', CURRENT_DATE()) AS DT_FONTE, '{project_id}.trusted.tb_base_legado' AS DS_BASE_FONTE
        FROM `{project_id}.trusted.tb_base_legado` WHERE acesso_id NOT IN (SELECT DISTINCT acesso_gpon FROM `{project_id}.landzone.et_base_legado` )
        """
        job_tmp = client.query(sql_tmp)
        job_tmp.result()

        logging.info("Criacao da tabela tmp concluida com os dados da et_base_legado e tb_base_legado")

        logging.info(f"Apagando dados existentes da tabela de destino {table_id}")
        client.query(f"DELETE FROM `{table_id}` WHERE 1=1").result()

        count_query = client.query(f"SELECT COUNT(1) as total_rows FROM `{table_id}`")
        rows_after_delete = count_query.result().to_dataframe().iloc[0]['total_rows']

        if rows_after_delete == 0:
            logging.info(f"DELETE OK: A tabela {table_id} limpa e pronta para carga!")
        else:
            logging.error(f"DELETE FALHOU: A tabela {table_id} nao foi limpa. Contem {rows_after_delete} linhas.")
            raise ValueError(f"A limpeza da tabela {table_id} falhou!")

        acessa_cofre()
        secret_key = keys['cofre'][key_cript]
        salt = keys['cofre'][salt_cript]
        
        # chunk para processamento devido a volumetria de dados de PRD
        chunk_size = 250_000
        chunk_idx = 0
        
        logging.info(f"Iniciando leitura da tabela {temp_table_id}")
        rows_iterator = client.list_rows(temp_table_id)
        
        chunk = []
        for row in rows_iterator:
            chunk.append(list(row.values()))
            if len(chunk) >= chunk_size:
                logging.info(f"Processando chunk {chunk_idx + 1} com {len(chunk)} registros")
                gcs_uri, tmp_file_path = processa_chunks(chunk, chunk_idx, secret_key, salt)
                gcs_uris.append(gcs_uri)
                tmp_files.append(tmp_file_path)
                
                chunk_idx += 1
                chunk = []

        if chunk:
            logging.info(f"Processando chunk final {chunk_idx + 1} com {len(chunk)} registros")
            gcs_uri, tmp_file_path = processa_chunks(chunk, chunk_idx, secret_key, salt)
            gcs_uris.append(gcs_uri)
            tmp_files.append(tmp_file_path)

        table_schema = [
            bigquery.SchemaField("CD_CPF_CRIPTOGRAFADO", "STRING"),
            bigquery.SchemaField("ID_GPON", "STRING"),
            bigquery.SchemaField("DT_FONTE", "STRING"),
            bigquery.SchemaField("DS_BASE_FONTE", "STRING"),
        ]

        job_config = bigquery.LoadJobConfig(
            schema=table_schema,
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=0,
            autodetect=False,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

        for uri in gcs_uris:
            logging.info(f"Carregando {uri} para a tabela {table_id}")
            load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
            load_job.result()
            logging.info(f"Carregamento concluido para {uri}")
        
        ti.xcom_push(key='carga_status', value='SUCCESS') 
            
    except Exception as e:
        logging.exception("Erro ao executar carga_tabela")
        ti.xcom_push(key='carga_status', value='FAILED') 
        raise

    finally:

        if client and temp_table_id:
            logging.info(f"Removendo tabela tmp do BigQuery: {temp_table_id}")
            client.delete_table(temp_table_id, not_found_ok=True)
        
        if tmp_files:
            logging.info("Iniciando limpeza de arquivos tmp locais.")
            for f in tmp_files:
                try:
                    os.remove(f)
                    logging.info(f"Arquivo tmp local removido: {f}")
                except Exception as e:
                    logging.warning(f"Erro ao remover arquivo tmp local {f}: {e}")
        
        if gcs_uris:
            logging.info("Iniciando limpeza dos arquivos tmp no GCS (legado_tmp)...")
            storage_client = storage.Client()
            for uri in gcs_uris:
                try:
                    if uri.startswith("gs://"):
                        path_parts = uri.replace("gs://", "").split("/")
                        bucket_name = path_parts[0]
                        blob_name = "/".join(path_parts[1:])
                        
                        bucket = storage_client.bucket(bucket_name)
                        blob = bucket.blob(blob_name)
                        blob.delete()
                        logging.info(f"Arquivo tmp removido do GCS: {uri}")
                except Exception as e:
                    logging.warning(f"Erro ao remover arquivo {uri} do GCS: {e}")

def grava_log_lg(**kwargs):
    bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default', use_legacy_sql=False)
    client = bq_hook.get_client()
    ti = kwargs['ti']

    dag_id = DAG_NAME
    run_id = kwargs['dag_run'].run_id
    execution_date_str = ti.xcom_pull(task_ids='check_max_dt', key='execution_date')
    execution_date = datetime.fromisoformat(execution_date_str) if execution_date_str else None
    
    start_time = kwargs['dag_run'].start_date
    end_time = datetime.now()
    
    message = ti.xcom_pull(task_ids='check_max_dt', key='message')

    carga_status = ti.xcom_pull(task_ids='carga_tabela', key='carga_status')

    status = 'FAILED' 
    if carga_status == 'SUCCESS':
        status = 'SUCCESS'
        message = message if message else 'Carga de dados concluida com sucesso.'
    elif carga_status == 'SKIPPED':
        status = 'SKIPPED'
        message = message if message else 'Carga de dados pulada: nenhum dado mais recente.'
    else: 
        check_max_dt_state = ti.get_upstream_task_instance(task_id='check_max_dt').current_state()
        if check_max_dt_state == 'failed':
            status = 'FAILED'
            message = message if message else 'Verificacao do max dt falhou.'
        elif carga_status == 'FAILED': 
            status = 'FAILED'
            message = message if message else 'Carga de dados falhou.'
        else:
            status = 'FAILED'
            message = message if message else 'Carga falhou!'


    log_sql = f"""
    INSERT INTO `{control_table_id}` (no_processo, id_execucao, dh_carga, ds_status, dh_ini_execucao, dh_fim_execucao, ds_message)
    VALUES ('{dag_id}', '{run_id}', '{execution_date}', '{status}', '{start_time}', '{end_time}', '{message}')
    """

    try:
        client.query(log_sql).result()
        logging.info("Status de execucao registrado com sucesso na LG.")
    except Exception as e:
        logging.error(f"Erro ao registrar status de execucao na LG: {e}")


with DAG(
    DAG_NAME,
    tags=['BASELEGADO'],
    start_date=datetime(2025,5,24),
    description="DAG responsavel pela criptografia dos registros legado",
    default_args=default_args,
    schedule_interval='0 23 * * *',
    catchup=False,
    max_active_runs=1
) as dag:

    begin = DummyOperator(task_id="begin")

    task_check_max_dt = PythonOperator(
        task_id='check_max_dt',
        python_callable=check_max_dt,
        provide_context=True,
    )

    task_carga_tabela = PythonOperator(
        task_id='carga_tabela',
        python_callable=carga_tabela,
        provide_context=True, 
        trigger_rule='all_done', 
    )

    task_grava_log_lg = PythonOperator(
        task_id='grava_log_lg',
        python_callable=grava_log_lg,
        provide_context=True,
        trigger_rule='all_done', 
    )

    end = DummyOperator(task_id="end")

    chain(
        begin,
        task_check_max_dt,
        task_carga_tabela,
        task_grava_log_lg,
        end
    )