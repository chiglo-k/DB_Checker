from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import requests
import pytz

local_tz = pytz.timezone("Asia/Magadan")

with DAG(
    dag_id='migrate_data_check',
    start_date=datetime(2025,4,4, tzinfo=local_tz),
    schedule='0 10,17 * * *',
    catchup=False
) as dag:

    start = DummyOperator(task_id='start')
    clear_table = PostgresOperator(
        task_id='clear_db',
        postgres_conn_id='postgres_oved',
        sql="""TRUNCATE TABLE check_sys.daily_check;"""
    )
    end = DummyOperator(task_id='end')

    tasks = {
        'fesco_paid_bills' : ('Неоплаченные счета Fesco: ',
                              "SELECT COUNT(*) FROM public.fesco_bill WHERE payment IS NULL"),
        'bd_space_lost' : ('Пропущенные значения в DB: ',
                           "SELECT COUNT(*) FROM public.conosaments_null"),
        'remain_storage' : ('Остатки на складе <Внутренний рынок>: ',
                            "SELECT COUNT(*) FROM public.inner_save WHERE files LIKE '%' || EXTRACT(YEAR FROM CURRENT_DATE) || '%'"),
        'export_not_closed': ('Незакрытые экспортные дополнения: ',
                              "SELECT COUNT(*) FROM public.bl_check_contract_all WHERE contract_storage NOT LIKE '%711%'")
    }

    start >> clear_table >> end

    insert_tasks = []

    for task_id, (name, query) in tasks.items():

        check_task = PostgresOperator(
            task_id=task_id,
            postgres_conn_id='postgres_oved',
            sql=query,
            do_xcom_push=True
        )

        insert_values = PostgresOperator(
            task_id=f'insert_{task_id}',
            postgres_conn_id='postgres_oved',
            sql=f"""
                INSERT INTO check_sys.daily_check (name_of_warning, count_of_warning)
                VALUES ('{name}', {{{{ ti.xcom_pull(task_ids='{task_id}')[0][0] }}}});
                """
        )



        start >> check_task >> insert_values >> end
