from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.utils.dates import days_ago


def run_etl():
    import psycopg2
    import requests
    import sys

    conn = psycopg2.connect(host='host.docker.internal', database='airflow', user='airflow', password='airflow')
    cur = conn.cursor()

    sess = requests.session()
    resp = sess.get('https://statsapi.web.nhl.com/api/v1/teams/21/stats')

    cur.execute('INSERT INTO aerode.stage_json VALUES (%s)', (resp.text,))
    cur.execute('SELECT aerode.f_parse_statistics()')

    result = cur.fetchone()[0]

    if result == 1:
        cur.execute('TRUNCATE aerode.stage_json')
    else:
        sys.exit('Unexpected error has occured')

    cur.connection.commit()
    cur.close()
    cur.connection.close()


with DAG(
    dag_id='Run_ETL',
    schedule_interval='0 */12 * * *',
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1
) as dag:

    etl_task = PythonVirtualenvOperator(
        task_id="etl_task",
        python_callable=run_etl,
        requirements=["psycopg2-binary", "requests"],
        system_site_packages=True,
    )

    etl_task

