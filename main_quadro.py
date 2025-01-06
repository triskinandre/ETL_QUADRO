"""
Script principal para o pipeline de quadro.
"""

from datetime import datetime, timedelta

def main_quadro_postgree():
    """
    Função principal para o processo standalone.
    """
    from etl.orchestrador.orch_quadro import main_quadro
    main_quadro()


def create_dag():
    """
    Cria e retorna o DAG para o Airflow.
    """
    try:
        from airflow import DAG
        from airflow.operators.bash import BashOperator
        from airflow.operators.python_operator import PythonOperator
    except ImportError:
        print("Airflow não está disponível. Ignorando criação do DAG.")
        return None

    # Configuração do DAG
    airflow_start_date = datetime.now() - timedelta(days=1)

    default_args = {
        'retries': 0,
        'retry_delay': timedelta(minutes=1440),
        'start_date': airflow_start_date,
    }

    dag = DAG(
        dag_id='quadro_postgree',
        default_args=default_args,
        schedule_interval='0 9 * * *',
        catchup=False,
    )

    # Tarefa Bash
    exec_quadro_bash_task = BashOperator(
        task_id='exec_quadro_postgree_bash',
        bash_command=(
            'cd /home/airflow/airflow/dags/05-TRISKIN-TEAM-TBOARD-ETL-QUADRO-RHSENIOR && '
            'source /home/airflow/airflow/venvs/venv_quadro_postgree/bin/activate && '
            'python -c "from etl.orchestrador.orch_quadro import main_quadro; main_quadro_postgree()"'
        ),
        dag=dag,
    )
    return dag


# Carrega o DAG no Airflow
dag = create_dag()
if dag is not None:
    globals()[dag.dag_id] = dag

# Execução standalone
if __name__ == "__main__":
    main_quadro_postgree()
