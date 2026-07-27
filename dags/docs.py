from airflow import DAG
from airflow.operators.python import PythonOperator, BashOperator


def render_jinja_template(**kwargs):
    print(kwargs["params"]["show_context"])
    print(kwargs["params"]["hihi"])


def render_jinja_template2(show_context, hihi):
    print(show_context)
    print(hihi)


with DAG(
        dag_id="docs",
        catchup=False,
        schedule=None,

) as dag:
    show_context_1 = PythonOperator(
        task_id="show_context_1",
        python_callable=render_jinja_template,
        params={
            "show_context": "GFian",
            "hihi": True
        },
    )
    show_context_2 = PythonOperator(
        task_id="show_context_2",
        python_callable=render_jinja_template,
        priority_weight=100,
        params={
            "show_context": "GFian",
            "hihi": True
        }
    )

    task_a = BashOperator(
        task_id="train_model",
        queue="gpu",
    )

    show_context_2 >> task_a
