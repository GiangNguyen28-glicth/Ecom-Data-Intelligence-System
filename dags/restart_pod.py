from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from kubernetes import client
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook



def restart_deployment():
    hook = KubernetesHook(
        conn_id="kubernetes_default"
    )
    api_client = hook.get_conn()
    apps_v1 = client.AppsV1Api(api_client)
    deployment_name = "ynm-proxy-manager-service-testing"
    namespace = "crawler-testing"
    body = {
        "spec": {
            "template": {
                "metadata": {
                    "annotations": {
                        "kubectl.kubernetes.io/restartedAt": datetime.utcnow().isoformat()
                    }
                }
            }
        }
    }

    apps_v1.patch_namespaced_deployment(
        name=deployment_name,
        namespace=namespace,
        body=body,
    )


with DAG(
    dag_id="restart_k8s_deployment",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    restart_task = PythonOperator(
        task_id="restart_deployment",
        python_callable=restart_deployment,
    )

    sensor_task = PythonOperator()