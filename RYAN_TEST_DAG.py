import json
from datetime import datetime
from airflow.models.dag import DAG
from airflow.models import Variable

from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

dag = DAG(dag_id='RYAN_TEST_DAG',
          default_args={
                'owner':"airflow",
                'start_date': datetime(2023, 4, 1),
                'email': ["ryan_walsh@colpal.com"],
                'email_on_failure': True

          }     schedule_interval=None)

env = Variable.get("landscape-abbv")
PROJECT_ID = Variable.get("gcp-project")
NAMESPACE = Variable.get("namespace")
SERVICE_ACCOUNT_NAME = 'vault-sidecar'

CONTAINER_NAME = "csv_to_parquet_test"

AIRFLOW_REG = "us-east4-docker.pkg/cp-artifact-registry/airflow-containers"
CONTAINER_IMG = f"{AIRFLOW_REG}/{CONTAINER_NAME}:dev"

DAG_ID = "{{dag.dag_id}}"
RUN_ID = "{{run_id}}"

PIPELINE_BUCKET_NAME = f"cp-advancedtech-sandbox-pipeline"
INPUT_PIPELINE_BUCKET_PATH = "ryan-git/input"
INPUT_FILENAME = "RYAN_TEST.csv"

OUTPUT_PIPELINE_BUCKET_PATH = f"{DAG_ID}/{CONTAINER_NAME}/{RUN_ID}"
OUTPUT_FILENAME = "container_test_csv_to_parquet_output.parquet"

OUTPUT_TABLE = f"{PROJECT_ID}.sandbox_output.{DAG_ID}_{CONTAINER_NAME}_{RUN_ID}"
INGESTION_TIME = str(datetime.now())

DATA_JSON = json.dumps(
    {
    
    }
)

SECRET_LOCATION = "/vault/secrets/{}"                                                  ################    NEED SECRETS??????????????????   #######################


annotations = {
    "vault.hashicorp.com/agent-inject": "true",                                        # ENABLES VAULT INJECTION <must be set to TRUE>
    "vault.hashicorp.com/agent-pre-populate-only": "true",                             # CONFIG WHETEHR INIT CONTAINER IS ONLY INJECTED CONTAINER(TRUE) OR NOT(FALSE)
    "vault.hashicarp.com/role": NAMESPACE,                                             # NAME OF KUBERNETES ROLE

    "vault.hashicorp.com/agent-inject-secret-gcp-sa-storage-json": "secret/teams/{}" + PROJECT_ID + "/gcp-sa-storage",                          ####  GCS  ##########
    "vault.hashicorp.com/agent-inject-template-gcp-sa-storage.json": """{{with secret "secret/teams/""" + PROJECT_ID + """/gcp-sa-storage }}
    {{ .Data.data.key | base64code}}
{{ end }}""",                                                                                                                                   ####  GCS  ##########

    "vault.hashicorp.com/agent-inject-secret-gcp-sa-bq-json": "secret/teams/{}" + PROJECT_ID + "/gcp-sa-bq",                                    ####  BIG QUERY  ####
    "vault.hashicorp.com/agent-inject-template-gcp-sa-bq.json": """{{with secret "secret/teams/""" + PROJECT_ID + """/gcp-sa-bq }}              
    {{ .Data.data.key | base64code}}
{{ end }}""",                                                                                                                                   ####  BIG QUERY  ####

    "vault.hashicorp.com/tls-skip-verify": "true",

}

start = DummyOperator(task_id='start',dag=dag)

container_task = KubernetesPodOperator(
    task_id ="container_task",
    name="container_task",
namespace=NAMESPACE,                                                              ####  NAME OF KUBERNETES ROLE....WILL ALWAYS MATCH NAMESPACE  #####          
    image=CONTAINER_IMG,                                                          #### REFERS TO LINE 26....f STRING of AIRFLOW REG and CONTAINER NAME :dev  ####
    image_pull_policy="Always",
    arguments=[
    "-------------------input_bucket_name",
    PIPELINE_BUCKET_NAME,                                                        #### f"cp-advancedtech-sandbox-pipeline"
    "-------------------input_path",
    INPUT_PIPELINE_BUCKET_PATH,                                                  #### "ryan-git/input"
    "-------------------input_filename",
    INPUT_FILENAME,                                                              #### "RYAN_TEST.csv" 
    "-------------------output_bucket_name",
    PIPELINE_BUCKET_NAME,                                                        #### f"cp-advancedtech-sandbox-pipeline"
    "-------------------output_path",
    OUTPUT_PIPELINE_BUCKET_PATH,                                                 #### f"{DAG_ID}/{CONTAINER_NAME}/{RUN_ID}"      
    "-------------------output_filename",
    OUTPUT_FILENAME                                                              #### "container_test_csv_to_parquet_output.parquet"
    ],

    annotations=annotations,
    get_logs=True,
    dag=dag,
    is_delete_operator_pod=False,
    service_account_name="vault-sidecar",
)

end = DummyOperator(task_id='end', dag=dag)

start >> container_task >> end












}
