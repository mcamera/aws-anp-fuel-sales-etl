import boto3
import sys
from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from include.extract_vendas_combustiveis import extractVendasCombustiveis

aws_access_key_id = Variable.get("aws_access_key_id")
aws_secret_access_key = Variable.get("aws_secret_access_key")

client = boto3.client("emr", region_name="us-east-1",
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key)

s3client = boto3.client("s3", aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key)

default_args = {
    'owner': 'Marcelo Camera',
    'start_date': days_ago(1),
    'retries': 0,
    # 'retry_delay': timedelta(minutes=2),
    # 'retry_exponential_backoff': False,
    # 'max_retry_delay': timedelta(minutes=20),
    'depends_on_past': False,
    "email_on_failure": False,
    "email_on_retry": False
}

@dag(
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=['ANP', 'aws', 'bronze', 'silver', 'oil fuels sales', 'diesel sales', 'emr', 's3'],
    description='Pipeline ANP vendas combustíveis'
    )
def anp_sales_etl():
    """
    Data processing pipeline
    """

    @task
    def extract_sales_sheet():
        extract = extractVendasCombustiveis(s3client)
        extract.get_vendas_combustiveis()
        return True

    @task
    def transform_oil_fuels_sales():
        cluster_id = client.run_job_flow(
            Name='ETL01',
            ServiceRole='EMR_DefaultRole',
            JobFlowRole='EMR_EC2_DefaultRole',
            VisibleToAllUsers=True,
            LogUri='s3://anp-emr-files/logs',
            ReleaseLabel='emr-6.3.0',
            Instances={
                'InstanceGroups': [
                    {
                        'Name': 'Master nodes',
                        'Market': 'SPOT',
                        'InstanceRole': 'MASTER',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 1,
                    },
                    {
                        'Name': 'Worker nodes',
                        'Market': 'SPOT',
                        'InstanceRole': 'CORE',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 1,
                    }
                ],
                'Ec2KeyName': 'ec2_key_pair',
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2SubnetId': 'subnet-02ba721d1f4017398'
            },

            Applications=[{'Name': 'Spark'}],

            Configurations=[{
                "Classification": "spark-env",
                "Properties": {},
                "Configurations": [{
                    "Classification": "export",
                    "Properties": {
                        "PYSPARK_PYTHON": "/usr/bin/python3",
                        "PYSPARK_DRIVER_PYTHON": "/usr/bin/python3"
                    }
                }]
            },
                {
                    "Classification": "spark-hive-site",
                    "Properties": {
                        "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                    }
                },
                {
                    "Classification": "spark-defaults",
                    "Properties": {
                        "spark.submit.deployMode": "cluster",
                        "spark.speculation": "false",
                        "spark.sql.adaptive.enabled": "true",
                        "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
                    }
                },
                {
                    "Classification": "spark",
                    "Properties": {
                        "maximizeResourceAllocation": "true"
                    }
                }
            ],

            Steps=[{
                'Name': 'Step 1',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['spark-submit',
                            '--packages', 'io.delta:delta-core_2.12:1.0.0', 
                            '--conf', 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension', 
                            '--conf', 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog', 
                            '--master', 'yarn',
                            '--deploy-mode', 'cluster',
                            's3://anp-etl-files/etl/02_transform_silver_oil_fuels_sales.py'
                        ]
                }
            }],
        )
        return cluster_id["JobFlowId"]
    
    @task
    def wait_transform_oil_fuels_sales(cid: str):
        waiter = client.get_waiter('step_complete')
        steps = client.list_steps(
            ClusterId=cid
        )
        stepId = steps['Steps'][0]['Id']

        waiter.wait(
            ClusterId=cid,
            StepId=stepId,
            WaiterConfig={
                'Delay': 30,
                'MaxAttempts': 120
            }
        )
        return cid, True

    @task
    def transform_diesel_sales(cid: str, success_before: bool):
        if success_before:
            newstep = client.add_job_flow_steps(
                JobFlowId=cid,
                Steps=[{
                    'Name': 'ETL02',
                    'ActionOnFailure': "TERMINATE_CLUSTER",
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['spark-submit',
                                '--packages', 'io.delta:delta-core_2.12:1.0.0', 
                                '--conf', 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension', 
                                '--conf', 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog', 
                                '--master', 'yarn',
                                '--deploy-mode', 'cluster',
                                's3://anp-etl-files/etl/03_transform_silver_diesel_sales.py'
                            ]
                    }
                }]
            )
            return cid, newstep['StepIds'][0]

    @task
    def wait_transform_diesel_sales(cid: str, stepId: str):
        waiter = client.get_waiter('step_complete')
        waiter.wait(
            ClusterId=cid,
            StepId=stepId,
            WaiterConfig={
                'Delay': 30,
                'MaxAttempts': 120
            }
        )
        return cid, True

    @task
    def terminate_emr_cluster(cid: str, success_before: str):
        if success_before:
            res = client.terminate_job_flows(
                JobFlowIds=[cid]
            )

    # Pipeline chain
    # res_extract = extract_sales_sheet()
    # cid = transform_oil_fuels_sales()
    # res_emr = wait_transform_oil_fuels_sales()
    # newstep = transform_diesel_sales(cid=cluid, success_before=res_emr)
    # res_ba = wait_transform_diesel_sales(cid=cluid, stepId=newstep)
    # res_ter = terminate_emr_cluster(success_before=res_ba, cid=cluid)

    terminate_emr_cluster(wait_transform_diesel_sales(transform_diesel_sales(wait_transform_oil_fuels_sales(transform_oil_fuels_sales(extract_sales_sheet())))))

dag = anp_sales_etl()
