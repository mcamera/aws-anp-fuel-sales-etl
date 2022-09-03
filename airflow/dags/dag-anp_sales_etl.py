""" ANP SALES ETL DAIRFLOW DAG
This DAG uses the Airflow Task Flow API.
"""

import boto3
import sys
sys.path.append('/opt/airflow')
from include.extract_vendas_combustiveis import extractVendasCombustiveis

from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Marcelo Camera',
    'start_date': days_ago(1),
    'retries': 0,
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
    """Data processing pipeline
    Creates an EMR cluster client.
    """
    aws_access_key_id = Variable.get("aws_access_key_id")
    aws_secret_access_key = Variable.get("aws_secret_access_key")

    client = boto3.client("emr", region_name="us-east-1",
                        aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key)

    s3client = boto3.client("s3", aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key)

    @task
    def extract_sales_sheets():
        """This task will extract the sheets from the data source and fixes the shift data problem in each one.
        Then, the sheets sales_oil_fuels and sales_diesel will be uploaded to the bronze S3 bucket.

        Returns:
            success(dict[bool]): True/false.
        """
        extract = extractVendasCombustiveis(s3client)
        extract.get_vendas_combustiveis()

        return True

    @task
    def ingest_silver_sales_oil_fuels(success_before: bool):
        """Executes a spark job from a S3 file.

        Args:
            success_before (bool): True/False from the previous task.

        Returns:
            cluster_id(dict[str]): EMR cluster id.
        """
        if success_before:
            cluster_id = client.run_job_flow(
                Name='ANP-ETL',
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
                    'Ec2SubnetId': 'subnet-0baf7ac9f346c734a'
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
                    'Name': 'Ingest Silver - Sales oil fuels',
                    'ActionOnFailure': 'TERMINATE_CLUSTER',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['spark-submit',
                                '--packages', 'io.delta:delta-core_2.12:1.0.0', 
                                '--conf', 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension', 
                                '--conf', 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog', 
                                '--master', 'yarn',
                                '--deploy-mode', 'cluster',
                                's3://anp-emr-files/etl/01_transform_silver_oil_fuels_sales.py'
                            ]
                    }
                }],
            )
            return cluster_id["JobFlowId"]

    @task
    def wait_silver_sales_oil_fuels(cid: str):
        """Task monitor for the Spark job in the AWS EMR.

        Args:
            cid (str): Cluster id from the previous task.

        Returns:
            cluster_id(dict[str]): EMR cluster id.
            success(dict[bool]): True/false.
        """
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
        return True

    @task
    def ingest_silver_sales_diesel(cid: str, success_before: bool):
        """Executes a spark job from a S3 file.

        Args:
            cid (str): Cluster id from the previous task.
            success_before (bool): True/False from the previous task.

        Returns:
            cluster_id(dict[str]): EMR cluster id.
            step_id(dict[str]): Step id
        """
        if success_before:
            newstep = client.add_job_flow_steps(
                JobFlowId=cid,
                Steps=[{
                    'Name': 'Ingest Silver - Sales diesel',
                    'ActionOnFailure': "TERMINATE_CLUSTER",
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['spark-submit',
                                '--packages', 'io.delta:delta-core_2.12:1.0.0', 
                                '--conf', 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension', 
                                '--conf', 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog', 
                                '--master', 'yarn',
                                '--deploy-mode', 'cluster',
                                's3://anp-emr-files/etl/02_transform_silver_diesel_sales.py'
                            ]
                    }
                }]
            )
            return newstep['StepIds'][0]

    @task
    def wait_silver_sales_diesel(cid: str, step_id: str):
        """Task monitor for the Spark job in the AWS EMR.

        Args:
            cid (str): Cluster id from the previous task.
            step_id (str): Step id from the previous task

        Returns:
            cluster_id(dict[str]): EMR cluster id.
            success(dict[bool]): True/false.
        """

        waiter = client.get_waiter('step_complete')

        waiter.wait(
            ClusterId=cid,
            StepId=step_id,
            WaiterConfig={
                'Delay': 30,
                'MaxAttempts': 120
            }
        )
        return True

    @task
    def ingest_gold_sales_oil_fuels(cid: str, success_before: bool):
        """Executes a spark job from a S3 file.

        Args:
            cid (str): Cluster id from the previous task.
            success_before (bool): True/False from the previous task.

        Returns:
            cluster_id(dict[str]): EMR cluster id.
            step_id(dict[str]): Step id
        """
        if success_before:
            newstep = client.add_job_flow_steps(
                JobFlowId=cid,
                Steps=[{
                    'Name': 'Ingest Gold - Sales oil fuels',
                    'ActionOnFailure': "TERMINATE_CLUSTER",
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['spark-submit',
                                '--packages', 'io.delta:delta-core_2.12:1.0.0', 
                                '--conf', 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension', 
                                '--conf', 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog', 
                                '--master', 'yarn',
                                '--deploy-mode', 'cluster',
                                's3://anp-emr-files/etl/03_transform_gold_oil_fuels_sales.py'
                            ]
                    }
                }]
            )
            return newstep['StepIds'][0]

    @task
    def wait_gold_sales_oil_fuels(cid: str, step_id: str):
        """Task monitor for the Spark job in the AWS EMR.

        Args:
            cid (str): Cluster id from the previous task.
            step_id (str): Step id from the previous task

        Returns:
            cluster_id(dict[str]): EMR cluster id.
            success(dict[bool]): True/false.
        """

        waiter = client.get_waiter('step_complete')
        waiter.wait(
            ClusterId=cid,
            StepId=step_id,
            WaiterConfig={
                'Delay': 30,
                'MaxAttempts': 120
            }
        )
        return True

    @task
    def ingest_gold_sales_diesel(cid: str, success_before: bool):
        """Executes a spark job from a S3 file.

        Args:
            cid (str): Cluster id from the previous task.
            success_before (bool): True/False from the previous task.

        Returns:
            cluster_id(dict[str]): EMR cluster id.
            step_id(dict[str]): Step id
        """
        if success_before:
            newstep = client.add_job_flow_steps(
                JobFlowId=cid,
                Steps=[{
                    'Name': 'Ingest Gold - Sales diesel',
                    'ActionOnFailure': "TERMINATE_CLUSTER",
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['spark-submit',
                                '--packages', 'io.delta:delta-core_2.12:1.0.0', 
                                '--conf', 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension', 
                                '--conf', 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog', 
                                '--master', 'yarn',
                                '--deploy-mode', 'cluster',
                                's3://anp-emr-files/etl/04_transform_gold_diesel_sales.py'
                            ]
                    }
                }]
            )
            return newstep['StepIds'][0]

    @task
    def wait_gold_sales_diesel(cid: str, step_id: str):
        """Task monitor for the Spark job in the AWS EMR.

        Args:
            cid (str): Cluster id from the previous task.
            step_id (str): Step id from the previous task

        Returns:
            cluster_id(dict[str]): EMR cluster id.
            success(dict[bool]): True/false.
        """

        waiter = client.get_waiter('step_complete')
        waiter.wait(
            ClusterId=cid,
            StepId=step_id,
            WaiterConfig={
                'Delay': 30,
                'MaxAttempts': 120
            }
        )
        return True

    @task
    def terminate_emr_cluster(cid: str, success_before: bool):
        """Kill the AWS EMR cluster

        Args:
            cid (str): Cluster id from the previous task.
            success_before (bool): True/False from the previous task.
        """
        if success_before:
            res = client.terminate_job_flows(
                JobFlowIds=[cid]
            )

    data_extracted = extract_sales_sheets()
    cluid = ingest_silver_sales_oil_fuels(data_extracted)
    silver_sales_oil_fuels_processed = wait_silver_sales_oil_fuels(cluid)

    ingest_silver_sales_diesel_stepid = ingest_silver_sales_diesel(cluid, silver_sales_oil_fuels_processed)
    silver_sales_diesel_processed = wait_silver_sales_diesel(cluid, ingest_silver_sales_diesel_stepid)

    ingest_gold_sales_oil_fuels_stepid = ingest_gold_sales_oil_fuels(cluid, silver_sales_diesel_processed)
    gold_sales_oil_fuels_processed = wait_gold_sales_oil_fuels(cluid, ingest_gold_sales_oil_fuels_stepid)

    ingest_gold_sales_diesel_stepid = ingest_gold_sales_diesel(cluid, gold_sales_oil_fuels_processed)
    gold_sales_diesel_processed = wait_gold_sales_diesel(cluid, ingest_gold_sales_diesel_stepid)

    terminate_emr_cluster(cluid, gold_sales_diesel_processed)

dag = anp_sales_etl()
