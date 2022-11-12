import sys
import boto3
import datetime
import random
import os
import time

session = boto3.Session()
forecast = session.client(service_name='forecast') 
glue_client = session.client(service_name='glue')

workflowName = 'FlightDelayPredictionWorkflow'
workflow = glue_client.get_workflow(Name=workflowName)
workflow_params = workflow['Workflow']['LastRun']['WorkflowRunProperties']
workflowRunId = workflow['Workflow']['LastRun']['WorkflowRunId']
iam = session.resource('iam')

# In our dataset, the timeseries values are recorded every day
DATASET_FREQUENCY = "D" 
TIMESTAMP_FORMAT = "yyyy-MM-dd"

rnd = str(random.getrandbits(12))
project = 'flight_delay_prediction_' + rnd
datasetName = project + '_ds'
datasetGroupName = project + '_dsg'
print('datasetName is: ' + datasetName)
print('datasetGroupName is: ' + datasetGroupName)
role = iam.Role('MLOpsUserRole')
bucket_name = workflow_params['inBucket']
airline_param = workflow_params['airline']
route_param = workflow_params['route']
data_file = airline_param+'/'+route_param+'/'+'flightdelaydata.csv'

s3DataPath = 's3://' + bucket_name + '/' + data_file
targetvalueToPredict = workflow_params['targetvalue']
print('role_arn is: ' + role.arn)
print('s3DataPath is: ' + s3DataPath)

print('targetvalueToPredict is: ' + targetvalueToPredict)

create_dataset_group_response = forecast.create_dataset_group(DatasetGroupName=datasetGroupName,
                                                            Domain="CUSTOM",
                                                            )
datasetGroupArn = create_dataset_group_response['DatasetGroupArn']
workflow_params['datasetGroupArn'] = datasetGroupArn
workflow_params['projectName'] = project

glue_client.put_workflow_run_properties(Name=workflowName, RunId=workflowRunId, RunProperties=workflow_params)
workflow_params = glue_client.get_workflow_run_properties(Name=workflowName,
                                        RunId=workflowRunId)["RunProperties"]

def start_data_import_job(s3DataPath, datasetName, datasetGroupArn, role_arn):
    # Specify the schema of your dataset here. Make sure the order of columns matches the raw data files.
    schema = {
    "Attributes": [
        {
            "AttributeName": "item_id",
            "AttributeType": "string"
        },
        {
            "AttributeName": "timestamp",
            "AttributeType": "timestamp"
        },
        {
            "AttributeName": "target_value",
            "AttributeType": "float"
        }
    ]
    }

    response = forecast.create_dataset(
                    Domain="CUSTOM",
                    DatasetType='TARGET_TIME_SERIES',
                    DatasetName=datasetName,
                    DataFrequency=DATASET_FREQUENCY, 
                    Schema = schema)

    TargetdatasetArn = response['DatasetArn']
    workflow_params['targetTimeSeriesDataset'] = TargetdatasetArn
    updateDatasetResponse = forecast.update_dataset_group(DatasetGroupArn=datasetGroupArn, DatasetArns=[TargetdatasetArn])

    # flight delay dataset import job
    datasetImportJobName = 'DELAY_DATA_DSIMPORT_JOB_TARGET'
    ds_import_job_response=forecast.create_dataset_import_job(DatasetImportJobName=datasetImportJobName,
                                                            DatasetArn=TargetdatasetArn,
                                                            DataSource= {
                                                                "S3Config" : {
                                                                    "Path": s3DataPath,
                                                                    "RoleArn": role_arn
                                                                } 
                                                            },
                                                            TimestampFormat=TIMESTAMP_FORMAT
                                                            )

    ds_import_job_arn=ds_import_job_response['DatasetImportJobArn']

    workflow_params['delaydataImportJobRunId'] = ds_import_job_arn
    
    return {
    "importJobArn": ds_import_job_arn,
    "datasetGroupArn": datasetGroupArn,
    "delaydataDatasetArn": TargetdatasetArn
    }

stock_import_result = start_data_import_job(s3DataPath, datasetName, datasetGroupArn, role.arn)

glue_client.put_workflow_run_properties(Name=workflowName, RunId=workflowRunId, RunProperties=workflow_params)
workflow_params = glue_client.get_workflow_run_properties(Name=workflowName,
                                        RunId=workflowRunId)["RunProperties"]

print('output Dataset Group Arn is: ' + workflow_params['datasetGroupArn'])