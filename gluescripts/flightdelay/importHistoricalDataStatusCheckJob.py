import sys
import boto3
import time

session = boto3.Session()
forecast = session.client(service_name='forecast') 
glue_client = session.client(service_name='glue')

workflowName = 'FlightDelayPredictionWorkflow'
workflow = glue_client.get_workflow(Name=workflowName)
workflow_params = workflow['Workflow']['LastRun']['WorkflowRunProperties']
workflowRunId = workflow['Workflow']['LastRun']['WorkflowRunId']
delaydata_import_ds_arn = workflow_params['targetTimeSeriesDataset']
# initialise import job status for while loop
delayDataImportStatus = forecast.describe_dataset(DatasetArn=delaydata_import_ds_arn)['Status']

while True:    
    if (delayDataImportStatus == 'ACTIVE'):
        break
    elif (delayDataImportStatus == 'CREATE_FAILED'):
        raise NameError('Import create failed')
    delayDataImportStatus = forecast.describe_dataset(DatasetArn=delaydata_import_ds_arn)['Status']
    time.sleep(10)

print ('Flight Delay Data Import status is: ' + delayDataImportStatus)