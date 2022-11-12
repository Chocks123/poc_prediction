import sys
import boto3

session = boto3.Session()
forecast = session.client(service_name='forecast') 
glue_client = session.client(service_name='glue')


workflowName = 'FlightDelayPredictionWorkflow'
workflow = glue_client.get_workflow(Name=workflowName)
workflow_params = workflow['Workflow']['LastRun']['WorkflowRunProperties']
workflowRunId = workflow['Workflow']['LastRun']['WorkflowRunId']
project = workflow_params['projectName']
forecastName= project + '_aml_forecast'
predictorArn = workflow_params['predictorArn']
print('forecastName is: ' + forecastName)    
create_forecast_response=forecast.create_forecast(ForecastName=forecastName,
                                                  PredictorArn=predictorArn)
forecastArn = create_forecast_response['ForecastArn']

workflow_params['forecastArn'] = forecastArn
workflow_params['forecastName'] = forecastName
glue_client.put_workflow_run_properties(Name=workflowName, RunId=workflowRunId, RunProperties=workflow_params)
workflow_params = glue_client.get_workflow_run_properties(Name=workflowName,
                                        RunId=workflowRunId)["RunProperties"]

print('Forecast Arn is: ' + workflow_params['forecastArn'])