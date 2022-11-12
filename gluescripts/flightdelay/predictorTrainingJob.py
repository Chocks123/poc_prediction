import sys
import boto3

session = boto3.Session()
forecast = session.client(service_name='forecast') 
glue_client = session.client(service_name='glue')

workflowName = 'FlightDelayPredictionWorkflow'
workflow = glue_client.get_workflow(Name=workflowName)
workflow_params = workflow['Workflow']['LastRun']['WorkflowRunProperties']
workflowRunId = workflow['Workflow']['LastRun']['WorkflowRunId']
datasetGroupArn = workflow_params['datasetGroupArn']
project = workflow_params['projectName']
predictorName= project + '_aml'


print('datasetGroupArn imported is: ' + datasetGroupArn)

forecastHorizon = 10
print('forecastHorizon used is: 10')
print('forecastFrequecy used is: D')

create_predictor_response=forecast.create_predictor(PredictorName=predictorName,
                                                ForecastHorizon=forecastHorizon,
                                                PerformAutoML= True,
                                                EvaluationParameters= {"NumberOfBacktestWindows": 1,
                                                                        "BackTestWindowOffset": 10}, 
                                                InputDataConfig= {"DatasetGroupArn": datasetGroupArn},
                                                FeaturizationConfig= {"ForecastFrequency": "D"
                                                })
predictorArn=create_predictor_response['PredictorArn']

workflow_params['predictorArn'] = predictorArn
glue_client.put_workflow_run_properties(Name=workflowName, RunId=workflowRunId, RunProperties=workflow_params)
workflow_params = glue_client.get_workflow_run_properties(Name=workflowName,
                                        RunId=workflowRunId)["RunProperties"]

print('output Predictor Arn is: ' + workflow_params['predictorArn'])