from aws_cdk import (
    core as cdk,
    aws_athena as _athena,
    aws_glue as _glue,
    aws_events as _event,
)

class GlueStack(cdk.Stack):

    def __init__(self, scope: cdk.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # The code that defines your stack goes here

        _catalogid = cdk.CfnParameter(self, "CatalogId", type='String')  #Catalog ID : 720549054830

        _catalogname = cdk.CfnParameter(self, "DATA_CATALOG_NAME", type="String") # Catalog name : DataFoundationsGlueCatalog_Test

        _databasename = cdk.CfnParameter(self, "DATABASE_NAME", type="String")

        _schedule = cdk.CfnParameter(self, "schedule", type="String")        

        _s3bucketname = cdk.CfnParameter(self, "S3_BUCKET", type="String") #dip-stock-parameters-mns-productlife-s3-test
        
        _athena_end_point = cdk.CfnParameter(self, "ATHENA_ENDPOINT_URL", type="String")

        _country = cdk.CfnParameter(self, "COUNTRY", type="String")

        script_location = "s3://" + _s3bucketname.value_as_string + "/scriptlocation/"
        temporary_directory = "s3://" + _s3bucketname.value_as_string + "/tempdirectory/"
        python_lib = "s3://" + _s3bucketname.value_as_string + "/extrapyfiles/"
        jar_lib = "s3://" + _s3bucketname.value_as_string + "/extrajarfiles/"
        other_lib = "s3://" + _s3bucketname.value_as_string + "/extrafiles/"


        AthenaCatalog = _athena.CfnDataCatalog(self, "MyGlueID", 
            name= _catalogname.value_as_string, 
            type = "GLUE", 
            parameters= {
                "catalog-id": _catalogid.value_as_string,  
            },
        ),
    
        GlueTrigger_1 = _glue.CfnTrigger(self, "mygluetriggerid_1", 
            actions=[
                _glue.CfnTrigger.ActionProperty(
                    arguments= {
                        '--DATA_CATALOG_NAME': _catalogname.value_as_string,
                        '--DATABASE_NAME': _databasename.value_as_string,
                        '--SOURCE_NAME' : 'product'
                    },
                    job_name= "MYGlueJob",
                    )
                ],
            start_on_creation=True,
            type = "SCHEDULED", 
            name="GlueTrigger_1", 
            predicate=None,
            schedule=_schedule.value_as_string,     #"cron(0 8 * * ? *)",
            workflow_name=None
        ),

        GlueTrigger_2 = _glue.CfnTrigger(self, "mygluetriggerid_2", 
            actions=[
                _glue.CfnTrigger.ActionProperty(
                    arguments= {
                        '--DATA_CATALOG_NAME': _catalogname.value_as_string,
                        '--DATABASE_NAME': _databasename.value_as_string,
                        '--SOURCE_NAME' : 'product',
                        '--ATHENA_ENDPOINT_URL' : _athena_end_point.value_as_string,
                        '--COUNTRY' : _country.value_as_string
                    },
                    job_name= "MYGlueJob",
                    )
                ],
            start_on_creation=False,
            type = "ON_DEMAND", 
            name="GlueTrigger_2", 
            predicate=None,
            #schedule= "cron(0 8 * * ? *)",     #"cron(0 8 * * ? *)",
            workflow_name=None
        ),

        GlueETLJob = _glue.CfnJob(self, "mygluejobid", 
            command = _glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version= None,
                script_location= script_location,
            ), 
            role="AWSGlueServiceRole-GlueRole", 
            allocated_capacity=None, connections=None, 
            default_arguments={
                "--TempDir" : temporary_directory,
                "--extra-py-files" : python_lib,
                "--extra-jars" : jar_lib,
                "--extra-files" : other_lib,
                "--enable-metrics": "",
            },        
            execution_property= _glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs = 12,
                ),
            glue_version="2.0", 
            log_uri=None, max_capacity=None, 
            max_retries=None, 
            name="MYGlueJob", 
            notification_property=None, 
            number_of_workers=2, 
            security_configuration=None, 
            timeout=30, 
            worker_type= "Standard"
        ),

        JobfailEvent = _event.Rule(
            self,
            "MyEventID",
            event_pattern = _event.EventPattern(
                source=[
                    "aws.glue",   
                ],
                detail_type=[
                    "Glue Job State Change",
                ],
                detail ={
                    "jobName":[
                        "MYGlueJob",
                    ],
                    "state": [
                        "FAILED",
                    ],
                },
            ),
        )
        

