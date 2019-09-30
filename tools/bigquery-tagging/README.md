# BigQuery tagging

This will be an example setup using aggregated stackdriver log exports, Bigquery, Google AppEngine to build a query tagging (hashing) job for every query jobs in an Organization.

## Overview

Being able to group queries into buckets can be important for those managing available resources and track jobs across different timeframe. SLA jobs that run on a scheduled basis have strict elapsed time as their output is critical for business operation. It is very desirable to track SLA jobs over time to see if their trend is not deteriorating. It is also desirable to see how much resources are used by SLA jobs and non-SLA jobs to allocate appropriate resources. 

#### Diagram
                                                                +--------------------+ 
         [1]                [2]                 [3]             |  BigQuery  [4]     |
    +------------+    +-------------+  +-------------------+    |   +--------------+ |
    | BigQuery   |    |             +--> Aggregated Export +--------> Logs Dataset | |
    | Production +----> Stackdriver |  +-------------------+  ------+              | |
    | Query      |    |    Logs     |                         | |   +--------------+ |
    | Jobs       |    |             |                         | |                    | 
    +------------+    +-------------+                         | |       [6]          |  
                                                              | |  +--------------+  |     
                                                              | |  | Query Tag    |  |   
                                                              | |  | Table        |  |  
                                                              | |  +------^-------+  |    
                                                              | +-------- | ---------+     
                                                              | [5]       |      
                                                        +-----V-------+   |           
                                                        | Query tag   |   |       
                                                        | App Engine  +---|           
                                                        +-------------+            

#### Basic Idea

* BQ job insert/complete entries appear in the audit log
* Setup an org-wide aggregated audit log export to a BigQuery DataSet
* Setup Google App Engine job that monitors BigQuery Logs dataset for any completed jobs
* Google App engine job extracts jobs metadata (Query) and calculates a hash 
* Google App engine job stores the hash in a Bigquery table (streaming inserts)
* Schedule a cron Google App engine service at desired interval to trigger the GAE job

## Prerequisites


## Setup

#### Configure Org-wide Log Export for BigQuery Queries

* Please refer to this document for instructions on how to setup an org-wide log export:
    * [https://github.com/glickbot/professional-services/blob/org-wide-log-export/examples/org-wide-log-export/README.md](https://github.com/glickbot/professional-services/blob/org-wide-log-export/examples/org-wide-log-export/README.md)
    * Use the following filter (to remove dry-runs):

    resource.type="bigquery_resource" 
    protoPayload.serviceName="bigquery.googleapis.com" 
    ((protoPayload.methodName="jobservice.insert" AND NOT 
    (protoPayload.serviceData.jobInsertRequest.resource.jobConfiguration.dryRun=true OR 
    protoPayload.serviceData.jobInsertResponse.resource.jobConfiguration.dryRun=true)) OR 
    protoPayload.methodName="jobservice.jobcompleted")
    
* Set the destination to a Bigquery Dataset.

#### Deploy App

* Checkout, review code and uncomment/modify settings if desired.
* Make appropriate entries into ```config.ini``` file. The properties that needs update are

    * PROJECT_ID = ```project id of bigquery logs dataset```
    * DATASET_ID = ```bigquery logs dataset id```
    * TABLE_ID = ```query_tag```

* Use ```gcloud app deploy``` to deploy the app using GAE best practices.

#### Configure cron scheduler for GAE

* update cron.yaml file to setup appropriate schedule for triggering the GAE service.
* Deploy the cron service using ```gcloud app deploy cron.yaml```

## TODO

* Tests
* Benchmarking
