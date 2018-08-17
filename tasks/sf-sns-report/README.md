# @cumulus/sf-sns-report

[![Build Status](https://travis-ci.org/nasa/cumulus.svg?branch=master)](https://travis-ci.org/nasa/cumulus)

Broadcast an incoming Cumulus message to SNS.  This lambda function works with Cumulus Message Adapter, and it can be used anywhere in a step function workflow to report granule and PDR status.

If the task's input includes a `payload` key, the value of the key is returned as the output of the task, otherwise the output will be an empty object.

To report the PDR's progress as it's being processed, add the following step after the pdr-status-check:

    PdrStatusReport:
      CumulusConfig:
        cumulus_message:
          input: '{$}'
      ResultPath: null
      Type: Task
      Resource: ${SfSnsReportLambdaFunction.Arn}

To report the start status of the step function:

    StartAt: StatusReport
    States:
     StatusReport:
      CumulusConfig:
        cumulus_message:
          input: '{$}'
      ResultPath: null
      Type: Task
      Resource: ${SfSnsReportLambdaFunction.Arn}

To report the final status of the step function:

    StopStatus:
      CumulusConfig:
        sfnEnd: true
        stack: '{$.meta.stack}'
        bucket: '{$.meta.buckets.internal.name}'
        stateMachine: '{$.cumulus_meta.state_machine}'
        executionName: '{$.cumulus_meta.execution_name}'
        cumulus_message:
          input: '{$}'
      ResultPath: null
      Type: Task
      Resource: ${SfSnsReportLambdaFunction.Arn}

## What is Cumulus?

Cumulus is a cloud-based data ingest, archive, distribution and management prototype for NASA's future Earth science data streams.

[Cumulus Documentation](https://nasa.github.io/cumulus)

## Contributing

See [Cumulus README](https://github.com/nasa/cumulus/blob/master/README.md#installing-and-deploying)
