const { Execution } = require('@cumulus/api/models');
const {
  buildAndExecuteWorkflow,
  waitForCompletedExecution,
  LambdaStep,
  api: apiTestUtils
} = require('@cumulus/integration-tests');

const {
  loadConfig,
  uploadTestDataToBucket,
  deleteFolder,
  timestampedTestDataPrefix
} = require('../helpers/testUtils');

const config = loadConfig();
const lambdaStep = new LambdaStep();

const taskName = 'DiscoverAndQueuePdrs';

const pdrFilename = 'MOD09GQ_1granule_v3.PDR';

const s3data = [
  '@cumulus/test-data/pdrs/MOD09GQ_1granule_v3.PDR'
];

describe('The Discover And Queue PDRs workflow', () => {
  const testDataFolder = timestampedTestDataPrefix(`${config.stackName}-DiscoverAndQueuePdrsSuccess`);
  const collection = { name: 'MOD09GQ', version: '006' };
  const provider = { id: 's3_provider' };
  let workflowExecution;
  let queuePdrsOutput;
  process.env.ExecutionsTable = `${config.stackName}-ExecutionsTable`;
  const executionModel = new Execution();

  beforeAll(async () => {
    // populate test data
    await uploadTestDataToBucket(config.bucket, s3data, testDataFolder);

    await deleteFolder(config.bucket, `${config.stackName}/pdrs`);
    workflowExecution = await buildAndExecuteWorkflow(
      config.stackName,
      config.bucket,
      taskName,
      collection,
      provider
    );

    queuePdrsOutput = await lambdaStep.getStepOutput(
      workflowExecution.executionArn,
      'QueuePdrs'
    );
  });

  afterAll(async () => {
    // clean up test data
    await deleteFolder(config.bucket, testDataFolder);
  });

  it('executes successfully', () => {
    expect(workflowExecution.status).toEqual('SUCCEEDED');
  });

  describe('the DiscoverPdrs Lambda', () => {
    let lambdaOutput = null;

    beforeAll(async () => {
      lambdaOutput = await lambdaStep.getStepOutput(workflowExecution.executionArn, 'DiscoverPdrs');
    });

    it('has expected path and name output', () => {
      expect(lambdaOutput.payload.pdrs[0].path).toEqual(testDataFolder);
      expect(lambdaOutput.payload.pdrs[0].name).toEqual(pdrFilename);
    });
  });

  describe('the QueuePdrs Lambda', () => {
    it('has expected output', () => {
      expect(queuePdrsOutput.payload.pdrs_queued).toEqual(1);
      expect(queuePdrsOutput.payload.running.length).toEqual(1);
    });
  });

  /**
   * The DiscoverAndQueuePdrs workflow kicks off a ParsePdr workflow, so check that the
   * ParsePdr workflow completes successfully. Above, we checked that there is
   * one running task, which is the ParsePdr workflow. The payload has the arn of the
   * running workflow, so use that to get the status.
   */
  describe('ParsePdr workflow', () => {
    let parsePdrWorkflowArn;
    let parsePdrExecutionStatus;

    beforeAll(async () => {
      parsePdrWorkflowArn = queuePdrsOutput.payload.running[0];
      parsePdrExecutionStatus = await waitForCompletedExecution(parsePdrWorkflowArn);
    });

    afterAll(async () => {
      // wait for child executions to complete
      const queueGranulesOutput = await lambdaStep.getStepOutput(
        parsePdrWorkflowArn,
        'QueueGranules'
      );
      await Promise.all(queueGranulesOutput.payload.running.map(async (arn) => {
        await waitForCompletedExecution(arn);
      }));
    });

    it('executes successfully', () => {
      expect(parsePdrExecutionStatus).toEqual('SUCCEEDED');
    });

    describe('ParsePdr lambda function', () => {
      it('successfully parses a granule from the PDR', async () => {
        const lambdaOutput = await lambdaStep.getStepOutput(
          parsePdrWorkflowArn,
          'ParsePdr'
        );
        expect(lambdaOutput.payload.granules.length).toEqual(1);
        expect(lambdaOutput.payload.pdr).toEqual(lambdaOutput.payload.pdr);
      });
    });
  });

  /** This test relies on the previous 'ParsePdr workflow' to complete */
  describe('When accessing an execution via the API that was triggered from a parent step function', () => {
    it('displays a link to the parent', async () => {
      const parsePdrWorkflowArn = queuePdrsOutput.payload.running[0];
      const parsePdrExecution = await apiTestUtils.getExecution({
        prefix: config.stackName,
        arn: parsePdrWorkflowArn
      });

      expect(parsePdrExecution.parentArn).toEqual(workflowExecution.executionArn);
    });
  });

  describe('When accessing an execution via the API that was not triggered from a parent step function', () => {
    it('does not display a parent link', async () => {
      const queuePdrsExecution = await apiTestUtils.getExecution({
        prefix: config.stackName,
        arn: workflowExecution.executionArn
      });

      expect(queuePdrsExecution.parentArn).toBeUndefined();
    });
  });


  describe('the sf-sns-report task has published a sns message and', () => {
    it('the execution record is added to DynamoDB', async () => {
      const record = await executionModel.get({ arn: workflowExecution.executionArn });
      expect(record.status).toEqual('completed');
    });
  });
});
