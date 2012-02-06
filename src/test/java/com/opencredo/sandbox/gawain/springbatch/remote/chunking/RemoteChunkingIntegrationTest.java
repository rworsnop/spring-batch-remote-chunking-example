package com.opencredo.sandbox.gawain.springbatch.remote.chunking;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Test;

import org.springframework.batch.core.BatchStatus;



public class RemoteChunkingIntegrationTest {

	private static final Log logger = LogFactory.getLog(RemoteChunkingIntegrationTest.class);

	@Test
	public void testRemoteChunkingOnSingleSlaveShouldComplete() throws Exception {

		final BrokerContext broker = new BrokerContext("classpath:/broker/broker-context.xml");
		final MasterBatchContext masterBatchContext = new MasterBatchContext("testjob", "classpath:/master/master-batch-context.xml");
		final SlaveContext slaveContext = new SlaveContext("classpath:/slave/slave-batch-context.xml");

		broker.start();
		masterBatchContext.start();
		slaveContext.start();
		
		waitForJobTopComplete(masterBatchContext);

		final BatchStatus batchStatus = masterBatchContext.getBatchStatus();
		logger.debug("job finished with status: " + batchStatus);
		logger.info("slave chunks written: " + slaveContext.writtenCount() );
		Assert.assertEquals("Batch Job Status", BatchStatus.COMPLETED, batchStatus);
		Assert.assertEquals("Slave written count",  10, slaveContext.writtenCount());
	}
	
	@Test
	public void testRemoteChunkingOnMultipleSlavesShouldLoadBalanceAndComplete() throws Exception {

		final BrokerContext broker = new BrokerContext("classpath:/broker/broker-context.xml");
		final MasterBatchContext masterBatchContext = new MasterBatchContext("testjob", "classpath:/master/master-batch-context.xml");
		final SlaveContext slaveContext1 = new SlaveContext("classpath:/slave/slave1-batch-context.xml");
		final SlaveContext slaveContext2 = new SlaveContext("classpath:/slave/slave2-batch-context.xml");

		broker.start();
		masterBatchContext.start();
		slaveContext1.start();
		slaveContext2.start();
		
		waitForJobTopComplete(masterBatchContext);

		final BatchStatus batchStatus = masterBatchContext.getBatchStatus();
		logger.info("job finished with status: " + batchStatus);
		Assert.assertEquals("Batch Job Status", BatchStatus.COMPLETED, batchStatus);
		logger.info("slave 1 chunks written: " + slaveContext1.writtenCount() );
		logger.info("slave 2 chunks written: " + slaveContext2.writtenCount() );
		Assert.assertEquals("slave chunks written", 5, slaveContext1.writtenCount() ); 
		Assert.assertEquals("slave chunks written", 5, slaveContext2.writtenCount()); 

	}	
	
	
	/**
	 * For remote failures to work some extra config is needed and special transports semantics apparently. 
	 * 
	 * @throws Exception
	 */
	@Test
	public void testRemoteChunkingShouldFailWithErrors() throws Exception {

		final BrokerContext broker = new BrokerContext("classpath:/broker/broker-context.xml");
		final MasterBatchContext masterBatchContext = new MasterBatchContext("testjob", "classpath:/master/master-batch-context.xml");
		final SlaveContext slaveContext = new SlaveContext("classpath:/slave/slave-batch-fail-context.xml");

		broker.start();
		masterBatchContext.start();
		slaveContext.start();

		waitForJobTopComplete(masterBatchContext);

		BatchStatus batchStatus = masterBatchContext.getBatchStatus();
		logger.debug("********** job finished with status: " + batchStatus);
		logger.info("slave chunks written: " + slaveContext.writtenCount() );
		Assert.assertEquals("Batch Job Status", BatchStatus.FAILED, batchStatus);

	}
	
	private void waitForJobTopComplete(MasterBatchContext batchContext) {
		boolean running = true;
		while (running) {
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
				throw new RuntimeException("exception during Thread.sleep()", e);
			}
			if ( isJobFinished(batchContext) ) {
				running = false;
			}
		}
	}
	

	private boolean isJobFinished(final MasterBatchContext batchContetxt) {
		final BatchStatus batchStatus = batchContetxt.getBatchStatus();
		if (    batchStatus == BatchStatus.STARTED  || 
				batchStatus == BatchStatus.STARTING || 
				batchStatus == BatchStatus.STOPPING || 
				batchStatus == null) {
			return false;
		} else {
			return true;
		}
	}

}
