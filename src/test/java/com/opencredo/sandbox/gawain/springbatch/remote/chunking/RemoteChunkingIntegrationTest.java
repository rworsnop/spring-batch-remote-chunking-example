package com.opencredo.sandbox.gawain.springbatch.remote.chunking;

import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.springframework.batch.core.BatchStatus;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.integration.MessageChannel;
import org.springframework.integration.core.MessagingOperations;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.util.FileSystemUtils;


@DirtiesContext
public class RemoteChunkingIntegrationTest {

	private static final Log logger = LogFactory.getLog(RemoteChunkingIntegrationTest.class);

    @Test
    public void mytest(){
        ApplicationContext context = new ClassPathXmlApplicationContext("classpath:/master/master-integration-context.xml");
        MessagingOperations ops = context.getBean(MessagingOperations.class);
        MessageChannel channel1 = (MessageChannel)context.getBean("toRabbit");
        MessageChannel channel3 = (MessageChannel)context.getBean("fromRabbit");
        while (ops == null){
            channel1.send(MessageBuilder.withPayload("hello").build());
            channel3.send(MessageBuilder.withPayload("hello").build());

            //           ops.send(MessageBuilder.withPayload("hello").build());
        }

    }
	
	@Test
	public void testRemoteChunkingOnMultipleSlavesShouldLoadBalanceAndComplete() throws Exception {


 //       final BrokerContext broker = (BrokerContext) new BrokerContext("classpath:/broker/broker-context.xml").start();
		final MasterBatchContext masterBatchContext = new MasterBatchContext("testjob", "classpath:/master/master-batch-context.xml");
		final SlaveContext slaveContext1 = new SlaveContext("classpath:/slave/slave1-batch-context.xml");
		final SlaveContext slaveContext2 = new SlaveContext("classpath:/slave/slave2-batch-context.xml");

		masterBatchContext.start();

 //       Thread.sleep(700000);

		slaveContext1.start();
		slaveContext2.start();


		
		BatchJobTestHelper.waitForJobTopComplete(masterBatchContext);

		final BatchStatus batchStatus = masterBatchContext.getBatchStatus();
		logger.info("job finished with status: " + batchStatus);
		Assert.assertEquals("Batch Job Status", BatchStatus.COMPLETED, batchStatus);
		logger.info("slave 1 chunks written: " + slaveContext1.writtenCount() );
		logger.info("slave 2 chunks written: " + slaveContext2.writtenCount());
		Assert.assertEquals("slave chunks written", 5, slaveContext1.writtenCount());
		Assert.assertEquals("slave chunks written", 5, slaveContext2.writtenCount());


	}	
	
	

}
