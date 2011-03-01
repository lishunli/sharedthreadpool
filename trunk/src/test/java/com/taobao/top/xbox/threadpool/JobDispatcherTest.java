package com.taobao.top.xbox.threadpool;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class JobDispatcherTest {

	private static final Log log = LogFactory.getLog(JobDispatcherTest.class);
	
	
	private ExecutorService es = Executors.newFixedThreadPool(100);
	private JobDispatcher jobDispatcher;
	private JobThreadWeightModel[] jobThreadWeightModel;
	private int maximumPoolSize = 10;
	
	@Before
	public void setUp() throws Exception {
		jobDispatcher = new JobDispatcher();
		jobDispatcher.setMaximumPoolSize(maximumPoolSize);
		//jobDispatcher.setPrivateUseFirst(false);
		
		jobThreadWeightModel = new JobThreadWeightModel[3];
		
		for(int i= 0 ; i < jobThreadWeightModel.length ; i++)
		{
			jobThreadWeightModel[i] = new JobThreadWeightModel();
			jobThreadWeightModel[i].setKey("tag:" + i);
		}

		//模型中tag:0保留2个资源，tag:1保留3个，tag:2最大3个资源，默认资源池5。
		jobThreadWeightModel[0].setType(JobThreadWeightModel.WEIGHT_MODEL_LEAVE);
		jobThreadWeightModel[0].setValue(2);
		
		jobThreadWeightModel[1].setType(JobThreadWeightModel.WEIGHT_MODEL_LEAVE);
		jobThreadWeightModel[1].setValue(3);
		
		jobThreadWeightModel[2].setType(JobThreadWeightModel.WEIGHT_MODEL_LIMIT);
		jobThreadWeightModel[2].setValue(1);
		
		jobDispatcher.setJobThreadWeightModel(jobThreadWeightModel);
		jobDispatcher.init();
		jobDispatcher.start();
	}

	@After
	public void tearDown() throws Exception {
		jobDispatcher.stopDispatcher();
	}

	@Test
	public void testSubmitConcurrentJob() throws Exception {
		final Job tag0Job = new MockJob("0");
		final Job tag1Job = new MockJob("1");
		final Job tag2Job = new MockJob("2");
		final Job tag3Job = new MockJob("3");

		for (int i = 0; i < 2; i++) {
			es.submit(new Runnable() {
				public void run() {
					jobDispatcher.submitJob(tag0Job);
					jobDispatcher.submitJob(tag1Job);
					jobDispatcher.submitJob(tag2Job);
					jobDispatcher.submitJob(tag3Job);
				}
			});
		}

//		Timer timer = new Timer();
//		timer.schedule(new TimerTask() {
//			public void run() {
//				// after all threads finished, all the counter should be zero
//				log.info("totalCounter=" + jobDispatcher.getTotalCounter());
//				log.info("defaultCounter=" + jobDispatcher.getDefaultCounter());
//				log.info("defaultQueueCounter=" + jobDispatcher.getDefaultQueueCounter());
//				log.info("map=" + jobDispatcher.getCounterPool());
//				log.info("queueMap=" + jobDispatcher.getQueueCounterPool());
//			}
//		}, 100, 200);
		
		int i = 0;
		int c = 0;
		
		while(true)
		{
			if (jobDispatcher.getTotalCounter().get() == 0)
				i += 1;
			else
				i = 0;
			
			if (i > 10)
				break;
			
			c += 1;
			
			if (c == 5)
			{
				jobThreadWeightModel[2].setValue(2);
				jobDispatcher.setJobThreshold(jobDispatcher.buildWeightModel(jobThreadWeightModel));
				log.info("--- change weight config---");
			}
				
			
//			log.info("totalCounter=" + jobDispatcher.getTotalCounter());
			log.info("----------");
			log.info("defaultCounter=" + jobDispatcher.getDefaultCounter());
			log.info("defaultQueueCounter=" + jobDispatcher.getDefaultQueueCounter());
			log.info("map=" + jobDispatcher.getCounterPool());
			log.info("queueMap=" + jobDispatcher.getQueueCounterPool());
			log.info("----------");
			
			Thread.sleep(100);
		}

	}

//	@Test
//	public void testSubmitSimpleJob(){
//		Job tag0Job = new MockJob("0");
//		Job tag1Job = new MockJob("1");
//		Job tag2Job = new MockJob("2");
//		Job tag3Job = new MockJob("3");
//		
//		//简单测试
//		for(int i=0; i < 10 ; i++)
//			jobDispatcher.submitJob(tag0Job);
//
//		Map<String, Object> status = jobDispatcher.getCurrentThreadStatus();
//		Assert.assertEquals(status.get("tag:0"), "2,0");
//		Assert.assertEquals(status.get(JobDispatcher.DEFAULT_COUNTER), new Integer(5));
//		
//		try {
//			Thread.sleep(88000);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//
//		for(int i=0; i < 10 ; i++)
//			jobDispatcher.submitJob(tag1Job);
//
//		status = jobDispatcher.getCurrentThreadStatus();
//		Assert.assertEquals(status.get("tag:1"), new Integer(3));
//		Assert.assertEquals(status.get(JobDispatcher.DEFAULT_COUNTER), new Integer(5));
//		
//		try {
//			Thread.sleep(8000);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//		
//		for(int i=0; i < 10 ; i++)
//			jobDispatcher.submitJob(tag2Job);
//
//		status = jobDispatcher.getCurrentThreadStatus();
//		Assert.assertEquals(status.get("tag:2"), new Integer(4));
//		
//		try {
//			Thread.sleep(10000);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//		
//		for(int i=0; i < 10 ; i++)
//			jobDispatcher.submitJob(tag3Job);
//
//		status = jobDispatcher.getCurrentThreadStatus();
//		Assert.assertEquals(status.get(JobDispatcher.DEFAULT_COUNTER), new Integer(5));
//		
//		try {
//			Thread.sleep(10000);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//	}
//	
//	@Test
//	public void testSubmitComplexJob(){
//		Job tag0Job = new MockJob("0");
//		//Job tag1Job = new MockJob("1");
//		//Job tag2Job = new MockJob("2");
//		Job tag3Job = new MockJob("3");
//		
//		//简单测试
//		for(int i=0; i < 10 ; i++) {
//			jobDispatcher.submitJob(tag0Job);
//		}
//
//		jobDispatcher.submitJob(tag3Job);
//		
//		Map<String, Integer> status = jobDispatcher.getCurrentThreadStatus();
//		Assert.assertEquals(status.get("tag:0"),new Integer(2));
//		Assert.assertEquals(status.get(JobDispatcher.DEFAULT_COUNTER), new Integer(5));
//		Assert.assertTrue(status.get(JobDispatcher.TOTAL_COUNTER) >= new Integer(7));
//		Assert.assertTrue(status.get(JobDispatcher.QUEUE_JOB_LENGTH) <= new Integer(4));
//		
//		try {
//			Thread.sleep(8000);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//
//		status = jobDispatcher.getCurrentThreadStatus();
//		Assert.assertEquals(status.get("tag:0"),new Integer(0));
//		Assert.assertEquals(status.get(JobDispatcher.DEFAULT_COUNTER),new Integer(0));
//		Assert.assertTrue(status.get(JobDispatcher.TOTAL_COUNTER) >= new Integer(0));
//		Assert.assertTrue(status.get(JobDispatcher.QUEUE_JOB_LENGTH) >= new Integer(0));
//	}

}
