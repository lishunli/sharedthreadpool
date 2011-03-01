/**
 * 
 */
package com.taobao.top.xbox.threadpool;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;

import com.taobao.top.xbox.test.ITopTestCase;
import com.taobao.top.xbox.util.NamedThreadFactory;

/**
 * @author fangweng
 * @email fangweng@taobao.com
 * @date 2011-2-28
 *
 */
public class JobThreadPoolStressTest{
	
	private static final Log log = LogFactory.getLog(JobThreadPoolStressTest.class);
	
	private JobDispatcher jobDispatcher;
	private JobThreadWeightModel[] jobThreadWeightModel;
	private int maximumPoolSize = 1000;
	private ThreadPoolExecutor threadPool;
	
	@Before
	public void setUp() throws Exception {
		jobDispatcher = new JobDispatcher();
		jobDispatcher.setMaximumPoolSize(maximumPoolSize);
		
		jobThreadWeightModel = new JobThreadWeightModel[3];
		
		for(int i= 0 ; i < jobThreadWeightModel.length ; i++)
		{
			jobThreadWeightModel[i] = new JobThreadWeightModel();
			jobThreadWeightModel[i].setKey("tag:" + i);
		}

		//模型中tag:0保留50个资源，tag:1保留50个，tag:2最大500个资源，默认资源池900。
		jobThreadWeightModel[0].setType(JobThreadWeightModel.WEIGHT_MODEL_LEAVE);
		jobThreadWeightModel[0].setValue(50);
		
		jobThreadWeightModel[1].setType(JobThreadWeightModel.WEIGHT_MODEL_LEAVE);
		jobThreadWeightModel[1].setValue(50);
		
		jobThreadWeightModel[2].setType(JobThreadWeightModel.WEIGHT_MODEL_LIMIT);
		jobThreadWeightModel[2].setValue(800);
		
		jobDispatcher.setJobThreadWeightModel(jobThreadWeightModel);
		jobDispatcher.init();
		jobDispatcher.start();
		
		
		threadPool = new ThreadPoolExecutor(maximumPoolSize,maximumPoolSize,0L, TimeUnit.SECONDS,
				new LinkedBlockingQueue<Runnable>(1000), new NamedThreadFactory("jobDispatcher_worker2"));
	}

	@After
	public void tearDown() throws Exception {
		jobDispatcher.stopDispatcher();
		threadPool.shutdown();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		int times = 500;
		boolean isNormalTest = false;
		AtomicInteger counter = new AtomicInteger(0);
		
		if (args != null && args.length > 0)
		{
			if (args.length >= 1)
				times = Integer.valueOf(args[0]);
			
			if (args.length >= 2)
				isNormalTest = Boolean.valueOf(args[1]);
		}
		
		JobThreadPoolStressTest stressTest = new JobThreadPoolStressTest();
		
		try
		{
			stressTest.setUp();
			
			long beg = System.currentTimeMillis();
			
			for(int i=0; i < times ; i++)
			{
				StressMockJob tag0Job = new StressMockJob("0",counter);
				StressMockJob tag1Job = new StressMockJob("1",counter);
				StressMockJob tag2Job = new StressMockJob("2",counter);
				StressMockJob tag3Job = new StressMockJob("3",counter);
				
				try
				{
					if (isNormalTest)
					{
						stressTest.threadPool.execute(tag0Job);
						stressTest.threadPool.execute(tag1Job);
						stressTest.threadPool.execute(tag2Job);
						stressTest.threadPool.execute(tag3Job);
					}
					else
					{
						stressTest.jobDispatcher.submitJob(tag0Job);
						stressTest.jobDispatcher.submitJob(tag1Job);
						stressTest.jobDispatcher.submitJob(tag2Job);
						stressTest.jobDispatcher.submitJob(tag3Job);
					}
				}
				catch(Exception ex)
				{
					ex.printStackTrace();
				}
				
			}
			
			
			while (counter.get() > 0)
			{
				Thread.sleep(100);
				
//				if (!isNormalTest)
//				{	
//					log.info(" status : " + stressTest.jobDispatcher.getCurrentThreadStatus());
//					log.info("----------");
//				}
				
			}
			
			System.out.println("total consume: " + String.valueOf(System.currentTimeMillis() - beg));
			
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		finally
		{
			try {
				stressTest.tearDown();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		

	}

}
