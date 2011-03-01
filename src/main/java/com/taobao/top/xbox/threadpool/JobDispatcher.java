package com.taobao.top.xbox.threadpool;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.top.xbox.util.NamedThreadFactory;

/**
 * 简单的任务分发类，支持根据线程权重模型来分配资源，当前支持两类
 *  1. 根据某一类key可以预留多少资源独享。
 *  2. 根据某一类key可以限制最多使用多少资源。
 * 
 * @author fangweng
 */
public class JobDispatcher extends Thread {

	private static final Log log = LogFactory.getLog(JobDispatcher.class);

	public static final String DEFAULT_COUNTER = "_defaultCounter";
	public static final String TOTAL_COUNTER = "_totalCounter";
	public static final String QUEUE_JOB_LENGTH = "_queueJobLength";
	public static final String DEFAULT_QUEUE_COUNTER = "_defaultQueueCounter";
	public static final String COUNTER_SNAPSHOT = "counterSnapShot";
	public static final String QUEUE_SNAPSHOT = "queueSnapShot";

	private BlockingQueue<Job> jobQueue;
	private JobThreadPoolExecutor threadPool;
	private JobThreadWeightModel[] jobThreadWeightModel;
	private Map<String, AtomicInteger> counterPool;
	private Map<String, AtomicInteger> queueCounterPool;//用于记录在队列中的不同资源类型数量
	private Map<String, AtomicInteger> defaultCounterPool;//用于记录默认计数器中不同资源类型的线程消耗
	
	private JobThreshold jobThreshold;//任务阀值
	
	private AtomicInteger defaultCounter;
	private AtomicInteger defaultQueueCounter;
	private AtomicInteger totalCounter;
	private int maximumPoolSize = 200;
	private int maximumQueueSize = 1000;
	private boolean isRunning = true;
	/**
	 * 保留型的资源配置，优先使用保留的资源
	 */
	private boolean privateUseFirst = true;
	

	public JobThreshold getJobThreshold() {
		return jobThreshold;
	}

	public void setJobThreshold(JobThreshold jobThreshold) {
		this.jobThreshold = jobThreshold;
	}

	public boolean isPrivateUseFirst() {
		return privateUseFirst;
	}

	public void setPrivateUseFirst(boolean privateUseFirst) {
		this.privateUseFirst = privateUseFirst;
	}
	

	public AtomicInteger getDefaultQueueCounter() {
		return defaultQueueCounter;
	}

	public void setDefaultQueueCounter(AtomicInteger defaultQueueCounter) {
		this.defaultQueueCounter = defaultQueueCounter;
	}
	
	

	public Map<String, AtomicInteger> getQueueCounterPool() {
		return queueCounterPool;
	}

	public void setQueueCounterPool(Map<String, AtomicInteger> queueCounterPool) {
		this.queueCounterPool = queueCounterPool;
	}

	public Map<String, Object> getCurrentThreadStatus() {
		Map<String, Object> status = new HashMap<String, Object>();

		status.put(DEFAULT_COUNTER, defaultCounter.get());
		status.put(QUEUE_JOB_LENGTH, jobQueue.size());
		status.put(TOTAL_COUNTER, totalCounter.get());
		status.put(DEFAULT_QUEUE_COUNTER, defaultQueueCounter.get());
		
		Iterator<Entry<String, AtomicInteger>> entrys = counterPool.entrySet().iterator();

		while (entrys.hasNext()) {
			Entry<String, AtomicInteger> e = entrys.next();
			
			status.put(e.getKey(), 
					new StringBuilder("private:").append(e.getValue())
					.append(",total:").append(defaultCounterPool.get(e.getKey()).get())
						.append(",queue:").append(queueCounterPool.get(e.getKey()).get()).toString());
			
		}
		
		return status;
	}
	
	

	public JobThreadPoolExecutor getThreadPool() {
		return threadPool;
	}

	public void setThreadPool(JobThreadPoolExecutor threadPool) {
		this.threadPool = threadPool;
	}


	public void init() {
		if (threadPool == null)
			threadPool = new JobThreadPoolExecutor(maximumPoolSize, 0L, TimeUnit.SECONDS,
				new LinkedBlockingQueue<Runnable>(maximumQueueSize), new NamedThreadFactory("jobDispatcher_worker"), this);

		defaultCounter = new AtomicInteger(0);
		totalCounter = new AtomicInteger(0);
		defaultQueueCounter = new AtomicInteger(0);
		jobQueue = new LinkedBlockingQueue<Job>(maximumQueueSize);
		counterPool = new ConcurrentHashMap<String, AtomicInteger>();
		queueCounterPool = new ConcurrentHashMap<String, AtomicInteger>();
		defaultCounterPool = new ConcurrentHashMap<String, AtomicInteger>();
		
		jobThreshold = buildWeightModel(jobThreadWeightModel);
	}

	public void stopDispatcher() {
		if (threadPool != null)
			threadPool.shutdownNow();

		if (jobQueue != null)
			jobQueue.clear();

		if (counterPool != null)
			counterPool.clear();

		if (jobThreshold.getThresholdPool() != null)
			jobThreshold.getThresholdPool().clear();
		
		if (queueCounterPool != null)
			queueCounterPool.clear();

		isRunning = false;
		this.interrupt();
	}
	
	/**
	 * 运行期修改模型
	 * @param newJobThreadWeightModel
	 */
	public JobThreshold buildWeightModel(JobThreadWeightModel[] newJobThreadWeightModel)
	{
		JobThreshold newJobThreshold = new JobThreshold();
		newJobThreshold.setDefaultThreshold(maximumPoolSize);
		
		if (newJobThreadWeightModel != null && newJobThreadWeightModel.length > 0) {
			for (JobThreadWeightModel j : newJobThreadWeightModel) {
				try {
					
					if (counterPool.get(j.getKey()) == null)
					{
						counterPool.put(j.getKey(), new AtomicInteger(0));
						queueCounterPool.put(j.getKey(), new AtomicInteger(0));
						defaultCounterPool.put(j.getKey(), new AtomicInteger(0));
					}
					
					if (j.getValue() == 0) continue;

					if (j.getType().equals(JobThreadWeightModel.WEIGHT_MODEL_LIMIT)) {
						newJobThreshold.getThresholdPool().put(j.getKey(), j.getValue());
					} else if (j.getType().equals(JobThreadWeightModel.WEIGHT_MODEL_LEAVE)) {
						newJobThreshold.getThresholdPool().put(j.getKey(), j.getValue());
						newJobThreshold.setDefaultThreshold(newJobThreshold.getDefaultThreshold() - j.getValue());
					} else
						throw new RuntimeException("thread weight config not support!");
				} catch (Exception ex) {
					log.error("create jobWeightModels: " + j.getKey() + " error!", ex);
				}
			}

			if (newJobThreshold.getDefaultThreshold() <= 0)
				throw new RuntimeException("total leave resource > total resource...");

			for (JobThreadWeightModel j : newJobThreadWeightModel) {
				try {
					if (j.getValue() == 0)
						continue;

					if (j.getType().equals(JobThreadWeightModel.WEIGHT_MODEL_LIMIT)) {
						if (newJobThreshold.getThresholdPool().get(j.getKey()) > newJobThreshold.getDefaultThreshold())
							newJobThreshold.getThresholdPool().put(j.getKey(), -newJobThreshold.getDefaultThreshold());
						else
							newJobThreshold.getThresholdPool().put(j.getKey(), -newJobThreshold.getThresholdPool().get(j.getKey()));
					}
				} catch (Exception ex) {
					log.error("create jobWeightModels: " + j.getKey() + " error!", ex);
				}
			}
		}
		
		return newJobThreshold;
	}

	/**
	 * 提交任务
	 */
	public void submitJob(Job job) {
		// 第一层做总量判断，同时锁定总资源
		if (totalCounter.incrementAndGet() > this.maximumPoolSize) {
			totalCounter.decrementAndGet();
			pushJob(job);
			return;
		}

		String key = job.getKey();
		Integer threshold = null;
		if (key != null)
			threshold = jobThreshold.getThresholdPool().get(key);
		
		boolean hasResource = false;

		if (key == null || threshold == null) {
			if (defaultCounter.incrementAndGet() > jobThreshold.getDefaultThreshold()) {
				defaultCounter.decrementAndGet();
			} else {
				hasResource = true;
			}
		} else {
			AtomicInteger counter = counterPool.get(key);
			if (threshold > 0) {// leave mode
				
				if (privateUseFirst){
					if (counter.incrementAndGet() > threshold) {
						counter.decrementAndGet();
						if (defaultCounter.incrementAndGet() > jobThreshold.getDefaultThreshold()) {
							defaultCounter.decrementAndGet();
						} else {
							hasResource = true;
						}
					} else {
						hasResource = true;
					}
				}
				else
				{
					if (defaultCounter.incrementAndGet() > jobThreshold.getDefaultThreshold()) {
						defaultCounter.decrementAndGet();
						if (counter.incrementAndGet() > threshold) 
							counter.decrementAndGet();
						else
							hasResource = true;
					} else {
						hasResource = true;
					}
				}
				
			} else {// limit mode
				if (counter.incrementAndGet() > -threshold) {
					counter.decrementAndGet();
				} else {
					if (defaultCounter.incrementAndGet() > jobThreshold.getDefaultThreshold()) {
						defaultCounter.decrementAndGet();
						counter.decrementAndGet();
					} else {
						hasResource = true;
					}
				}
			}
		}

		if (hasResource) 
		{
			threadPool.execute(job);
		} else {
			totalCounter.decrementAndGet();
			
			pushJob(job);
			
		}
	}

	public JobThreadWeightModel[] getJobThreadWeightModel() {
		return jobThreadWeightModel;
	}

	public void setJobThreadWeightModel(JobThreadWeightModel[] jobThreadWeightModel) {
		this.jobThreadWeightModel = jobThreadWeightModel;
	}

	public void setJobThreadWeightModel(List<JobThreadWeightModel> jobThreadWeightModel) {
		this.jobThreadWeightModel = jobThreadWeightModel.toArray(new JobThreadWeightModel[0]);
	}

	public BlockingQueue<Job> getJobQueue() {
		return jobQueue;
	}

	public void setJobQueue(BlockingQueue<Job> jobQueue) {
		this.jobQueue = jobQueue;
	}

	public int getMaximumPoolSize() {
		return maximumPoolSize;
	}

	public void setMaximumPoolSize(int maximumPoolSize) {
		this.maximumPoolSize = maximumPoolSize;
	}

	public int getMaximumQueueSize() {
		return this.maximumQueueSize;
	}

	public void setMaximumQueueSize(int maximumQueueSize) {
		this.maximumQueueSize = maximumQueueSize;
	}

	public Map<String, AtomicInteger> getCounterPool() {
		return counterPool;
	}

	public Map<String, Integer> getThresholdPool() {
		return jobThreshold.getThresholdPool();
	}

	public int getDefaultThreshold() {
		return jobThreshold.getDefaultThreshold();
	}

	public AtomicInteger getDefaultCounter() {
		return defaultCounter;
	}

	public AtomicInteger getTotalCounter() {
		return totalCounter;
	}

	@Override
	public void run() {
		try {
			while (isRunning) {
				Job job = jobQueue.poll(1, TimeUnit.SECONDS);

				if (job != null) 
				{
					popJob(job);
					submitJob(job);
				}
				else 
				{
					Thread.sleep(100);
				}
			}
		} catch (InterruptedException ex) {
			// do nothing
		}
	}
	
	public void pushJob(Job job)
	{
		if (!jobQueue.offer(job)) {// 补偿job
			throw new RuntimeException("can't submit job, queue full...");
		}
		else
		{
			if (queueCounterPool.get(job.getKey()) != null)
				queueCounterPool.get(job.getKey()).incrementAndGet();
			else
				defaultQueueCounter.incrementAndGet();
		}
	}
	
	public void popJob(Job job)
	{
		if (queueCounterPool.get(job.getKey()) != null)
			queueCounterPool.get(job.getKey()).decrementAndGet();
		else
			defaultQueueCounter.decrementAndGet();
	}
	
	public void beforeExecuteJob(Job job)
	{
		//用于统计默认线程中不同的请求消耗的线程数
		if (job.getKey() != null)
		{
			AtomicInteger tmpCounter = defaultCounterPool.get(job.getKey());
			
			if (tmpCounter != null)
			{
				tmpCounter.incrementAndGet();
			}
		}
	}

	public void releaseJob(Job job) {
		String key = job.getKey();
		this.getTotalCounter().decrementAndGet();

		if (this.getCounterPool().size() == 0) {
			this.getDefaultCounter().decrementAndGet();
		} else {
			AtomicInteger counter = this.getCounterPool().get(key);
			if (counter != null) {
				
				defaultCounterPool.get(key).decrementAndGet();
				
				if (counter.decrementAndGet() < 0) { // leave mode (use default)
					counter.incrementAndGet();
					this.getDefaultCounter().decrementAndGet();
				} else {
					Integer size = this.getThresholdPool().get(key);
					if (size < 0) { // limit mode (use default)
						// counter.decrementAndGet();
						this.getDefaultCounter().decrementAndGet();
					} else { // leave mode (use itself)
						// nothing to do
					}
				}
			} else {
				this.getDefaultCounter().decrementAndGet();
			}
		}
	}

}
