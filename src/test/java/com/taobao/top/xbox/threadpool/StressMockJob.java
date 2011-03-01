package com.taobao.top.xbox.threadpool;

import java.util.concurrent.atomic.AtomicInteger;

public class StressMockJob implements Job{

	private String tag;
	private AtomicInteger counter;

	public StressMockJob(String tag,AtomicInteger counter) {
		this.tag = tag;
		this.counter = counter;
	}

	@Override
	public String getKey() {
		if (tag != null)
			return "tag:" + tag;
		else
			return "tag:default";
	}

	@Override
	public void run() {
		
		counter.incrementAndGet();
		
		try 
		{
			Thread.sleep(1000);
		}
		catch (InterruptedException e) 
		{
			e.printStackTrace();
		}
		finally
		{
			counter.decrementAndGet();
		}		

	}
}
