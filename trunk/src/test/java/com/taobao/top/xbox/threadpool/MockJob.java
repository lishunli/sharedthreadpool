package com.taobao.top.xbox.threadpool;

/**
 * @author fangweng
 * 
 */
public class MockJob implements Job {

	private String tag;

	public MockJob(String tag) {
		this.tag = tag;
	}

	@Override
	public String getKey() {
		return "tag:" + tag;
	}

	@Override
	public void run() {
		try {
			Thread.sleep(1000);
			System.out.println(getKey() + " done..");
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
