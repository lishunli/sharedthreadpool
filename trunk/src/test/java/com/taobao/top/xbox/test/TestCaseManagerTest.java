package com.taobao.top.xbox.test;

import junit.framework.Assert;

import org.junit.Test;


/**
 * @author fangweng
 *
 */
public class TestCaseManagerTest
{

	@Test
	public void testTestCaseManager()
	{
		TestCaseManager caseManager = new TestCaseManager();
		
		caseManager.register(new My(true), "mytest1");
		caseManager.register(new My(false), "mytest2");
		caseManager.register(new My(true), "mytest3");
		
		String result = TestCaseManager.getStrFromTestResults(caseManager.doBatchTest());
		
		System.out.print(result);
		Assert.assertTrue(result.indexOf("mytest2") >= 0);

	}
	
	class My implements ITopTestCase
	{
		boolean result;
		
		public My(boolean result)
		{
			this.result = result;
		}
		
		@Override
		public boolean doTest()
		{
			return result;
		}
		
	}
}
