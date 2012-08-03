package com.cloudera.sa.hcu.io.get;

public abstract class AbstractGetter
{
	long recordsWriten = 0;
	long lastTime = System.currentTimeMillis();
	
	abstract public void getFile(String[] args) throws Exception;
	
	public void onWritenRecord()
	{
		recordsWriten++;

		// Give a heart beat after 10 seconds
		if ((System.currentTimeMillis() - lastTime) > 10000)
		{
			System.out.println(" - Records so far: " + recordsWriten);
			lastTime = System.currentTimeMillis();
		}
	}
	
	public void onFinishedWriting()
	{

		System.out.println("Finished - Total Records: " + recordsWriten);
	}
}
