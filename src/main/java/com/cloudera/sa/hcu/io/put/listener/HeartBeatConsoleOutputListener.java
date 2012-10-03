package com.cloudera.sa.hcu.io.put.listener;

public class HeartBeatConsoleOutputListener implements PutListener
{
	int waitMS;
	long timePostedMS;
	long lastPosting;
	long numberOfFiles;
	public HeartBeatConsoleOutputListener(int waitSeconds)
	{
		this.waitMS = waitSeconds * 1000;
		this.timePostedMS = 0;
		
	}

	public void onA1000Processed(long rowsAdded, long lastReadTime, long lastWriteTime)
	{
		timePostedMS = System.currentTimeMillis() - lastPosting;
		if (timePostedMS > waitMS)
		{
			System.out.println("Rows Added:" + rowsAdded + ", Read Time last 1000:" + lastReadTime + ", Write Time last 1000" + lastWriteTime);
			
			lastPosting = System.currentTimeMillis();
			timePostedMS = 0;
		}
	}

	public void onStart(long numberOfFiles)
	{
		lastPosting = System.currentTimeMillis();
		System.out.println("Starting: reading in " + numberOfFiles + " files");
		
		this.numberOfFiles = numberOfFiles;
	}

	public void onFinished(long rowsAdded)
	{
		System.out.println("Finished: process " + rowsAdded + " rows from " + numberOfFiles + " files.");
	}
}
