package com.cloudera.sa.hcu.io.route.scheduler.thread;

import java.io.File;


public class InputDirWatcherThread  implements IRouteThread
{
	boolean continueRunning = true;
	File inputFile;
	DirWatcherObserver observer;
	int waitInSeconds;
	int reportEveryNSeconds;
	String routePrefixName;
	
	public InputDirWatcherThread(String routePrefixName,File inputFile, DirWatcherObserver observer, int waitInSeconds)
	{
		this(routePrefixName, inputFile, observer, waitInSeconds, waitInSeconds);
	}
	
	public InputDirWatcherThread(String routePrefixName, File inputFile, DirWatcherObserver observer, int waitInSeconds, int reportEveryNSeconds)
	{
		this.inputFile = inputFile;
		this.observer = observer;
		this.waitInSeconds = waitInSeconds;
		this.reportEveryNSeconds = reportEveryNSeconds;
		this.routePrefixName = routePrefixName;
	}

	public void run()
	{
		long lastRunTime = System.currentTimeMillis();
		
		System.out.println(routePrefixName + " - InputDirWatcherThread starting...");
		try
		{
			while (continueRunning)
			{
				synchronized(this)
				{
					if ((System.currentTimeMillis() - lastRunTime) > (waitInSeconds * 1000))
					{
						lastRunTime = System.currentTimeMillis();
						
						File[] files = inputFile.listFiles();
						if (files.length > 0)
						{
							observer.onDirWatcherFoundFiles(files);
						}
					}else
					{
						long millsLeft = (waitInSeconds *1000) - (System.currentTimeMillis() - lastRunTime);
						observer.onDirWatcherSecondsLeftReport((((double)(millsLeft))/1000.0));
					}
					this.wait(reportEveryNSeconds * 1000);
				}
			}
		}catch (Exception e)
		{
			e.printStackTrace();
			observer.onDirWatcherFiredExceptin( e);
		}
	}

	public void slowKill()
	{
		continueRunning = false;
		
	}

	
}
