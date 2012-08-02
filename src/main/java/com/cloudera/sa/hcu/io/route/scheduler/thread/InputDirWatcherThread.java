package com.cloudera.sa.hcu.io.route.scheduler.thread;

import java.io.File;


public class InputDirWatcherThread  implements IRouteThread
{
	boolean continueRunning = true;
	File inputFile;
	DirWatcherObserver observer;
	int internalInSec;
	
	public InputDirWatcherThread(File inputFile, DirWatcherObserver observer, int internalInSec)
	{
		this.inputFile = inputFile;
		this.observer = observer;
		this.internalInSec = internalInSec;
	}

	public void run()
	{
		System.out.println("InputDirWatcherThread starting...");
		try
		{
			while (continueRunning)
			{
				synchronized(this)
				{
					File[] files = inputFile.listFiles();
					if (files.length > 0)
					{
						observer.onDirWatcherFoundFiles(files);
					}
					this.wait(internalInSec * 1000);
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
