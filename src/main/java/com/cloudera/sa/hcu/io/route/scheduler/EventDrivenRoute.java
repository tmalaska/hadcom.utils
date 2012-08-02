package com.cloudera.sa.hcu.io.route.scheduler;

import java.io.File;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.Properties;

import com.cloudera.sa.hcu.io.put.listener.HeartBeatConsoleOutputListener;
import com.cloudera.sa.hcu.io.put.local.reader.AbstractLocalFileColumnReader;
import com.cloudera.sa.hcu.io.route.scheduler.thread.InputDirWatcherThread;
import com.cloudera.sa.hcu.io.route.scheduler.thread.PutExecutionThread;


public class EventDrivenRoute extends AbstractRoute
{

	
	public EventDrivenRoute(String routeNamePrefix, Properties prop) throws IOException
	{
		super(routeNamePrefix, prop);
	}
	
	@Override
	protected void init(String routePrefix, Properties prop) throws IOException
	{

	}

	@Override
	protected InputDirWatcherThread initInputDirWatchThread() throws Exception
	{
		return new InputDirWatcherThread(routeNamePrefix, inputDir, this, 2);
	}
	
	public void onDirWatcherFoundFiles(File[] files) throws Exception
	{
		//move files to processing dir
		for (File file: files)
		{
			File newFileLocal = new File(processDir, file.getName());
			boolean success = file.renameTo(newFileLocal);
			if (!success) {
			    System.err.println("<error> Unable to move file '" + newFileLocal.getAbsolutePath() + "' to processing directory '" + processDir + "'");
			}
			
			System.out.println("Loading file into Processing: " + newFileLocal.getAbsolutePath());
			
			//Start thread to start sending files
			PutExecutionThread putExecuter = new PutExecutionThread(newFileLocal, putProperties, this, this);
			putThreadPool.execute(putExecuter);
		}
	}

	public void onDirWatcherFiredExceptin(Exception e)
	{
		System.err.println("<error> got exception from thread ");
		e.printStackTrace();
	}
}
