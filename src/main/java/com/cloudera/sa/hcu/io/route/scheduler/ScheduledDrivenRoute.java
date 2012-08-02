package com.cloudera.sa.hcu.io.route.scheduler;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.Map.Entry;

import com.cloudera.sa.hcu.io.put.listener.HeartBeatConsoleOutputListener;
import com.cloudera.sa.hcu.io.put.local.reader.AbstractLocalFileColumnReader;
import com.cloudera.sa.hcu.io.route.scheduler.thread.InputDirWatcherThread;
import com.cloudera.sa.hcu.io.route.scheduler.thread.PutExecutionThread;
import com.cloudera.sa.hcu.utils.PropertyUtils;

public class ScheduledDrivenRoute extends AbstractRoute
{

	public static final String CONF_BATCH_SEND_EVERY_N_MINUTES = ".batch.send.every.n.min";
	
	int internalTimeInMinutes;
	
	public ScheduledDrivenRoute(String routeNamePrefix, Properties prop) throws IOException
	{
		super(routeNamePrefix, prop);
	}
	
	@Override
	protected void init(String routeName, Properties prop) throws IOException
	{
		internalTimeInMinutes = PropertyUtils.getIntProperty(prop, routeName + CONF_BATCH_SEND_EVERY_N_MINUTES);
	}

	@Override
	protected InputDirWatcherThread initInputDirWatchThread() throws Exception
	{
		return new InputDirWatcherThread(routeNamePrefix, inputDir, this, internalTimeInMinutes * 60, internalTimeInMinutes * 6);
	}
	
	public void onDirWatcherFoundFiles(File[] files) throws Exception
	{
		//move files to processing dir
		
		File batchDir = new File(processDir + "_" + System.currentTimeMillis());
		batchDir.mkdirs();
		
		for (File file: files)
		{
			File newLocation = new File(batchDir, file.getName());
			
			boolean success = file.renameTo(newLocation);
			
			if (!success) {
			    System.err.println("<error> Unable to move file '" + file.getAbsolutePath() + "' to processing directory '" + processDir + "'");
			}
		}
		
		//Start thread to start sending files
		PutExecutionThread putExecuter = new PutExecutionThread(batchDir, putProperties, this, this);
		putThreadPool.execute(putExecuter);
	}

	public void onDirWatcherFiredExceptin(Exception e)
	{
		System.err.println(routeNamePrefix + " - <error> got exception from thread ");
		e.printStackTrace();
		throw new RuntimeException(e);
	}

	public void onDirWatcherSecondsLeftReport(double secondsLeft )
	{
		System.err.println(routeNamePrefix + " - " + secondsLeft + " seconds lefts before next batch.");
	}

}

