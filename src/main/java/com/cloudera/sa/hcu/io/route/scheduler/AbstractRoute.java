package com.cloudera.sa.hcu.io.route.scheduler;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.cloudera.sa.hcu.io.put.hdfs.writer.AbstractHdfsWriter;
import com.cloudera.sa.hcu.io.put.listener.PutListener;
import com.cloudera.sa.hcu.io.route.scheduler.thread.DirWatcherObserver;
import com.cloudera.sa.hcu.io.route.scheduler.thread.InputDirWatcherThread;
import com.cloudera.sa.hcu.io.route.scheduler.thread.PutExecuterObserver;
import com.cloudera.sa.hcu.utils.PropertyReaderUtils;

public abstract class AbstractRoute implements IRouteWorker, DirWatcherObserver, PutExecuterObserver, PutListener
{
	InputDirWatcherThread inputDirWatcher  = null;
	
	public static final String CONF_INPUT_DIR = ".input.dir";
	public static final String CONF_PROCESS_DIR = ".process.dir";
	public static final String CONF_SUCCESS_DIR = ".success.dir";
	public static final String CONF_FAILURE_DIR = ".failure.dir";
	public static final String CONF_CORE_THREAD_COUNT = ".core.thread.count";
	public static final String CONF_MAX_THREAD_COUNT = ".max.thread.count";
	
	
	protected File inputDir;
	protected File processDir;
	protected File successDir;
	protected File failureDir;
	int coreThreadCount;
	int maxThreadCount;
	
	Properties putProperties;
	
	Object foo = new Object();
	
	ThreadPoolExecutor putThreadPool;
	
	String routeNamePrefix;
	
	public AbstractRoute(String routeNamePrefix, Properties prop) throws IOException
	{
		this.routeNamePrefix = routeNamePrefix;
		inputDir = prepDirectory(routeNamePrefix + CONF_INPUT_DIR, prop.getProperty(routeNamePrefix + CONF_INPUT_DIR));
		processDir = prepDirectory(routeNamePrefix + CONF_PROCESS_DIR, prop.getProperty(routeNamePrefix + CONF_PROCESS_DIR));
		successDir = prepDirectory(routeNamePrefix + CONF_SUCCESS_DIR, prop.getProperty(routeNamePrefix + CONF_SUCCESS_DIR));
		failureDir = prepDirectory(routeNamePrefix + CONF_FAILURE_DIR, prop.getProperty(routeNamePrefix + CONF_FAILURE_DIR));
		 
		coreThreadCount = PropertyReaderUtils.getIntProperty(prop, routeNamePrefix + CONF_CORE_THREAD_COUNT);
		maxThreadCount = PropertyReaderUtils.getIntProperty(prop, routeNamePrefix + CONF_MAX_THREAD_COUNT);
		
		putProperties = new Properties();
		int lengthOfRoutePrefix = routeNamePrefix.length();
		for (Entry<Object, Object> entry: prop.entrySet())
		{
			String key = entry.getKey().toString();
			if (key.startsWith(routeNamePrefix) && key.length() > lengthOfRoutePrefix + 1)
			{
				putProperties.put(key.substring(lengthOfRoutePrefix + 1) , entry.getValue());
			}
		}
		
		prepHdfsDirectory(routeNamePrefix, prop);
		
		init(routeNamePrefix, prop);
	}

	private void prepHdfsDirectory(String routeNamePrefix, Properties prop) throws IOException
	{
		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);
		String hdfsRootDirectory = PropertyReaderUtils.getStringProperty(prop, routeNamePrefix + "." + AbstractHdfsWriter.CONF_OUTPUT_PATH);
		Path rootDirectory = new Path(hdfsRootDirectory);
		
		System.out.println(routeNamePrefix + "- Creating " + rootDirectory + " directory in hdfs.");
		System.out.println(hdfs.mkdirs(rootDirectory));
		hdfs.close();
	}
	 
	public void onPutSuccess(File sourceFile)
	{
		if (sourceFile.renameTo(new File(successDir, sourceFile.getName())))
		{
			System.out.println(routeNamePrefix + " - Moved '" + sourceFile.getName() + "' to successDir.");
		}else
		{
			throw new RuntimeException(routeNamePrefix + " - failed to move '" + sourceFile.getName() + "'");
		}
	}

	public void onPutFailure(File sourceFile)
	{
		if (sourceFile.renameTo(new File(failureDir, sourceFile.getName())))
		{
			System.out.println(routeNamePrefix + " - Moved '" + sourceFile.getName() + "' to failureDir.");
		}else
		{
			throw new RuntimeException(routeNamePrefix + " - failed to move '" + sourceFile.getName() + "'");
		}
	}
	
	abstract protected void init(String routeNamePrefix, Properties prop) throws IOException;
	
	public void start() throws Exception
	{
		System.out.println("Route starting...");
		synchronized(foo)
		{
			if (inputDirWatcher != null)
			{
				throw new RuntimeException("Route is already running");
			}
			putThreadPool = new ThreadPoolExecutor(coreThreadCount, maxThreadCount, 1, TimeUnit.MINUTES, new ArrayBlockingQueue<Runnable>(maxThreadCount, true));
			
			
			inputDirWatcher = initInputDirWatchThread();
			Thread thread = new Thread(inputDirWatcher);
			thread.start();
			
		}
	}
	
	
	protected File prepDirectory(String confName, String path)
	{
		if (path == null || path.isEmpty())
		{
			throw new RuntimeException(confName + " value '" + path + "' is not a directory.");
		}
		
		File dir = new File(path);
		
		if (dir.exists() == false)
		{
			dir.mkdirs();
		}
		
		if (dir.isDirectory() == false)
		{
			throw new RuntimeException(confName + " value '" + path + "' is not a directory.");
		}
		return dir;
	}
	
	abstract protected InputDirWatcherThread initInputDirWatchThread() throws Exception;
	
	
    public void softStop() throws Exception
    {
		synchronized(inputDirWatcher)
		{
			if (inputDirWatcher == null)
			{
				throw new RuntimeException("Route cannot stop.  Thread is null");
			}
			
			inputDirWatcher.slowKill();
			putThreadPool.shutdown();
			inputDirWatcher = null;
		}
    }
    
    public void hardStop() throws Exception
    {
    	synchronized(inputDirWatcher)
		{
			if (inputDirWatcher == null)
			{
				throw new RuntimeException("Route cannot stop.  Thread is null");
			}
			
			inputDirWatcher.slowKill();
			putThreadPool.shutdownNow();
			inputDirWatcher = null;
		}
    }
    
	public void onA1000Processed(long rowsAdded, long lastReadTime, long lastWriteTime)
	{
		System.out.println(routeNamePrefix + " - <Put Event> Rows Added:" + rowsAdded + ", Read Time last 1000:" + lastReadTime + ", Write Time last 1000" + lastWriteTime);
	}

	public void onStart(long numberOfFiles)
	{
		System.out.println(routeNamePrefix + " - <Put Event> Starting: reading in " + numberOfFiles + " files");
	}

	public void onFinished(long rowsAdded)
	{
		System.out.println(routeNamePrefix + " - <Put Event> Finished: process " + rowsAdded );
	}
}
