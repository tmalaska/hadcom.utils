package com.cloudera.sa.hcu.io.put;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.cloudera.sa.hcu.io.put.hdfs.writer.AbstractHdfsWriter;
import com.cloudera.sa.hcu.io.put.hdfs.writer.WriterFactory;
import com.cloudera.sa.hcu.io.put.listener.PutListener;
import com.cloudera.sa.hcu.io.put.local.reader.AbstractLocalFileColumnReader;
import com.cloudera.sa.hcu.io.put.local.reader.ReaderFactory;
import com.cloudera.sa.hcu.io.utils.LocalFileUtils;
import com.cloudera.sa.hcu.utils.PropertyUtils;

public class Putter
{

	public static final String CONF_NUM_THREAD = "batch.files.thread.split";
	
	ArrayList<PutListener> listeners = new ArrayList<PutListener>();
	
	public void addListener(PutListener listener)
	{
		listeners.add(listener);
	}
	
	private void notifyWriten1000Rows(long rowsAdded, long lastReadTime, long lastWriteTime )
	{
		for (PutListener pl: listeners)
		{
			pl.onA1000Processed(rowsAdded, lastReadTime, lastWriteTime);
		}
	}
	
	private void notifyOfStart(long numberOfFiles)
	{
		for (PutListener pl: listeners)
		{
			pl.onStart(numberOfFiles);
		}
	}
	
	private void notifyOfFinished(long rowsAdded)
	{
		for (PutListener pl: listeners)
		{
			pl.onFinished(rowsAdded);
		}
	}
	
	public void put(Properties p) throws IOException
	{
		String numOfThreadsStr = p.getProperty(CONF_NUM_THREAD);
		int numOfThreads = 1;
		if (numOfThreadsStr != null)
		{
			try
			{
				numOfThreads = Integer.parseInt(numOfThreadsStr);
			}catch(NumberFormatException e)
			{
				System.out.println("Value for '" + CONF_NUM_THREAD + "' is not a valid number. Value was '" + numOfThreadsStr + "'.");
				return;
			}
		}
		
		put(p, numOfThreads);
	}
	
	public void put( Properties properties, int threads) throws IOException
	{
		System.out.println("Put threads set to " + threads );
		
		
		if (threads == 1)
		{
			putSingleThread(properties);
		}else
		{
			putMultiThread(properties, threads);
		}
		
		
	}

	private void putMultiThread(Properties properties, int threads) throws IOException
	{
		String[] inputFilePaths = PropertyUtils.getStringProperty(properties ,AbstractLocalFileColumnReader.CONF_INPUT_PATHS).split(",");
		String rootOutputDir = PropertyUtils.getStringProperty(properties, AbstractHdfsWriter.CONF_OUTPUT_PATH);
		
		inputFilePaths = LocalFileUtils.createStringArrayOfFiles(inputFilePaths);
		
		ArrayList<ArrayList<String>> seperatedFiles = new ArrayList<ArrayList<String>>();
		
		for (int i = 0; i < threads; i++)
		{
			seperatedFiles.add(new ArrayList<String>());
		}
		
		for (int i = 0; i < inputFilePaths.length; i++)
		{
			int goToThreadIdx = Math.abs(inputFilePaths[i].hashCode() % threads);
			
			System.out.println(inputFilePaths[i] + " is going to be processed by thread " + goToThreadIdx);
			
			seperatedFiles.get(goToThreadIdx).add(inputFilePaths[i]);
		}
		
		//Open hdfs file system
		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);
		
		//Create root folder
		Path outputFilePath = new Path(rootOutputDir);
		
		System.out.println("Checking if directory exist: " + outputFilePath + " " + hdfs.exists(outputFilePath) + " " + hdfs.isDirectory(outputFilePath));
		if (hdfs.exists(outputFilePath) == false)
		{
			System.out.println("Tring to make outputDirectory: " + outputFilePath);
			hdfs.mkdirs(outputFilePath);
		}
		
		hdfs.close();
		
		ArrayList<PutThread> putThreadList = new ArrayList<PutThread>();
		
		for (int i = 0; i < threads; i++)
		{
			PutThread putThread = new PutThread(seperatedFiles.get(i).toArray(new String[0]), rootOutputDir, i, properties);
			
			putThread.start();
			putThreadList.add(putThread);
		}
		
		System.out.println("Threads Started: " + outputFilePath);
		
		
		synchronized(putThreadList)
		{
			boolean canExit = false;
			while(canExit == false)
			{
				canExit = true;
				for (PutThread t: putThreadList)
				{
					if (t.isAlive())
					{
						canExit = false;
						break;
					}
				}
				try
				{
					putThreadList.wait(1000);
				} catch (InterruptedException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	private void putSingleThread(Properties properties) throws IOException
	{
		AbstractLocalFileColumnReader reader = ReaderFactory.initReader(properties);
		AbstractHdfsWriter writer = WriterFactory.initWriter(properties);
		
		String[] columns;
		long rowsAddedCounter = 0;
		long readTimeCounter = 0;
		long writeTimeCounter = 0;
		
		
		long writeStartTime = 0;
		
		notifyOfStart(reader.getNumberOfFiles());
		
		long readStartTime = System.currentTimeMillis();
		
		while((columns = reader.getNextRow()) != null)
		{
			readTimeCounter += System.currentTimeMillis() - readStartTime;
			
			writeStartTime = System.currentTimeMillis();	
			writer.writeRow(reader.getRowType(), columns);
			writeTimeCounter += System.currentTimeMillis() - writeStartTime;
			
			rowsAddedCounter++;
			if (rowsAddedCounter % 1000 == 0)
			{
				notifyWriten1000Rows(rowsAddedCounter, readTimeCounter, writeTimeCounter);
				readTimeCounter = 0;
				writeTimeCounter = 0;
			}
			
			readStartTime = System.currentTimeMillis();
		}
		writer.close();
		reader.close();
		
		notifyOfFinished(rowsAddedCounter);
	}
	
	private class PutThread extends Thread
	{
		String[] inputFilePaths; 
		String rootOutputDir; 
		Properties properties;
		int threadNum;
		
		PutThread(String[] inputFilePaths, String rootOutputDir, int threadNum, Properties properties)
		{
			this.inputFilePaths = inputFilePaths;
			this.rootOutputDir = rootOutputDir;
			this.properties = properties;
			this.threadNum = threadNum;
		}
		
		public void run()
		{
			try
			{
				Properties putProperties = (Properties)properties.clone();
				putProperties.setProperty(AbstractLocalFileColumnReader.CONF_INPUT_PATHS,PropertyUtils.convertStringArrayToString(inputFilePaths));
				putProperties.setProperty(AbstractHdfsWriter.CONF_OUTPUT_PATH, putProperties.getProperty(AbstractHdfsWriter.CONF_OUTPUT_PATH) + "/part-i-" + threadNum);
				
				(new Putter()).put( putProperties, 1);
			} catch (IOException e)
			{
				e.printStackTrace();
			}
		}
	}
}
