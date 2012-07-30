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

public class Putter
{
	ArrayList<PutListener> listeners = new ArrayList<PutListener>();
	
	public void addListener(PutListener listener)
	{
		listeners.add(listener);
	}
	
	private void notifyListenersOfA1000Rows(long rowsAdded, long lastReadTime, long lastWriteTime )
	{
		for (PutListener pl: listeners)
		{
			pl.onA1000Processed(rowsAdded, lastReadTime, lastWriteTime);
		}
	}
	
	private void notifyListenersOfStart(long numberOfFiles)
	{
		for (PutListener pl: listeners)
		{
			pl.onStart(numberOfFiles);
		}
	}
	
	private void notifyListenersOfFinished(long rowsAdded)
	{
		for (PutListener pl: listeners)
		{
			pl.onFinished(rowsAdded);
		}
	}
	
	public void put(String[] inputFiles, String rootOutputDir, Properties properties) throws IOException
	{
		AbstractLocalFileColumnReader reader = ReaderFactory.initReader(inputFiles, properties);
		AbstractHdfsWriter writer = WriterFactory.initWriter(rootOutputDir, properties);
		
		String[] columns;
		long rowsAddedCounter = 0;
		long readTimeCounter = 0;
		long writeTimeCounter = 0;
		
		
		long writeStartTime = 0;
		
		notifyListenersOfStart(reader.getNumberOfFiles());
		
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
				notifyListenersOfA1000Rows(rowsAddedCounter, readTimeCounter, writeTimeCounter);
				readTimeCounter = 0;
				writeTimeCounter = 0;
			}
			
			readStartTime = System.currentTimeMillis();
		}
		writer.close();
		reader.close();
		
		notifyListenersOfFinished(rowsAddedCounter);
	}
	
	public void put(String[] inputFilePaths, String rootOutputDir, Properties properties, int threads) throws IOException
	{
		if (threads == 1)
		{
			put( inputFilePaths, rootOutputDir, properties);
		}else
		{
			inputFilePaths = LocalFileUtils.createStringArrayOfFiles(inputFilePaths);
			
			ArrayList<ArrayList<String>> seperatedFiles = new ArrayList<ArrayList<String>>();
			
			for (int i = 0; i < threads; i++)
			{
				seperatedFiles.add(new ArrayList<String>());
			}
			
			for (int i = 0; i < inputFilePaths.length; i++)
			{
				int goToThreadIdx = Math.abs(inputFilePaths[i].hashCode() % threads);
				
				seperatedFiles.get(goToThreadIdx).add(inputFilePaths[i]);
			}
			
			//Open hdfs file system
			Configuration config = new Configuration();
			FileSystem hdfs = FileSystem.get(config);
			
			//Create root folder
			Path outputFilePath = new Path(rootOutputDir);
			hdfs.mkdirs(outputFilePath);
			
			hdfs.close();
			
			for (int i = 0; i < threads; i++)
			{
				PutThread putThread = new PutThread(seperatedFiles.get(i).toArray(new String[0]), rootOutputDir, i, properties);
				
				putThread.start();
			}
		}
		
		
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
				(new Putter()).put(inputFilePaths, rootOutputDir + "/part-i-" + threadNum, properties);
			} catch (IOException e)
			{
				e.printStackTrace();
			}
		}
	}
}
