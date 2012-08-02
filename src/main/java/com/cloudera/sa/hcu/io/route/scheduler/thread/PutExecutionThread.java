package com.cloudera.sa.hcu.io.route.scheduler.thread;

import java.io.File;
import java.util.Properties;
import java.util.Map.Entry;

import com.cloudera.sa.hcu.io.put.Putter;
import com.cloudera.sa.hcu.io.put.hdfs.writer.AbstractHdfsWriter;
import com.cloudera.sa.hcu.io.put.listener.PutListener;
import com.cloudera.sa.hcu.io.put.local.reader.AbstractLocalFileColumnReader;



public class PutExecutionThread implements Runnable
{
	Properties p;
	PutListener listener;
	File sourceFile;
	PutExecuterObserver observer;

	public PutExecutionThread(File sourceFile, Properties p, PutListener listener, PutExecuterObserver observer) 
	{
		Properties pClone = (Properties)p.clone();
		pClone.setProperty(AbstractLocalFileColumnReader.CONF_INPUT_PATHS, sourceFile.getAbsolutePath());
		pClone.setProperty(AbstractHdfsWriter.CONF_OUTPUT_PATH, p.getProperty(AbstractHdfsWriter.CONF_OUTPUT_PATH) + "/" + sourceFile.getName());
		
		this.sourceFile = sourceFile;
		
		this.p = pClone;
		this.listener = listener;
		this.observer = observer;
		
	}
	
	public void run()
	{
		try
		{
			Putter put = new Putter();
			
			put.addListener(listener);
			
			put.put(p);
			
			observer.onPutSuccess(sourceFile);
		}catch(Exception e)
		{
			for (Entry<Object, Object> e1: p.entrySet())
			{
				System.out.println(e1.getKey() + "-" + e1.getValue());
			}
			
			e.printStackTrace();
			observer.onPutFailure(sourceFile);
			
			throw new RuntimeException(e);
		}
	}


}
