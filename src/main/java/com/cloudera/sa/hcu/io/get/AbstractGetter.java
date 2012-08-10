package com.cloudera.sa.hcu.io.get;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.zip.GZIPOutputStream;

public abstract class AbstractGetter
{
	long recordsWriten = 0;
	long lastTime = System.currentTimeMillis();
	
	abstract public void getFile(String[] args) throws Exception;
	
	public BufferedWriter createBufferedWriter(String filePath) throws IOException
	{
		if (filePath.endsWith(".zip") || filePath.endsWith(".gz"))
		{
			return new BufferedWriter(new OutputStreamWriter( new GZIPOutputStream(new FileOutputStream(filePath))));
		}else
		{
			return new BufferedWriter(new FileWriter(new File(filePath)));
		}
	}
	
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
