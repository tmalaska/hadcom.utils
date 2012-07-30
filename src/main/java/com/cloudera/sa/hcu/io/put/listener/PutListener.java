package com.cloudera.sa.hcu.io.put.listener;

public interface PutListener
{
	public void onStart(long numberOfFiles);
	
	public void onA1000Processed(long rowsAdded, long lastReadTime, long lastWriteTime);
	
	public void onFinished(long rowsAdded);
}
