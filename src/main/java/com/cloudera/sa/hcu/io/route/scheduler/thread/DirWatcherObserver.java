package com.cloudera.sa.hcu.io.route.scheduler.thread;

import java.io.File;

public interface DirWatcherObserver
{
	
	public void onDirWatcherFoundFiles(File[] files) throws Exception;
	
	public void onDirWatcherFiredExceptin( Exception e);

	public void onDirWatcherSecondsLeftReport(double secondsLeft );
}
