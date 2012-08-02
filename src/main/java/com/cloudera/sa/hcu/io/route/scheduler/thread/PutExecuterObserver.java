package com.cloudera.sa.hcu.io.route.scheduler.thread;

import java.io.File;

public interface PutExecuterObserver
{
	public void onPutSuccess(File sourceFile);
	
	public void onPutFailure(File sourceFile);
}
