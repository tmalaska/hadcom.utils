package com.cloudera.sa.hcu.io.route.scheduler;

import java.io.IOException;
import java.util.Properties;

public interface IRouteWorker
{
	
	abstract public void start() throws Exception;
	
	abstract public void hardStop() throws Exception;
	
	abstract public void softStop() throws Exception;
}
