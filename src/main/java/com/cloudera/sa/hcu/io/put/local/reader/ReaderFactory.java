package com.cloudera.sa.hcu.io.put.local.reader;

import java.lang.reflect.Constructor;
import java.util.Properties;

public class ReaderFactory
{
	public static final String CONF_READER_CLASS = "put.reader";
	
	public static AbstractLocalFileColumnReader initReader(Properties p)
	{
		String readerClass = p.getProperty(CONF_READER_CLASS);
		
		try
		{
			Constructor constructor = Class.forName(readerClass).getConstructor(Properties.class);
		
			return (AbstractLocalFileColumnReader)constructor.newInstance(p);
		}catch(Exception e)
		{
			throw new RuntimeException(CONF_READER_CLASS + " value of '" + readerClass + "' was unable to construct into a " + AbstractLocalFileColumnReader.class.getName(), e);
		}
		
	}


}
