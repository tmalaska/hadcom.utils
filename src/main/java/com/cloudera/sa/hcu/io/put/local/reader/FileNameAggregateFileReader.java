package com.cloudera.sa.hcu.io.put.local.reader;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

import com.cloudera.sa.hcu.utils.PropertyReaderUtils;

public class FileNameAggregateFileReader extends AbstractLocalFileColumnReader
{
	public static final String CONF_READER = "reader.aggregate.reader";
	
	AbstractLocalFileColumnReader rootReader;
	
	public FileNameAggregateFileReader( Properties p) throws Exception
	{
		super(p);
	}
	
	@Override
	protected void init(String[] inputPaths, Properties p) throws IOException, SecurityException, NoSuchMethodException, ClassNotFoundException, IllegalArgumentException, InstantiationException, IllegalAccessException, InvocationTargetException
	{
		String className = PropertyReaderUtils.getStringProperty(p, CONF_READER);
		
		try
		{
			Constructor<AbstractLocalFileColumnReader> constructor = (Constructor<AbstractLocalFileColumnReader>)Class.forName(className).getConstructor(Properties.class);
		
			rootReader = (AbstractLocalFileColumnReader)constructor.newInstance(p);
		}catch(Exception e)
		{
			throw new RuntimeException(CONF_READER + " value of '" + className + "' was unable to construct into a " + AbstractLocalFileColumnReader.class.getName(), e);
		}
	}
	
	public String[] getNextRow() throws IOException
	{
		String[] initResults = rootReader.getNextRow();
		String[] finalResults = new String[initResults.length + 1];
		
		System.arraycopy(initResults, 0, finalResults, 1, initResults.length);
		
		finalResults[0] = rootReader.getCurrentFileName();
		
		return finalResults;
	}

	public void close() throws IOException
	{
		rootReader.close();
		
	}

	public String getCurrentFileName()
	{
		return rootReader.getCurrentFileName();
	}

	public String getRowType()
	{
		return rootReader.getRowType();
	}

	@Override
	public long getNumberOfFiles()
	{
		return rootReader.getNumberOfFiles();
	}

}
