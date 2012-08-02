package com.cloudera.sa.hcu.io.put.local.reader;

import java.io.IOException;
import java.util.Properties;

import com.cloudera.sa.hcu.utils.PropertyUtils;

public abstract class AbstractLocalFileColumnReader
{
	protected static final String DELIMITER_REGEX = "reader.delimiter.regex";
	protected static final String FIELD_LENGTH_ARRAY = "reader.field.lengths";
	public static final String CONF_INPUT_PATHS = "reader.inputPaths";
	
	String[] inputPaths;
	
	public AbstractLocalFileColumnReader(Properties prop) throws Exception
	{
		inputPaths = PropertyUtils.getStringProperty(prop, CONF_INPUT_PATHS).split(",");
		init(inputPaths, prop);
	}
	
	abstract protected void init(String[] inputPaths, Properties prop) throws Exception;
	
	abstract public String getCurrentFileName();
	
	abstract public String[] getNextRow() throws IOException;
	
	abstract public String getRowType();
	
	abstract public void close() throws IOException;

	abstract public long getNumberOfFiles();
}
