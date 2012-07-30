package com.cloudera.sa.hcu.io.put.local.reader;

import java.io.IOException;
import java.util.Properties;

public abstract class AbstractLocalFileColumnReader
{
	protected static final String DELIMITER_REGEX = "reader.delimiter.regex";
	protected static final String FIELD_LENGTH_ARRAY = "reader.field.lengths";
	
	public AbstractLocalFileColumnReader(String[] inputPaths, Properties prop) throws Exception
	{
		init(inputPaths, prop);
	}
	
	abstract protected void init(String[] inputPaths, Properties prop) throws Exception;
	
	abstract public String getCurrentFileName();
	
	abstract public String[] getNextRow() throws IOException;
	
	abstract public String getRowType();
	
	abstract public void close() throws IOException;

	abstract public long getNumberOfFiles();
}
