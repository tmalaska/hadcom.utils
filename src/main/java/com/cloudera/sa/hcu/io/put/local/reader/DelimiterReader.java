package com.cloudera.sa.hcu.io.put.local.reader;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Properties;
import java.util.regex.Pattern;

import com.cloudera.sa.hcu.utils.PropertyReaderUtils;

public class DelimiterReader extends LocalOneOrMoreFileColumnReader
{
	Pattern pattern;
	BufferedReader reader;
	
	public static final String CONF_DELIMITER_REGEX = DELIMITER_REGEX;
	
	public DelimiterReader( Properties p) throws Exception
	{
		super( p);
	}
	
	
	@Override
	protected void init(String[] inputPaths, Properties p) throws IOException
	{
		pattern = Pattern.compile(PropertyReaderUtils.getStringProperty(p, CONF_DELIMITER_REGEX));
	}
	
	public String[] parseRow(String row) throws IOException
	{
		return pattern.split(row);	
	}
}
