package com.cloudera.sa.hcu.io.put.local.reader;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.regex.Pattern;

import com.cloudera.sa.hcu.utils.PropertyReaderUtils;

public class DelimiterReader extends LocalOneOrMoreFileColumnReader
{
	Pattern pattern;
	BufferedReader reader;
	
	public static final String CONF_DELIMITER_REGEX = DELIMITER_REGEX;
	
	public DelimiterReader(String[] filePaths, Properties p) throws Exception
	{
		super(filePaths, p);
	}
	
	public DelimiterReader(String filePath, String delimiterRegex) throws Exception
	{
		this(new String[]{filePath}, delimiterRegex);
	}
	
	public DelimiterReader(String[] filePaths, String delimiterRegex) throws Exception
	{
		super(filePaths, makeProperties(delimiterRegex));
	}

	private static Properties makeProperties(String delimiterRegex)
	{
		Properties p = new Properties();
		p.setProperty(CONF_DELIMITER_REGEX, delimiterRegex);
		return p;
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
