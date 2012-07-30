package com.cloudera.sa.hcu.io.put.local.reader;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import com.cloudera.sa.hcu.utils.PropertyReaderUtils;

public class VaribleLengthDelimiterReader extends LocalOneOrMoreFileColumnReader
{
	private static final String CONF_ROW_TYPE_INDEX = "reader.row.type.index";
	private static final String CONF_DELIMITER_REGEX = DELIMITER_REGEX;
	
	int rowTypeIndex = 0;
	String currentRowType;
	
	Pattern pattern;
	BufferedReader reader;
	
	public VaribleLengthDelimiterReader(String[] filePaths, Properties p) throws Exception
	{
		super(filePaths, p);
	}
	
	public VaribleLengthDelimiterReader(String filePath, String delimiterRegex, int rowTypeIndex) throws Exception
	{
		this(new String[]{filePath}, delimiterRegex, rowTypeIndex);
	}
	
	public VaribleLengthDelimiterReader(String[] filePaths, String delimiterRegex, int rowTypeIndex) throws Exception
	{
		super(filePaths, makeProperties(delimiterRegex, rowTypeIndex));
	}

	private static Properties makeProperties(String delimiterRegex, int rowTypeIndex)
	{
		Properties p = new Properties();
		p.setProperty(CONF_DELIMITER_REGEX, delimiterRegex);
		p.setProperty(CONF_ROW_TYPE_INDEX, Integer.toString(rowTypeIndex));
		return p;
	}
	
	@Override
	protected void init(String[] inputPaths, Properties p) throws IOException
	{
		rowTypeIndex = PropertyReaderUtils.getIntProperty(p, CONF_ROW_TYPE_INDEX);
		pattern = Pattern.compile(PropertyReaderUtils.getStringProperty(p, CONF_DELIMITER_REGEX));
	}
	
	public String[] parseRow(String row) throws IOException
	{
		String[] resultArray = pattern.split(row);
		
		currentRowType = resultArray[rowTypeIndex];
		
		return resultArray;
	}
	
	public String getRowType()
	{
		return currentRowType;
	}
}