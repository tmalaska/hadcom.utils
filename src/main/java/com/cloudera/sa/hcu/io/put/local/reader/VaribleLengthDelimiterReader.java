package com.cloudera.sa.hcu.io.put.local.reader;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import com.cloudera.sa.hcu.utils.PropertyUtils;

public class VaribleLengthDelimiterReader extends LocalOneOrMoreFileColumnReader
{
	private static final String CONF_ROW_TYPE_INDEX = "reader.row.type.index";
	private static final String CONF_DELIMITER_REGEX = DELIMITER_REGEX;
	
	int rowTypeIndex = 0;
	String currentRowType;
	
	Pattern pattern;
	BufferedReader reader;
	
	public VaribleLengthDelimiterReader( Properties p) throws Exception
	{
		super(p);
	}

	
	@Override
	protected void init(String[] inputPaths, Properties p) throws IOException
	{
		rowTypeIndex = PropertyUtils.getIntProperty(p, CONF_ROW_TYPE_INDEX);
		pattern = Pattern.compile(PropertyUtils.getStringProperty(p, CONF_DELIMITER_REGEX));
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