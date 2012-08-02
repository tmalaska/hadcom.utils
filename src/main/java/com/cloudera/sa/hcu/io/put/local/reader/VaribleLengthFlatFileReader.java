package com.cloudera.sa.hcu.io.put.local.reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import com.cloudera.sa.hcu.utils.PropertyUtils;

public class VaribleLengthFlatFileReader extends LocalOneOrMoreFileColumnReader
{
	public static final String CONF_ROW_TYPE_START_INDEX = "reader.row.type.start.index";
	public static final String CONF_ROW_TYPE_LENGTH = "reader.row.type.length";

	
	int rowTypeStartIndex = 0;
	int rowTypeLength = 0;
	Map<String, int[]> rowTypeLengthArrayMap;
	String currentRowType;
	
	public VaribleLengthFlatFileReader(String[] filePaths, Properties p) throws Exception
	{
		super(p);
	}
		
	@Override
	protected void init(String[] inputPaths, Properties p) throws IOException
	{
		this.rowTypeLength = PropertyUtils.getIntProperty(p, CONF_ROW_TYPE_LENGTH);
		this.rowTypeLengthArrayMap = null;
		this.rowTypeStartIndex = PropertyUtils.getIntProperty(p, CONF_ROW_TYPE_START_INDEX);
	}


	public String[] parseRow(String line) throws IOException
	{
		currentRowType = line.substring(rowTypeStartIndex, rowTypeStartIndex + rowTypeLength);
		
		int[] lengthArray = rowTypeLengthArrayMap.get(currentRowType);
		
		String[] resultArray = new String[lengthArray.length];
	
		int startingIndex = 0;
		
		for (int i = 0; i < lengthArray.length; i++)
		{
			String columnValue = line.substring(startingIndex, startingIndex + lengthArray[i]);
			resultArray[i] = columnValue;
			
			startingIndex += lengthArray[i];
		}
		return resultArray;
	}
	
	public String getRowType()
	{
		return currentRowType;
	}

	
}
