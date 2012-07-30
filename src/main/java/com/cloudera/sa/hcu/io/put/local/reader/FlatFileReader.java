package com.cloudera.sa.hcu.io.put.local.reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import com.cloudera.sa.hcu.utils.PropertyReaderUtils;

public class FlatFileReader extends LocalOneOrMoreFileColumnReader
{
	private static final String CONF_FIELD_LENGTH_ARRAY = FIELD_LENGTH_ARRAY;
	
	BufferedReader reader;
	int[] lengthArray;
	int totalLength;
	
	public FlatFileReader(String[] filePaths, Properties p) throws Exception
	{
		super(filePaths, p);
	}
	
	public FlatFileReader(String filePath, int[] lengthArray) throws Exception
	{
		this(new String[]{filePath}, lengthArray);
	}
	
	public FlatFileReader(String[] filePaths, int[] lengthArray) throws Exception
	{
		super(filePaths, makeProperties(lengthArray));
	}

	private static Properties makeProperties(int[] lengthArray)
	{
		Properties p = new Properties();
		p.setProperty(CONF_FIELD_LENGTH_ARRAY, PropertyReaderUtils.convertIntArrayToString(lengthArray));
		return p;
	}
	
	@Override
	protected void init(String[] inputPaths, Properties p) throws IOException
	{
		lengthArray = PropertyReaderUtils.getIntArrayProperty(p, CONF_FIELD_LENGTH_ARRAY);
		for (int i: lengthArray)
		{
			totalLength += i;
		}
	}
	
	public String[] parseRow(String line) throws IOException
	{
		String[] resultArray = new String[lengthArray.length];
		if (line.length() > totalLength)
		{
			int startingIndex = 0;
			
			for (int i = 0; i < lengthArray.length; i++)
			{
				String columnValue = line.substring(startingIndex, startingIndex + lengthArray[i]);
				resultArray[i] = columnValue;
				
				startingIndex += lengthArray[i];
			}
		}
		return resultArray;	
	}

	

	
}
