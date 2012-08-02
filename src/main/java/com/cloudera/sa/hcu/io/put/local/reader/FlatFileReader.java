package com.cloudera.sa.hcu.io.put.local.reader;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Properties;

import com.cloudera.sa.hcu.utils.PropertyUtils;

public class FlatFileReader extends LocalOneOrMoreFileColumnReader
{
	private static final String CONF_FIELD_LENGTH_ARRAY = FIELD_LENGTH_ARRAY;
	
	BufferedReader reader;
	int[] lengthArray;
	int totalLength;
	
	public FlatFileReader(Properties p) throws Exception
	{
		super(p);
	}
	
	@Override
	protected void init(String[] inputPaths, Properties p) throws IOException
	{
		lengthArray = PropertyUtils.getIntArrayProperty(p, CONF_FIELD_LENGTH_ARRAY);
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
		}else
		{
			for (int i = 0; i < resultArray.length; i++)
			{
				resultArray[i] = "";
			}
		}
		return resultArray;	
	}

	

	
}
