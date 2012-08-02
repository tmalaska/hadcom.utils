package com.cloudera.sa.hcu.utils;

import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;

import com.cloudera.sa.hcu.io.put.local.reader.AbstractLocalFileColumnReader;


public class PropertyUtils
{

	public static String getStringProperty(Properties p, String key)
	{
		String value = p.getProperty(key);
		if (value == null)
		{
			throw new RuntimeException("Property:" + key + " is required.");
		}
		return value;
	}
	
	public static int getIntProperty(Properties p, String key)
	{
		try
		{
			return Integer.parseInt(getStringProperty(p, key));
		}catch(NumberFormatException e)
		{
			throw new RuntimeException("Property:" + key + " is most be an int.  Value '" + getStringProperty(p, key) + "' is not valid", e);
		}
	}
	
	
	public static CompressionCodec getCompressionCodecProperty(Properties p , String key)
	{
		String value = getStringProperty(p, key).toLowerCase();
		
		if (value.equals("snappy"))
		{
			return new SnappyCodec();
		}else if (value.equals("gzip"))
		{
			return new GzipCodec();
		}else if (value.equals("bzip2"))
		{
			return new BZip2Codec();
		}else
		{
			return new SnappyCodec();
		}
	}

	private static Pattern pattern = Pattern.compile(",");
	
	public static String convertIntArrayToString(int[] intArray)
	{
		StringBuilder strBuilder = new StringBuilder();
		for (int val: intArray)
		{
			strBuilder.append(val + ",");
		}
		strBuilder.delete(strBuilder.length() - 1, strBuilder.length());
		
		return strBuilder.toString();
	}
	
	public static String convertStringArrayToString(String[] strArray)
	{
		StringBuilder strBuilder = new StringBuilder();
		for (String val: strArray)
		{
			strBuilder.append(val + ",");
		}
		strBuilder.delete(strBuilder.length() - 1, strBuilder.length());
		
		return strBuilder.toString();
	}
	
	
	
	public static int[] getIntArrayProperty(Properties p, String key)
	{
		String[] fieldLengthStrings = pattern.split(getStringProperty(p,key));
		int[] results = new int[fieldLengthStrings.length];
		
		for (int i = 0; i < fieldLengthStrings.length; i++)
		{
			results[i] = Integer.parseInt(fieldLengthStrings[i]);
		}
		
		return results;
	}

}
