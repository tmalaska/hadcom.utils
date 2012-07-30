package com.cloudera.sa.hcu.io.put.local.reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import com.cloudera.sa.hcu.io.utils.LocalFileUtils;;

public abstract class LocalOneOrMoreFileColumnReader extends AbstractLocalFileColumnReader
{
	public LocalOneOrMoreFileColumnReader(String[] inputPaths, Properties prop) throws Exception
	{
		super(inputPaths, prop);
		loadFile(inputPaths);
	}

	protected BufferedReader reader;
	protected File[] fileArray;
	protected int fileIndex = 0;
	
	protected void loadFile(String[] filePathArray) throws FileNotFoundException
	{
		fileArray = LocalFileUtils.createFileArray(filePathArray);
		
		reader = new BufferedReader( new FileReader(fileArray[fileIndex]));
		
	}
	
	public long getNumberOfFiles()
	{
		return fileArray.length;
	}
	
	public String getCurrentFileName()
	{
		return fileArray[fileIndex].getName();
	}
	
	public String[] getNextRow() throws IOException
	{
		String line = reader.readLine();
		
		if (line == null)
		{
			fileIndex++;
			if (fileIndex < fileArray.length)
			{
				reader.close();
				reader = new BufferedReader( new FileReader(fileArray[fileIndex]));
				return getNextRow();
			}
			return null;
		}
		
		return parseRow(line);
		
	}
	
	protected abstract String[] parseRow(String row) throws IOException;
	
	public String getRowType()
	{
		return null;
	}
	
	public void close() throws IOException
	{
		reader.close();
	}
}
