package com.cloudera.sa.hcu.io.put.local.reader;

import java.io.IOException;
import java.util.Properties;

import com.cloudera.sa.hcu.io.utils.LocalFileUtils;

import au.com.bytecode.opencsv.CSVReader;

public class CsvReader extends LocalOneOrMoreFileColumnReader
{

	CSVReader csvReader;
	
	public CsvReader(Properties prop) throws Exception
	{
		super(prop);
	}
	
	protected void loadFile(String[] filePathArray) throws IOException
	{
		fileArray = LocalFileUtils.createFileArray(filePathArray);
		
		csvReader = new CSVReader(getReaderForFileType(fileArray[fileIndex]));
		
	}
	
	public String[] getNextRow() throws IOException
	{
		String[] line = csvReader.readNext();
		
		if (line == null)
		{
			fileIndex++;
			if (fileIndex < fileArray.length)
			{
				csvReader.close();
				csvReader = new CSVReader(getReaderForFileType(fileArray[fileIndex]));
				return getNextRow();
			}
			return null;
		}
		
		return line;
		
	}
	
	public void close() throws IOException
	{
		csvReader.close();
	}
	
	public String[] parseRow(String row) throws IOException
	{
		//do nothing	
		return null;
	}


	
	@Override
	protected void init(String[] inputPaths, Properties prop) throws Exception
	{
		//Do nothing
		
	}

}
