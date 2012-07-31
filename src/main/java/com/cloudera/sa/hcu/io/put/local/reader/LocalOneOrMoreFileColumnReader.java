package com.cloudera.sa.hcu.io.put.local.reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;
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
	
	protected void loadFile(String[] filePathArray) throws IOException
	{
		fileArray = LocalFileUtils.createFileArray(filePathArray);
		
		reader = getReaderForFileType(fileArray[fileIndex]);
		
	}
	
	/**
	 * If the file is a gzip file then use the GZipInputStream.  Otherwise use the normal inputStream.
	 * @param inputFile
	 * @return
	 * @throws IOException
	 */
	private BufferedReader getReaderForFileType(File inputFile) throws IOException
	{
		String fileName = inputFile.getName().toLowerCase();
		if (fileName.endsWith(".gz") || fileName.endsWith(".zip"))
		{
			GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(inputFile));
			
			return new BufferedReader(new InputStreamReader(gzip));
			
		}else
		{
			return new BufferedReader( new FileReader(inputFile));
		}
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
				reader = getReaderForFileType(fileArray[fileIndex]);
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
