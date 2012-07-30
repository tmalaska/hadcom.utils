package com.cloudera.sa.hcu.io.put;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;


public class PutMain
{
	public static final String CONF_NUM_THREAD = "";
	public static void main(String[] args) throws Exception
	{
		if (args.length < 3)
		{
			System.out.println("Put Help:");
			System.out.println("Parameters: <inputFilePath(s)> <outputPath> <propertyFilePath>");
			System.out.println();
			System.out.println("If you use more then one inputFilePath, then just sperate by ','");
			return;
		}
		
		String inputFilePaths = args[0];
		String rootOutputDir = args[1];
		Properties p = new Properties();
		p.load(new FileInputStream(new File(args[2])));
		
		String numOfThreadsStr = p.getProperty(CONF_NUM_THREAD);
		int numOfThreads = 1;
		if (numOfThreadsStr != null)
		{
			try
			{
				numOfThreads = Integer.parseInt(numOfThreadsStr);
			}catch(NumberFormatException e)
			{
				System.out.println("Value for '" + CONF_NUM_THREAD + "' is not a valid number. Value was '" + numOfThreadsStr + "'.");
				return;
			}
		}
		String[] inputFilePathsArray = inputFilePaths.split(",");
		
		Putter put = new Putter();
		
		put.put(inputFilePathsArray, rootOutputDir, p, numOfThreads);
	}
}
