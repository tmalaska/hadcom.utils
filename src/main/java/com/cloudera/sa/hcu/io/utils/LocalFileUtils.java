package com.cloudera.sa.hcu.io.utils;

import java.io.File;
import java.util.ArrayList;

public class LocalFileUtils
{


	public static File[] createFileArray(String[] filePathArray)
	{
		ArrayList<File> fileList = new ArrayList<File>();
		File[] tempFileArray;
		
		for (String filePath: filePathArray)
		{
			File origFile = new File(filePath);
			
			if (origFile.isDirectory())
			{
				tempFileArray = origFile.listFiles();
				
				for (File file: tempFileArray)
				{
					fileList.add(file);
				}
			}else
			{
				fileList.add(origFile);
			}
		}
		
		return fileList.toArray(new File[0]);
	}
	
	public static String[] createStringArrayOfFiles(String[] filePathArray)
	{
		ArrayList<String> filePathList = new ArrayList<String>();
		File[] tempFileArray;
		
		for (String filePath: filePathArray)
		{
			File origFile = new File(filePath);
			
			if (origFile.isDirectory())
			{
				tempFileArray = origFile.listFiles();
				
				for (File file: tempFileArray)
				{
					filePathList.add(file.getAbsolutePath());
				}
			}else
			{
				filePathList.add(origFile.getAbsolutePath());
			}
		}
		
		return filePathList.toArray(new String[0]);
	}


}
