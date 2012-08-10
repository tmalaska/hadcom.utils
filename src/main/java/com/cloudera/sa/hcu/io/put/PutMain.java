package com.cloudera.sa.hcu.io.put;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import com.cloudera.sa.hcu.io.put.hdfs.writer.AbstractWriter;
import com.cloudera.sa.hcu.io.put.listener.HeartBeatConsoleOutputListener;
import com.cloudera.sa.hcu.io.put.listener.PutListener;
import com.cloudera.sa.hcu.io.put.local.reader.AbstractLocalFileColumnReader;
import com.cloudera.sa.hcu.utils.PropertyUtils;


public class PutMain
{
	
	public static void main(String[] args) throws Exception
	{
		if (args.length < 3)
		{
			System.out.println("Put Help:");
			System.out.println("Parameters: <inputFilePath(s)> <outputPath> <propertyFilePath>");
			System.out.println();
			System.out.println("If you use more then one inputFilePath, then just sperate by ','");
			System.out.println();
			System.out.println("If you are putting into HBase, then put \"HBase\" in the \"outputPath\" spot");
			return;
		}
		
		String inputFilePaths = args[0];
		String rootOutputDir = args[1];
		Properties p = new Properties();
		p.load(new FileInputStream(new File(args[2])));
		
		Putter put = new Putter();
		
		PutListener listener = new HeartBeatConsoleOutputListener(5);
		put.addListener(listener);
		
		p.put(AbstractLocalFileColumnReader.CONF_INPUT_PATHS, inputFilePaths);
		p.put(AbstractWriter.CONF_OUTPUT_PATH, rootOutputDir);
		
		put.put(p);
	}
}
