package com.cloudera.sa.hcu.out;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
//import org.apache.hadoop.hive.ql.io.RCFileRecordReader;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.LongWritable;

public class ConsoleOutRcFile {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException 
	{
		if (args.length < 1)
		{
			System.out.println("ConsoleOutRcFile:");
			System.out.println("Parameter: <inputFile> <optional delimiter>");
		}
		String inputFile = args[0];
		
		
		String delimiter = "\t";
		if (args.length > 1)
		{
			delimiter = args[1];
		}
		
		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);
		
		Path inputFilePath = new Path(inputFile);

		
		RCFile.Reader reader = new RCFile.Reader(hdfs, inputFilePath, config);

		LongWritable next = new LongWritable(1);
		
		BytesRefArrayWritable row = new BytesRefArrayWritable();
		
		
		while (reader.next(next))
		{
			reader.getCurrentRow(row);
			
			for (int j = 0; j < row.size(); j++)
			{
				BytesRefWritable byteWritable = row.get(j);
				if (byteWritable.getStart() > 0 && byteWritable.getLength() > 0)
				{
					System.out.print(new String(byteWritable.getData()).substring(byteWritable.getStart(), byteWritable.getStart() + byteWritable.getLength()) + delimiter);
				}
			}
		}
		
		
		reader.close();
	}

}
