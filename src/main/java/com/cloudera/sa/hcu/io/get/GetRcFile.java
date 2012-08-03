package com.cloudera.sa.hcu.io.get;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.LongWritable;

public class GetRcFile extends AbstractGetter
{
	public static void main(String[] args) throws Exception
	{
		(new GetRcFile()).getFile(args);
		
	}

	@Override
	public void getFile(String[] args) throws Exception
	{
		if (args.length < 2)
		{
			System.out.println("Get RC File:");
			System.out.println();
			System.out.println("Parameter: <hdfs input file path> <local output data file path> <optionally define a delimiter>");
			System.out.println();
			System.out.println("Note: default delimiter is a '|'.");
		}
		String inputLocation = args[0];
		String outputLocation = args[1];
		
		String delimiter = "|";
		if (args.length > 2)
		{
			delimiter = args[2];
		}

		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);

		Path inputFilePath = new Path(inputLocation);

		RCFile.Reader reader = new RCFile.Reader(hdfs, inputFilePath, config);

		BufferedWriter localDataWriter = new BufferedWriter(new FileWriter(new File(outputLocation)));

		try
		{
			LongWritable next = new LongWritable(1);
	
			BytesRefArrayWritable row = new BytesRefArrayWritable();
	
			while (reader.next(next))
			{
				reader.getCurrentRow(row);
	
				for (int j = 0; j < row.size(); j++)
				{
					BytesRefWritable byteWritable = row.get(j);
					if (byteWritable.getStart() >= 0 && byteWritable.getLength() > 0)
					{
						localDataWriter.write(new String(byteWritable.getData()).substring(byteWritable.getStart(), byteWritable.getStart() + byteWritable.getLength()));
	
					}
					if (j < row.size() - 1)
					{
						localDataWriter.write(delimiter);
					}
				}
	
				localDataWriter.newLine();
			}
		}finally
		{
			localDataWriter.close();
			reader.close();	
		}
		
	}
}
