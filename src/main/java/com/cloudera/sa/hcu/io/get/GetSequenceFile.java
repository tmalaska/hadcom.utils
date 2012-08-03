package com.cloudera.sa.hcu.io.get;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;

public class GetSequenceFile  extends AbstractGetter
{
	public static void main(String[] args) throws Exception
	{
		(new GetSequenceFile()).getFile(args);
		
	}

	@Override
	public void getFile(String[] args) throws Exception
	{
		if (args.length < 2)
		{
			System.out.println("Get Sequence File:");
			System.out.println();
			System.out.println("Parameter: <hdfs input file path> <local output data file path>");
			
		}
		String inputLocation = args[0];
		String outputLocation = args[1];
		

		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);

		Path inputFilePath = new Path(inputLocation);
		
		SequenceFile.Reader.file(inputFilePath);
		
		SequenceFile.Reader reader = new SequenceFile.Reader(config, SequenceFile.Reader.file(inputFilePath));

		BufferedWriter localDataWriter = new BufferedWriter(new FileWriter(new File(outputLocation)));

		Writable key = createWriter(reader.getKeyClass());
		Writable val = createWriter(reader.getValueClass());
		
		try
		{
			LongWritable next = new LongWritable(1);
	
			BytesRefArrayWritable row = new BytesRefArrayWritable();
	
			while (reader.next(key, val))
			{
				localDataWriter.write(key + "\t" + val);
				localDataWriter.newLine();
				this.onWritenRecord();
			}
		}finally
		{
			localDataWriter.close();
			reader.close();	
		}
		this.onFinishedWriting();
	}

	private Writable createWriter(Class cls) throws InstantiationException, IllegalAccessException
	{
		if (cls.equals(NullWritable.class) == true)
		{
			return NullWritable.get();
		}else
		{
			return (Writable)cls.newInstance();
		}
	}
}
