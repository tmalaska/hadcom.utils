package com.cloudera.sa.hcu.io.get;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class GetAvroFile extends AbstractGetter
{
	public static void main(String[] args) throws Exception
	{
		(new GetAvroFile()).getFile(args);
	}

	public void getFile(String[] args) throws IOException
	{
		if (args.length < 2)
		{
			System.out.println("Get Avro File:");
			System.out.println("");
			System.out.println("Parameter: <hdfs input file path> <local output data file path> <optionally local output schema file path>");
			return;
		}

		String inputLocation = args[0];
		String outputLocation = args[1];
		String schemaOutputLocation = null;
		if (args.length == 3)
		{
			schemaOutputLocation = args[2];
		}

		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);

		Path inputFilePath = new Path(inputLocation);

		FSDataInputStream dataInputStream = hdfs.open(inputFilePath);

		DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
		// writer.setSchema(s); // I guess I don't need this

		DataFileStream<GenericRecord> dataFileReader = new DataFileStream<GenericRecord>(dataInputStream, reader);

		BufferedWriter localDataWriter = new BufferedWriter(new FileWriter(new File(outputLocation)));



		try
		{
			Schema s = dataFileReader.getSchema();

			if (schemaOutputLocation != null)
			{
				BufferedWriter localSchemaWriter = new BufferedWriter(new FileWriter(new File(schemaOutputLocation)));
				try
				{
					localSchemaWriter.write(s.toString());
				} finally
				{
					localSchemaWriter.close();
				}
			}

			while (dataFileReader.hasNext())
			{
				GenericRecord record = dataFileReader.next();

				localDataWriter.write(record.toString());

				onWritenRecord();
			}
		} finally
		{
			localDataWriter.close();
			dataFileReader.close();
			dataInputStream.close();
		}
		onFinishedWriting();
	}

}
