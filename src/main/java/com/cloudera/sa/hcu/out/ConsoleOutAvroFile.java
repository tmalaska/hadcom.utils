package com.cloudera.sa.hcu.out;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ConsoleOutAvroFile {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException 
	{
		String inputFile = args[0];
		
		
		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);
		
		Path inputFilePath = new Path(inputFile);

		FSDataInputStream dataInputStream = hdfs.open(inputFilePath);
		
		DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
		//writer.setSchema(s); // I guess I don't need this
		
		DataFileStream<GenericRecord> dataFileReader = new DataFileStream<GenericRecord>(dataInputStream, reader);
		
		Schema s = dataFileReader.getSchema();
		System.out.println(s.getName() + " " + s);
		
		System.out.println("-");
		
		while(dataFileReader.hasNext())
		{
			GenericRecord record = dataFileReader.next();
			record.toString();
			
			System.out.println("  " + record);
		}
		
		System.out.println("-");
		
		dataFileReader.close();
		dataInputStream.close();
	}

}
