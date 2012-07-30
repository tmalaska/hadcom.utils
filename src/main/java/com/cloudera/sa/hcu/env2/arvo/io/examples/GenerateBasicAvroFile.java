package com.cloudera.sa.hcu.env2.arvo.io.examples;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class GenerateBasicAvroFile {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException 
	{
		String outputFile = args[0];
		
		String schemaDescription = " {    \n"
                + " \"name\": \"FacebookUser\", \n"
                + " \"type\": \"record\",\n" + " \"fields\": [\n"
                + "   {\"name\": \"name\", \"type\": \"string\"},\n"
                + "   {\"name\": \"num_likes\", \"type\": \"int\"},\n"
                + "   {\"name\": \"num_photos\", \"type\": \"int\"},\n"
                + "   {\"name\": \"num_groups\", \"type\": \"int\"} ]\n" + "}";
		
		Schema s = Schema.parse(schemaDescription);
		 
		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);
		
		Path outputFilePath = new Path(outputFile);

		FSDataOutputStream dataOutputStream = hdfs.create(outputFilePath);
		
		DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>();
		//writer.setSchema(s); // I guess I don't need this
		
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(writer);
		
		dataFileWriter.create(s, dataOutputStream);
		
		GenericRecord datum = new GenericData.Record(s);
		datum.put("name", new Utf8("ted"));
		datum.put("num_likes", 1);
		datum.put("num_groups", 423);
		datum.put("num_photos", 0);
		
		dataFileWriter.append(datum);
		
		datum.put("name", new Utf8("karen"));
		datum.put("num_likes", 2);
		datum.put("num_groups", 123);
		datum.put("num_photos", 1);
		
		dataFileWriter.append(datum);
		
		dataFileWriter.close();
		dataOutputStream.close();
		
	}

}
