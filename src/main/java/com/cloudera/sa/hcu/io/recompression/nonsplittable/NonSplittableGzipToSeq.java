package com.cloudera.sa.hcu.io.recompression.nonsplittable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.EnumSet;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;

public class NonSplittableGzipToSeq {

	public static void main(String[] args) throws IOException {
		
		if (args.length < 3)
		{
			System.out.println("NonSplittableGzipToSeq Help:");
			System.out.println("Parameters: <inputFilePath(s)> <outputPath> <compressionCodec>");
			System.out.println();
			return;
		}
		
		String inputLocation = args[0];
		String outputLocation = args[1];
		String compressionCodec = args[2];
		
		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);

		Path inputFilePath = new Path(inputLocation);		
		Path outputFilePath = new Path(outputLocation);
		
		BufferedReader reader = getGzipReader(hdfs, inputFilePath);
		try
		{
			SequenceFile.Writer writer = getSequenceFileWriter(config, hdfs, outputFilePath, compressionCodec);
		
			try
			{
				Text value = new Text();
				String currentLine = null;
				long counter = 0;
				
				while((currentLine = reader.readLine()) != null)
				{
					value.set(currentLine);
					writer.append(NullWritable.get(), value);
					counter++;
					if (counter % 10000 == 0)
					{
						System.out.println("Processed " + counter + " lines.");
					}
				}
				
				System.out.println("Finished: Processed " + counter + " lines.");
			} finally {
				if (writer != null) {
					writer.close();
					writer = null;
				}
			}
			
		}finally
		{
			if (reader != null) {
				reader.close();
				reader = null;
			}
		}
		
	}
	
	public static BufferedReader getGzipReader(FileSystem hdfs, Path path) throws IOException {
		FSDataInputStream inputStream = hdfs.open(path);
		
		if (path.getName().endsWith("gz") || path.getName().endsWith("gzip")) {
			GZIPInputStream gzip = new GZIPInputStream(inputStream);
			
			System.out.println("processing gzip file");
			
			return new BufferedReader(new InputStreamReader(gzip));	
		} else {
			throw new IOException("UnKnown compress type.  Can only process files with ext of (gzip, gz)");
		}
	}

	public static SequenceFile.Writer getSequenceFileWriter(Configuration config, FileSystem hdfs, Path path, String compressionCodecStr) throws IOException {
		//Created our writer
		SequenceFile.Metadata metaData = new SequenceFile.Metadata();
	
		EnumSet<CreateFlag> enumSet = EnumSet.of(CreateFlag.CREATE);
		return SequenceFile.createWriter( FileContext.getFileContext(), config, path, NullWritable.class, Text.class, SequenceFile.CompressionType.BLOCK, getCompressionCodec(compressionCodecStr), metaData, enumSet);
	}
	
	public static CompressionCodec getCompressionCodec(String value)
	{
		
		if (value.equals("snappy"))
		{
			return new SnappyCodec();
		}else if (value.equals("gzip"))
		{
			return new GzipCodec();
		}else if (value.equals("bzip2"))
		{
			return new BZip2Codec();
		}else
		{
			return new SnappyCodec();
		}
	}
}
