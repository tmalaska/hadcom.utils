package com.cloudera.sa.hcu.io.put.hdfs.writer;

import java.io.IOException;
import java.util.Properties;

public abstract class AbstractHdfsWriter
{	
	protected static final String COMPRESSION_CODEC = "writer.compression.codec";
	
	public AbstractHdfsWriter(String outputPath, Properties prop) throws IOException
	{
		init(outputPath, prop);
	}
	
	abstract protected void init(String outputPath, Properties prop) throws IOException;
	
	abstract public void writeRow(String rowType, String[] columns) throws IOException;
	
	abstract public void close() throws IOException;
	
	
}
