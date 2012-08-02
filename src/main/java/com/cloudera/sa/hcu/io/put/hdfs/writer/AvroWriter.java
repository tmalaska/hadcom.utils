package com.cloudera.sa.hcu.io.put.hdfs.writer;

import java.io.IOException;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
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

import com.cloudera.sa.hcu.utils.PropertyUtils;

public class AvroWriter extends AbstractHdfsWriter
{
	DataFileWriter<GenericRecord> dataFileWriter;
	Schema schema;
	
	public static final String CONF_SCHEMA_JSON = "avro.writer.schema.json";
	public static final String CONF_COMPRESSION_CODEC = COMPRESSION_CODEC;
	
	public AvroWriter(Properties p) throws Exception
	{
		super( p);
	}
	
	public AvroWriter(String outputPath, String schemaJson, String compressionCodec) throws IOException
	{
	
		super(makeProperties(outputPath, schemaJson, compressionCodec));
	}
		
	private static Properties makeProperties(String outputPath, String schemaJson, String compressionCodec)
	{
		Properties p = new Properties();
		
		p.setProperty(CONF_OUTPUT_PATH, outputPath);
		p.setProperty(CONF_SCHEMA_JSON, schemaJson);
		p.setProperty(CONF_COMPRESSION_CODEC, compressionCodec);
		
		return p;
	}
	
	@Override
	protected void init(String outputPath, Properties p) throws IOException
	{
		schema = (new Schema.Parser()).parse(PropertyUtils.getStringProperty(p, CONF_SCHEMA_JSON));
		 
		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);
		
		Path outputFilePath = new Path(outputPath);

		FSDataOutputStream dataOutputStream = hdfs.create(outputFilePath);
		
		DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>();
		
		dataFileWriter = new DataFileWriter<GenericRecord>(writer);
		
		dataFileWriter.create(schema, dataOutputStream);
	
	}

	public void writeRow(String rowType, String[] columns) throws IOException
	{
		GenericRecord datum = new GenericData.Record(schema);
		
		for (int i = 0; i < columns.length; i++)
		{
			String fieldValue = columns[i];

			Schema.Field field = schema.getFields().get(i);
			Type type = field.schema().getType();

			//This will give the value too avro in the right format
			//TODO I've got to believe there is a better way to do this
			if (type.equals(Type.STRING))
			{
				datum.put(field.name(), new Utf8(fieldValue));
			} else if (type.equals(Type.DOUBLE))
			{
				datum.put(field.name(), new Double(fieldValue));
			} else if (type.equals(Type.INT))
			{
				datum.put(field.name(), new Integer(fieldValue));
			} else if (type.equals(Type.LONG))
			{
				datum.put(field.name(), new Long(fieldValue));
			} else if (type.equals(Type.FLOAT))
			{
				datum.put(field.name(), new Float(fieldValue));
			} else
			{
				throw new RuntimeException("ConvertEnvMultiTable2MultiAvro doesn't supper type :" + type + " yet.  Put in a bug request");
			}
		}
		dataFileWriter.append(datum);
		
	}

	public void close() throws IOException
	{
		dataFileWriter.close();
	}
}
