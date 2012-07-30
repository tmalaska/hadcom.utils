package com.cloudera.sa.hcu.env2.arvo.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.regex.Pattern;

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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.TextNode;

/**
 * Summary of map reduce job: <br>
 * <br>
 * This job will take a file where each line in that file in of the following
 * format. <br>
 * <br>
 * {unique id}|{field name}|{field value} <br>
 * <br>
 * An example file would be the following <br>
 * <br>
 * Josh|Title|Data Scientist Josh|Age|33 <br>
 * Josh|Favorite Restaurant|Heirloom Cafe <br>
 * Ted|Office Location|Palo Alto <br>
 * Ted|Title|Systems Engineer <br>
 * Ted|Favorite Sport|Golf <br>
 * <br>
 * 
 * This job will take this data and create a avro schema like the following <br>
 * <br>
 * { "name": "givenSchemaName", "type": "record", "fields": [<br>
 * {"name": "Title", "type": "string"}, <br>
 * {"name": "Age", "type": "string"}, <br>
 * {"name": "Favorite_Restaurant", "type": "double"}, <br>
 * {"name": "Office_Location", "type": "string"}, <br>
 * {"name": "Favorite_Sport", "type": "string"} ]};<br>
 * <br>
 * Then write the output file.<br>
 * <br>
 * This is all done in one map/reduce. To do this I have to use the following
 * tricks: <br>
 * 1. The mapper send field information to every reducer. This is done using a
 * override partitioner <br>
 * 2. The avro file is created in the reducer not at the time the job started.
 * This has to be because the schema is not known before the sort and shuffle is
 * complete.<br>
 * <br>
 * 
 * @author ted.malaska
 * 
 */
public class ConvertEnvSingleTable2Avro
{
	//This is the post fix of the normal reducers output directory
	//TODO need to figure out how to turn off normal reducer output
	public static final String STAT_DIR_POST_NAME = "_stats";
	
	//These are name of configuration properties that are added my the main function
	public static final String CONF_OUTPUT_DIR = "convertEnt2Avro.outputDir";
	public static final String CONF_NUM_OF_REDUCERS = "convertEnt2Avro.numOfReducers";
	public static final String CONF_SCHEMA_NAME = "convertEnt2Avro.schemaName";

	//Precompiled pattern for spliting pipe delimitered strings
	public static Pattern pipeSplit = Pattern.compile("\\|");

	// TODO in future versions I may try to figure out the schema at run time
	public static Pattern isIntPattern = Pattern.compile("^(\\+|-)?\\d+$");
	public static Pattern isDoublePattern = Pattern.compile("^(\\+|-)?[0-9]+(\\.[0-9][0-9]?)?");

	public static class CustomMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		int numOfReducers = 0;

		HashSet<String> fieldSet = new HashSet<String>();

		Text newKey = new Text();
		Text newValue = new Text();

		static final int KEY_IDX = 0;
		static final int FIELD_IDX = 1;
		static final int VALUE_IDX = 2;

		public static final String VALUES_SORT_FLAG = "V";

		@Override
		public void setup(Context context)
		{
			numOfReducers = Integer.parseInt(context.getConfiguration().get(CONF_NUM_OF_REDUCERS));
		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String values[] = pipeSplit.split(value.toString());

			newKey.set(VALUES_SORT_FLAG + values[KEY_IDX]);

			String field = values[1].replace(' ', '_');
			field = field.replace('-', '_');

			newValue.set(field + "|" + values[2]);

			context.write(newKey, newValue);

			fieldSet.add(field);
		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException
		{

			for (String field : fieldSet)
			{

				for (int i = 0; i < numOfReducers; i++)
				{
					newKey.set(Integer.toString(i) + "|" + field);
					newValue.set("");

					context.write(newKey, newValue);
				}
			}
		}
	}

	public static class CustomReducer extends Reducer<Text, Text, LongWritable, Text>
	{
		LongWritable newKey = new LongWritable();
		Text newValue = new Text();

		boolean isSchemaBuildingMode = true;

		FSDataOutputStream dataOutputStream;
		DataFileWriter<GenericRecord> dataFileWriter;
		Path outputFilePath;
		FileSystem hdfs;

		Schema schema;
		ArrayList<Schema.Field> fieldList = new ArrayList<Schema.Field>();

		boolean closedFile = false;
		
		String schemaName;

		@Override
		public void setup(Context context) throws IOException
		{
			context.getCounter("debug", "Setup").increment(1);

			String outputDir = context.getConfiguration().get(CONF_OUTPUT_DIR);

			Configuration config = new Configuration();
			hdfs = FileSystem.get(config);

			outputFilePath = new Path(outputDir + "/part-r-" + context.getTaskAttemptID().getTaskID().getId() + "-" + context.getTaskAttemptID().getId());

			dataOutputStream = hdfs.create(outputFilePath);

			DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>();
			// writer.setSchema(s); // I guess I don't need this

			dataFileWriter = new DataFileWriter<GenericRecord>(writer);
			
			//add key field to field list: schemaName_ID
			Schema s = Schema.create(Schema.Type.STRING);
			JsonNode defaultValue = new TextNode("-");
			
			schemaName = context.getConfiguration().get(CONF_SCHEMA_NAME);
			
			Schema.Field schemaField = new Schema.Field(schemaName + "_ID", s, "Field: " + schemaName + "_ID", defaultValue);
			fieldList.add(schemaField);

		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException
		{

			try
			{

				dataFileWriter.close();
				dataOutputStream.close();
				hdfs.close();
			} catch (Exception e)
			{
				// do nothing right now
			}
			closedFile = true;

		}

		@Override
		public void run(Context context) throws IOException, InterruptedException
		{
			try
			{
				setup(context);
				while (context.nextKey())
				{
					reduce(context.getCurrentKey(), context.getValues(), context);
				}
				cleanup(context);
			} catch (Exception e)
			{
				exceptionThrown();
				throw new RuntimeException(e);
			}
		}

		public void exceptionThrown() throws IOException, InterruptedException
		{
			// if the file hasn't closed then close it and kill it.
			if (closedFile == false)
			{
				try
				{

					dataFileWriter.close();

					dataOutputStream.close();
				} catch (Exception e)
				{
					// already closed
				}
				try
				{

					hdfs.deleteOnExit(outputFilePath);
					hdfs.close();
				} catch (Exception e)
				{
					// couldn't delete
				}

			}
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			if (key.toString().startsWith(CustomMapper.VALUES_SORT_FLAG))
			{
				// Check is isSchemaBuildingMode true. If so then this is the
				// first data record
				if (isSchemaBuildingMode == true)
				{
					isSchemaBuildingMode = false;
				
					setUpAvroWriter(context);

				}

				context.getCounter("debug", "Record-Add").increment(1);

				//TODO I think I can avoid constructing this evertime
				GenericRecord datum = new GenericData.Record(schema);

				applyDefaultValuesToRecord(datum);

				//add key as schemaName_ID 
				// - also remember to remove first character of the key because it was added for sorting reasons
				datum.put(schemaName + "_ID", new Utf8(key.toString().substring(1)));
				
				//add all the other fields
				for (Text value : values)
				{
					String valueParts[] = pipeSplit.split(value.toString());
					try
					{
						datum.put(valueParts[0], new Utf8(valueParts[1]));
					} catch (Exception e)
					{
						throw new RuntimeException(valueParts[0] + " " + valueParts[1], e);
					}
				}

				dataFileWriter.append(datum);

			} else
			{
				// We are still building the schema
				addFieldToFieldList(key);
			}

		}

		private void applyDefaultValuesToRecord(GenericRecord datum)
		{
			for (Schema.Field field : fieldList)
			{
				datum.put(field.name(), new Utf8(""));
			}
		}

		private void setUpAvroWriter(Context context) throws IOException
		{
			context.getCounter("debug", "Record-Defined").increment(1);

			schema = Schema.createRecord(schemaName, "Generated Schema", null, false);
			
			schema.setFields(fieldList);

			dataFileWriter.create(schema, dataOutputStream);
		}

		private void addFieldToFieldList(Text key)
		{
			String field = pipeSplit.split(key.toString())[1];

			Schema s = Schema.create(Schema.Type.STRING);
			JsonNode defaultValue = new TextNode("-");

			Schema.Field schemaField = new Schema.Field(field, s, "Field: " + field, defaultValue);
			fieldList.add(schemaField);
		}
	}

	public static class CustomPartitioner extends Partitioner<Text, Text>
	{

		public static final int GROUP_KEY_IDX = 0;

		@Override
		public int getPartition(Text key, Text value, int numPartitions)
		{

			if (numPartitions == 1)
			{
				return 0;
			} else
			{
				String keyString = key.toString();
				if (keyString.startsWith(CustomMapper.VALUES_SORT_FLAG))
				{
					int result = keyString.hashCode() % numPartitions;

					return Math.abs(result);
				} else
				{
					return Integer.parseInt(pipeSplit.split(keyString)[0]);
				}
			}
		}

	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException
	{
		if (args.length != 4 || args[0].contains("-h"))
		{
			System.out.println("ConvertEnt2Avro <inputPath> <outputPath> <schemaName> <# reducers>");
			System.out.println();
			System.out.println("ConvertEnt2Avro ./input ./old testSchemaName 10");
			return;
		}

		// Get values from args
		String inputPath = args[0];
		String outputPath = args[1];
		String schemaName = args[2];
		String numberOfReducers = args[3];

		// Create job
		Job job = new Job();
		job.setJarByClass(ConvertEnvSingleTable2Avro.class);

		// Set some configurations
		job.getConfiguration().set(CONF_OUTPUT_DIR, outputPath);
		job.getConfiguration().set(CONF_NUM_OF_REDUCERS, numberOfReducers);
		job.getConfiguration().set(CONF_SCHEMA_NAME, schemaName);

		// Define input format and path
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));

		// Define output format and path
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(outputPath + STAT_DIR_POST_NAME));

		// Define the mapper and reducer
		job.setMapperClass(CustomMapper.class);
		job.setReducerClass(CustomReducer.class);
		job.setPartitionerClass(CustomPartitioner.class);

		// Define the key and value format
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setNumReduceTasks(Integer.parseInt(numberOfReducers));

		// create output director
		createHdfsDirectoryForFinalOutput(outputPath);

		// Exit
		job.waitForCompletion(true);

		//delete the stat directory because it is empty
		//TODO if we could find a way to not make the stat directory that would be great
		deleteStatDirectory(outputPath);
	}

	private static void deleteStatDirectory(String outputPath) throws IOException
	{
		Configuration config;
		FileSystem hdfs;
		config = new Configuration();
		hdfs = FileSystem.get(config);
		hdfs.deleteOnExit(new Path(outputPath + STAT_DIR_POST_NAME));
	}

	private static void createHdfsDirectoryForFinalOutput(String outputPath) throws IOException
	{
		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);

		hdfs.mkdirs(new Path(outputPath));

		hdfs.close();
	}

}
