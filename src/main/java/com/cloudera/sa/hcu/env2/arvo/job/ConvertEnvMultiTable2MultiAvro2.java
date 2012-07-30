package com.cloudera.sa.hcu.env2.arvo.job;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 
 * Summary of map reducer job:<br>
 * <br>
 * This job will take three major inputs: <br>
 * <br>
 * 1. A flat file filled with data of multiple row types <br>
 * <br>
 * - This flat file is unique because a fixed column defines the row type. So
 * one data file will contain many row types (or these could be seen as multiple
 * tables in one data file).<br>
 * <br>
 * - Below is an example where the second column defines the row type.<br>
 * <br>
 * key1|car|red|sport|49600|corvette <br>
 * key2|shoe|blue|running|100|Zoom|4 <br>
 * key3|car|blue|eco|27000|leaf <br>
 * <br>
 * 2. Files for every row type that contain a avro schema for that row type. <br>
 * -Below is an example of the avro schemas for the data set above. <br>
 * <br>
 * File1<br>
 * { "name": "car", "type": "record", "fields": [<br>
 * {"name": "id", "type": "string"}, <br>
 * {"name": "color", "type": "string"}, <br>
 * {"name": "type", "type": "string"}, <br>
 * {"name": "price", "type": "double"}, <br>
 * {"name": "title", "type": "string"} ]}; <br>
 * <br>
 * File2<br>
 * { "name": "shoe", "type": "record", "fields": [ <br>
 * {"name": "id", "type": "string"}, <br>
 * {"name": "color", "type": "string"}, <br>
 * {"name": "type", "type": "string"}, <br>
 * {"name": "price", "type": "double"}, <br>
 * {"name": "title", "type": "string"}, <br>
 * {"name": "weight", "type": "double"} ]}; <br>
 * <br>
 * - File name is not important, but schema name is. It must match the row type
 * value.<br>
 * <br>
 * - Make sure the fields in the schema are in the same order as the columns in
 * the flat file. The column that defines the row type is skipped over because
 * there is no need to store it in the avro file. <br>
 * <br>
 * - All these files will be in the same directory on the local file system. The
 * directory name will be given to the program. <br>
 * <br>
 * 3. The column index of the flat file that defines what row type the row is. <br>
 * <br>
 * - For the data set above the value you be 1, because the second column is the
 * row type column and I'm using a 0 as the first index <br>
 * <br>
 * The output of this job will be a file for every avro schema. <br>
 * <br>
 * Map only or map/reduce: This problem can be solved many different ways. <br>
 * <br>
 * - Map only: yes this will execute faster then its map/reduce brother but it
 * will have more smaller files, which may effect the performance of jobs
 * running output the resulting files <br>
 * <br>
 * - Simple Map/Reduce: sort on the key will give the caller the power to
 * determine the number of files getting generated. {Schema count} * {reducers}<br>
 * <br>
 * - Smart Map/Reduce: If we gave each schema a percentage of total reducers we
 * could have much better control on the number of files per schema. This
 * becomes very important when a one or a couple of row types have a lot more
 * data then other row types <br>
 * I'm going to do the Smart Map/Reduce because it feels like the most
 * interesting. So I will need additional inputs.<br>
 * <br>
 * So a fourth input will be a property file with sort column and number of
 * reducers for schema (or row type). This property file will look like the
 * following for the above data set<br>
 * <br>
 * car.sort.idx=0<br>
 * car.reduce.count=2<br>
 * shoe.sort.idx=3<br>
 * shoe.reduce.count=4<br>
 * <br>
 * This will sort the cars by id and the shoes by type. The shoes will get twice
 * as many reducers as the cars will.<br>
 * <br>
 * Just put this property file in the schema directory
 * 
 * @author ted.malaska
 * 
 */

public class ConvertEnvMultiTable2MultiAvro2
{
	private static final String SORT_IDX_POSTFIX = "sort.idx";

	private static final String REDUCE_COUNT_POSTFIX = "reduce.count";

	final static String STAT_DIR_POST_NAME = "_STAT";

	final static String CONF_NUM_OF_AVRO_SCHEMAS = "numOfAvroSchems";
	final static String CONF_ROW_TYPE_COLUMN_INDEX = "rowTypeColumnIndex";
	final static String CONF_AVRO_SCHEMA_PREFIX = "avroSchema.";
	final static String CONF_OUTPUT_DIR = "customOutputDir";
	final static String CONF_SORT_IDX = "sort.idx.";
	final static String CONF_REDUCE_COUNT = "reduce.count.";

	public static Pattern pipeSplit = Pattern.compile("\\|");

	/**
	 * This mapper will output following key
	 * 
	 * {sort value}|{row type}
	 * 
	 * The value will be all values that are not in the key, with a pipe
	 * delimiter
	 * 
	 * @author ted.malaska
	 * 
	 */
	public static class CustomMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		Text newKey = new Text();
		Text newValue = new Text();

		//This is the column index of the row type column
		int rowTypeColumnIndex;

		//This map will contain the name of the schema as the key and the index of the field to be sorted as the value
		HashMap<String, Integer> schemaSortIndexMap = new HashMap<String, Integer>();

		
		/**
		 * Before we start greating record we need some information from the configuration first.<br>
		 * <br>
		 * setup will get the following information<br>
		 * <br>
		 * 1. The row type column index<br>
		 * 2. The sort column index for every schema (row type)
		 */
		@Override
		public void setup(Context context)
		{
			rowTypeColumnIndex = Integer.parseInt(context.getConfiguration().get(CONF_ROW_TYPE_COLUMN_INDEX));
			int numberOfSchemas = Integer.parseInt(context.getConfiguration().get(CONF_NUM_OF_AVRO_SCHEMAS));

			for (int i = 0; i < numberOfSchemas; i++)
			{
				String sortIndexValue = context.getConfiguration().get(CONF_SORT_IDX + i);
				int equalIndex = sortIndexValue.indexOf('=');
				String sortName = sortIndexValue.substring(0, equalIndex);
				Integer sortIndex = Integer.parseInt(sortIndexValue.substring(equalIndex + 1, sortIndexValue.length()));

				schemaSortIndexMap.put(sortName, sortIndex);
			}

		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] valueSplit = pipeSplit.split(value.toString());

			String rowType = valueSplit[rowTypeColumnIndex];

			Integer sortIndexInteger = schemaSortIndexMap.get(rowType);
			if (sortIndexInteger == null)
			{
				//If we fail to get the sort index throw an exception
				throwRowTypeNotFoundException(rowType);
			}

			//the key will be made of the sort field and then the rowtype
			newKey.set(valueSplit[sortIndexInteger] + "|" + rowType);

			//Get all the left over values.
			StringBuilder strBuilder = new StringBuilder();
			for (int i = 0; i < valueSplit.length; i++)
			{
				//make sure the index doesn't equal the rowType or the sortIndex
				if (i != sortIndexInteger && i != rowTypeColumnIndex)
				{
					strBuilder.append(valueSplit[i] + "|");
				}
			}
			//remove the last pipe
			strBuilder.deleteCharAt(strBuilder.length() - 1);

			newValue.set(strBuilder.toString());

			context.write(newKey, newValue);
		}

		/**
		 * This function will throw a RuntimeException that says a rowType has no matching schema..
		 * This function will output in the exception message all the schemas defined and the 
		 * rowType that is not found.
		 * 
		 * @param rowType
		 */
		private void throwRowTypeNotFoundException(String rowType)
		{
			StringBuilder strBuilder = new StringBuilder();
			for (String str : schemaSortIndexMap.keySet())
			{
				strBuilder.append(str + "|");
			}

			throw new RuntimeException("Failed trying to get sort index for row type '" + rowType + "'. The map only has the following keys: " + strBuilder.toString());
		}

	}

	/**
	 * A reduce will only get one row type and only output one file. However the
	 * reduce will not know which file until the first row is given to it.
	 * 
	 * @author ted.malaska
	 * 
	 */
	public static class CustomReducer extends Reducer<Text, Text, LongWritable, Text>
	{
		LongWritable newKey = new LongWritable();
		Text newValue = new Text();

		int rowTypeColumnIndex;
		int sortColumnIndex;

		Schema schema;
		
		//This holders all the sort indexes.  We only need one so this is over kill.  
		HashMap<String, Integer> schemaSortIndexMap = new HashMap<String, Integer>();

		boolean isFirstRecord = true;
		AvroMultipleOutputs amos;
		String rowType ;

		boolean closedFile = false;

		/**
		 * Before we start getting rows we need the following information from the configuration
		 * <br><br>
		 * 1. rowType column index
		 * 2. sort indexes of every schema type 
		 * @param context
		 */
		@Override
		public void setup(Context context)
		{

			rowTypeColumnIndex = Integer.parseInt(context.getConfiguration().get(CONF_ROW_TYPE_COLUMN_INDEX));
			int numberOfSchemas = Integer.parseInt(context.getConfiguration().get(CONF_NUM_OF_AVRO_SCHEMAS));

			amos = new AvroMultipleOutputs(context);
			
			for (int i = 0; i < numberOfSchemas; i++)
			{
				String sortIndexValue = context.getConfiguration().get(CONF_SORT_IDX + i);
				int equalIndex = sortIndexValue.indexOf('=');
				String sortName = sortIndexValue.substring(0, equalIndex);
				Integer sortIndex = Integer.parseInt(sortIndexValue.substring(equalIndex + 1, sortIndexValue.length()));

				schemaSortIndexMap.put(sortName, sortIndex);
			}

		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			//Spliting the key. {sortValue}|{rowType}
			String[] keySplit = pipeSplit.split(key.toString());

			for (Text value : values)
			{

				String[] valueSplit = pipeSplit.split(value.toString());

				// If this is the first record we need to open the file
				if (isFirstRecord)
				{
					isFirstRecord = false;

					//We can open the avro file now because with the first record we now know what schema to use
					rowType = keySplit[1];
					sortColumnIndex = schemaSortIndexMap.get(rowType);
					
					schema = new Schema.Parser().parse(context.getConfiguration().get(SCHEMA_PRE_CONF + rowType));
					
					context.getCounter("debug", "Setup schema: " + rowType).increment(1);

				}

				//This will count of many of a rowType was outputed 
				context.getCounter("debug", "Reducer output for schema: " + rowType).increment(1);

				// TODO I think I can avoid constructing this evertime
				GenericRecord datum = new GenericData.Record(schema);

				//THis strBuilder is building a debug string that will only be seen if there is a problem writing out the record.
				StringBuilder debugStrBuilder = new StringBuilder();

				//This is the index of what value we are reading from the records value string
				int valueIdx = 0;
				for (int i = 0; i < schema.getFields().size(); i++)
				{
					Schema.Field field = schema.getFields().get(i);

					String fieldValue = "";

					Type type = field.schema().getType();
					
					try
					{
						//check if i is point to the sort column.  If so get it from the key not the value
						if ((i < rowTypeColumnIndex && i == sortColumnIndex) || (i >= rowTypeColumnIndex && i + 1 == sortColumnIndex))
						{
							fieldValue = keySplit[0];

							debugStrBuilder.append("{sort}-");
						} else
						{
							fieldValue = valueSplit[valueIdx];
							valueIdx++;

							debugStrBuilder.append("{nonsort}-");
						}

						debugStrBuilder.append(fieldValue + "-" + valueIdx + "|");
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
					} catch (Exception e)
					{
						//There is a lot that can go wrong in the above code.  This exception gives enough debug information to solve the problem.
						throw new RuntimeException("Problem inserting value :" + fieldValue + " for field:" + field.name() + "for Type:" + type + " for i: " + i + " for schema:" + schema.getName() + ".  This is the whole record: " + key.toString() + " "
								+ value.toString() + "=" + debugStrBuilder.toString(), e);
					}

				}

				//write the record out
				amos.write(rowType, datum);
				//dataFileWriter.append(datum);
			}

		}


		@Override
		public void cleanup(Context context) throws IOException, InterruptedException
		{
			amos.close();
		}


	}

	/**
	 * This is a complex partitioner. It has to group rows by row type and only
	 * give them to the reducers that they are assigned to. A reducer will only
	 * process one row type. This will greatly reduce output files and address
	 * skewing
	 * 
	 * @author ted.malaska
	 * 
	 */
	public static class CustomPartitioner extends Partitioner<Text, Text> implements Configurable
	{

		public static final int GROUP_KEY_IDX = 0;

		Configuration conf;
		HashMap<String, ReducerRangePojo> schemaReducerRangeMap = new HashMap<String, ReducerRangePojo>();

		@Override
		public int getPartition(Text key, Text value, int numPartitions)
		{

			if (numPartitions == 1)
			{
				return 0;
			} else
			{
				String[] keySplit = pipeSplit.split(key.toString());

				// keySplit 1 is the rowType
				ReducerRangePojo rrp = schemaReducerRangeMap.get(keySplit[1]);

				// pick a random reducer with in the range
				// keySplit 0 is the sort key
				int result = keySplit[0].hashCode() % rrp.count;

				return Math.abs(result) + rrp.startIndex;

			}
		}

		/**
		 * We need to get the properties so we can get the reducers ranges
		 */
		public void setConf(Configuration conf)
		{
			this.conf = conf;
			int numberOfSchemas = Integer.parseInt(conf.get(CONF_NUM_OF_AVRO_SCHEMAS));

			int reducerIdx = 0;

			for (int i = 0; i < numberOfSchemas; i++)
			{
				String sortIndexValue = conf.get(CONF_REDUCE_COUNT + i);

				if (sortIndexValue == null)
				{
					throw new RuntimeException("Config " + CONF_REDUCE_COUNT + i + " was not found.");
				}

				int equalIndex = sortIndexValue.indexOf('=');
				String schemaName = sortIndexValue.substring(0, equalIndex);
				Integer reduceCount = Integer.parseInt(sortIndexValue.substring(equalIndex + 1, sortIndexValue.length()));
				ReducerRangePojo rrp = new ReducerRangePojo();
				rrp.startIndex = reducerIdx;
				rrp.count = reduceCount;

				schemaReducerRangeMap.put(schemaName, rrp);

				reducerIdx += reduceCount;
			}
		}

		public Configuration getConf()
		{
			return conf;
		}

		private class ReducerRangePojo
		{
			int startIndex;
			int count;
		}
	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException
	{
		if (args.length != 4 || args[0].contains("-h"))
		{
			System.out.println("ConvertEnvMultiTable2MultiAvro <data file hdfs input path> <path to local directory containing avro schema> <index of row type column> <hdfs output path>");
			System.out.println();
			System.out.println("ConvertEnvMultiTable2MultiAvro ./inputFile ./schemas 2 ./multiAvroOutput");
			return;
		}

		// Get values from args
		String inputPath = args[0];
		String schemaDirectory = args[1];
		String rowTypeColumn = args[2];
		String outputPath = args[3];

		// Create job
		Job job = new Job();
		job.setJarByClass(ConvertEnvSingleTable2Avro.class);

		// Set some configurations
		job.getConfiguration().set(CONF_OUTPUT_DIR, outputPath);
		job.getConfiguration().set(CONF_ROW_TYPE_COLUMN_INDEX, rowTypeColumn);
		
		processAllSchemasFiles(outputPath, schemaDirectory, job);

		// Define input format and path
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));

		// Define output format and path
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		// Define the mapper & reducer & partitioner
		job.setMapperClass(CustomMapper.class);
		job.setReducerClass(CustomReducer.class);
		job.setPartitionerClass(CustomPartitioner.class);

		// Define the key and value format
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// Exit
		job.waitForCompletion(true);

	}



	/**
	 * This function will read in all the schema files from the schema directory
	 * and put them in the job's configuration.<br>
	 * <br>
	 * Also this function will create all the HDFS directories that will house
	 * the final data <br>
	 * <br>
	 * IMPORTANT: schema name must match the row type value to match.
	 * 
	 * @param schemaDir
	 * @param configuration
	 * @throws IOException
	 */
	protected static void processAllSchemasFiles(String outputPath, String schemaDir, Job job) throws IOException
	{
		Configuration configuration = job.getConfiguration();
		File folder = new File(schemaDir);
		File[] listOfFiles = folder.listFiles();

		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);

		int numberOfSchemaFiles = 0;
		int numberOfSchemaSortIndexes = 0;
		int numberOfSchemaReduceCounts = 0;
		int numberOfReducers = 0;

		try
		{
			for (int i = 0; i < listOfFiles.length; i++)
			{
				if (listOfFiles[i].isFile())
				{
					BufferedReader br = null;
					try
					{
						// open each file
						br = new BufferedReader(new FileReader(listOfFiles[i]));
						String line;

						if (listOfFiles[i].getName().endsWith("properties"))
						{

							while ((line = br.readLine()) != null)
							{

								String schemaName = line.substring(0, line.indexOf('.'));
								String propertyName = line.substring(0, line.indexOf('='));
								String value = line.substring(line.indexOf('=') + 1, line.length());

								if (propertyName.endsWith(REDUCE_COUNT_POSTFIX))
								{
									configuration.set(CONF_REDUCE_COUNT + numberOfSchemaReduceCounts, schemaName + "=" + value);
									numberOfSchemaReduceCounts++;
									numberOfReducers += Integer.parseInt(value);
									System.out.println("Info: " + schemaName + " has " + value + " reducers");
								} else if (propertyName.endsWith(SORT_IDX_POSTFIX))
								{
									configuration.set(CONF_SORT_IDX + numberOfSchemaSortIndexes, schemaName + "=" + value);
									numberOfSchemaSortIndexes++;
								} else
								{
									System.err.println("Warning: Unknow property - " + propertyName);
								}
							}

						} else
						{
							numberOfSchemaFiles++;

							addSchema(outputPath, job, br);
						}
					} finally
					{
						if (br != null)
						{
							br.close();
						}
					}
				}
			}
			if (numberOfSchemaFiles > 0)
			{
				configuration.set(CONF_NUM_OF_AVRO_SCHEMAS, Integer.toString(numberOfSchemaFiles));
			} else
			{
				throw new RuntimeException("No schema files found in:" + schemaDir);
			}
			if (numberOfReducers > 0)
			{
				job.setNumReduceTasks(numberOfReducers);
			} else
			{
				throw new RuntimeException("Number of reducers is set to 0");
			}
			if (numberOfSchemaFiles != numberOfSchemaSortIndexes || numberOfSchemaFiles != numberOfSchemaReduceCounts)
			{
				throw new RuntimeException("Number of schema definitions don't match sort and reduce properties: numberOfSchemaFiles:" + numberOfSchemaFiles + ", numberOfSchemaSortIndexes:"
						+ numberOfSchemaSortIndexes + ", numberOfSchemaReduceCounts:" + numberOfSchemaReduceCounts);
			}
		} finally
		{
			hdfs.close();
		}
	}

	public static String SCHEMA_PRE_CONF = "schemaPreConf.";
	
	private static void addSchema(String outputPath, Job job, BufferedReader br) throws IOException
	{
		String line;
		StringBuilder strBuilder = new StringBuilder();

		// read the file
		while ((line = br.readLine()) != null)
		{
			strBuilder.append(line);
		}

		String schemaStr = strBuilder.toString();

		// get the schema name and make a hdfs directory out
		// of
		// it.
		Schema schema;

		try
		{
			schema = new Schema.Parser().parse(schemaStr);
		} catch (Exception e)
		{
			throw new RuntimeException("Unable to parse schema file: " + schemaStr, e);
		}

		getNamedOutputsList(job);
		
		job.getConfiguration().set(SCHEMA_PRE_CONF + schema.getName(), schemaStr);
		AvroMultipleOutputs.addNamedOutput(job, schema.getName(), AvroKeyOutputFormat.class, schema);
		
		// clear the builder
		strBuilder.delete(0, strBuilder.length());
	}
	
	  private static final String MULTIPLE_OUTPUTS = "avro.mapreduce.multipleoutputs";
	
	  private static List<String> getNamedOutputsList(JobContext job) {
		    List<String> names = new ArrayList<String>();
		    StringTokenizer st = new StringTokenizer(
		      job.getConfiguration().get(MULTIPLE_OUTPUTS, ""), " ");
		    while (st.hasMoreTokens()) {
		      names.add(st.nextToken());
		    }
		    return names;
		  }
	 

}
