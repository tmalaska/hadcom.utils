package com.cloudera.sa.hcu.env2.arvo.test.gen;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class GenerateEnvSingleTableTestFile
{

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException
	{
		if (args.length != 3 || args[0].contains("-h"))
		{
			System.out.println("GenerateTestFile <outputPath> <number of keys> <number of fields>");
			System.out.println();
			System.out.println("GenerateTestFile ./filename 100 10");
			return;
		}
		
		String filename = args[0];
		int numOfKeys = Integer.parseInt(args[1]);
		int numOfFields = Integer.parseInt(args[2]);
		
		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);
		
		FSDataOutputStream outputStream = hdfs.create(new Path(filename));

		BufferedWriter br=new BufferedWriter(new OutputStreamWriter(outputStream));
		
		for (int i = 0; i < numOfKeys; i++)
		{
			for (int j = 0; j < numOfFields; j++)
			{
				//This will leave 20% of the fields empty for each key
				if (Math.random() < .8)
				{
					br.write("key-" + i + "|field-" + j + "|ValueFor" + i + "-" + j + "\n");
				}
			}
		}
		br.close();
	}

}
