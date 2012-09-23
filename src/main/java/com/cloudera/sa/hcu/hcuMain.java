package com.cloudera.sa.hcu;

import com.cloudera.sa.hcu.env2.arvo.job.EnvMain;
import com.cloudera.sa.hcu.io.console.out.ConsoleOutMain;
import com.cloudera.sa.hcu.io.get.GetMain;
import com.cloudera.sa.hcu.io.put.PutMain;
import com.cloudera.sa.hcu.io.recompression.nonsplittable.NonSplittableGzipToSeq;
import com.cloudera.sa.hcu.io.recompression.nonsplittable.NonSplittableZipToSeq;
import com.cloudera.sa.hcu.io.route.RouteMain;

public class hcuMain
{

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception
	{
		if (args.length < 1)
		{
			outputConsoleHelp();
			return;
		}
		
		String[] subCommand = args;
		String[] subCommandArgs = new String[subCommand.length-1];
		System.arraycopy( subCommand, 1, subCommandArgs, 0, subCommandArgs.length );
		
		if (subCommand[0].equals("put"))
		{
			PutMain.main(subCommandArgs);
		}else if (subCommand[0].equals("out"))
		{
			ConsoleOutMain.main(subCommandArgs);
		}else if (subCommand[0].equals("env"))
		{
			EnvMain.main(subCommandArgs);
		}else if (subCommand[0].equals("route"))
		{
			RouteMain.main(subCommandArgs);
		}else if (subCommand[0].equals("get"))
		{
			GetMain.main(subCommandArgs);
		}else if (subCommand[0].equals("mkSplittableGzip"))
		{
			NonSplittableGzipToSeq.main(subCommandArgs);
		}else if (subCommand[0].equals("mkSplittableZip"))
		{
			NonSplittableZipToSeq.main(subCommandArgs);
		} else
		{
			outputConsoleHelp();
		}
	}

	private static void outputConsoleHelp()
	{
		System.out.println("List of sub commands:");
		System.out.println(" put : N threaded of multible local file formats to put into HDFS compressed and in a splittable format.");
		System.out.println(" route : Define directories to be file transports into HDFS.");
		System.out.println(" out : Writes common splittable formated HDFS files to your console.");
		System.out.println(" get : Writes common splittable formated HDFS files to local uncompressed files.");
		System.out.println(" env : Convert Entity-attribute-value HDFS files to RC or Avro Files using MR");
		System.out.println(" mkSplittableGzip : Converts a non splittable gzip file in hdfs to a sequence files compressed in snappy, gzip, or bzip2");
		System.out.println(" mkSplittableZip : Converts a non splittable zzip file in hdfs to a sequence files compressed in snappy, gzip, or bzip2");
		
		
	}

}
