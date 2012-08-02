package com.cloudera.sa.hcu.io.console.out;

import com.cloudera.sa.hcu.io.out.ConsoleOutAvroFile;
import com.cloudera.sa.hcu.io.out.ConsoleOutRcFile;

public class ConsoleOutMain
{
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
		
		if (subCommand[0].equals("avro"))
		{
			ConsoleOutAvroFile.main(subCommandArgs);
		}else if (subCommand[0].equals("rc"))
		{
			ConsoleOutRcFile.main(subCommandArgs);
		}else
		{
			outputConsoleHelp();
		}
	}

	private static void outputConsoleHelp()
	{
		System.out.println("List of sub commands:");
		System.out.println(" avro : Output avro file to the console");
		System.out.println(" rc : Output a rc file to the console.");
		
	}
}
