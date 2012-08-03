package com.cloudera.sa.hcu.io.get;


public class GetMain
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
		
		if (subCommand[0].equals("rc"))
		{
			GetRcFile.main(subCommandArgs);
		}else if (subCommand[0].equals("avro"))
		{
			GetAvroFile.main(subCommandArgs);
		}else if (subCommand[0].equals("seq"))
		{
			GetSequenceFile.main(subCommandArgs);
		}else
		{
			outputConsoleHelp();
		}
	}

	private static void outputConsoleHelp()
	{
		System.out.println("List of sub commands:");
		System.out.println(" seq : Writes a sequence file to local as uncompress text");
		System.out.println(" avro : Writes a avro file to local as uncompress text");
		System.out.println(" rc : Writes a rc file to local as uncompress text");
		
	}
}
