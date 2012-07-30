package com.cloudera.sa.hcu.env2.arvo.job;


public class EnvMain
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
		
		if (subCommand[0].equals("kfv"))
		{
			ConvertEnvSingleTable2Avro.main(subCommandArgs);
		}else if (subCommand[0].equals("rowType"))
		{
			ConvertEnvMultiTable2MultiAvro.main(subCommandArgs);
		}else
		{
			outputConsoleHelp();
		}
	}

	private static void outputConsoleHelp()
	{
		System.out.println("List of sub commands:");
		System.out.println(" kfv : For files where each row is <key>|<field>|<value>.  This will create a schema that will store all the rows");
		System.out.println(" rowType : For files where one column determines the row type.  This will sperate row types into different avro files with different schemas.");
		
	}
}
