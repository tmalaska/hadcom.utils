package com.cloudera.sa.hcu.io.route;

import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.Constructor;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.regex.Pattern;

import com.cloudera.sa.hcu.io.route.scheduler.IRouteWorker;

public class RouteMain
{
	public static final String CONF_ROOT_ROUTES = "route.";
	
	public static void main(String[] args) throws Exception
	{
		if (args.length < 1)
		{
			System.out.println("Route Help:");
			System.out.println("Parameters: <propertyFilePath>");
			System.out.println();
			return;
		}
		
		Properties p = new Properties();
		p.load(new FileInputStream(new File(args[0])));
		
		Pattern pattern = Pattern.compile("\\.");
		
		for (Entry<Object, Object> entry: p.entrySet())
		{
			String[] keySplit = pattern.split(entry.getKey().toString());
			
			if (keySplit.length == 2)
			{
				if (keySplit[0].equals("route"))
				{
					String routePrefix = keySplit[0] + "." + keySplit[1];
					
					String routeClass = entry.getValue().toString();
					
					try
					{
						Constructor constructor = Class.forName(routeClass).getConstructor(String.class, Properties.class);
					
						IRouteWorker route = (IRouteWorker)constructor.newInstance(routePrefix, p);
						
						route.start();
					}catch(Exception e)
					{

						
						throw new RuntimeException(routePrefix + " value of '" + routeClass + "' was unable to construct into a " + IRouteWorker.class.getName(), e);
					}
				}
			}
		}
		
	}
}
