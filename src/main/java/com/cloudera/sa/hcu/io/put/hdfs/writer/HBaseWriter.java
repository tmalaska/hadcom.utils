package com.cloudera.sa.hcu.io.put.hdfs.writer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.conf.Configuration;

public class HBaseWriter extends AbstractWriter
{

	public static final String CONF_TABLE_PREFIX = "hbase.writer.table";
	
	private HashMap<String, HTablePutter> putterMap;
	
	public static final Pattern pattern = Pattern.compile("\\.");
	
	Configuration config;
	
	public HBaseWriter(Properties prop) throws IOException
	{
		super(prop);
	}
	
	
	/**
	 * Init will read the property file to see how to populate the table from the file.<br>
	 * <br>
	 * Here is the why the property file should look<br>
	 * <br>
	 * hbase.writer.table.user.key=%4$1s-P-%3$1s
	 * hbase.writer.table.user.cf1.firstNm=%1$1s
	 * hbase.writer.table.user.cf1.lastNm=%2$1s
	 * hbase.writer.table.event.key=%5$1s-P
	 * hbase.writer.table.event.cf1.user_id=%4$1s
	 * 
	 * This will write each row to the user and to the event table
	 * 
	 * @param outputPath
	 * @param prop
	 * @throws IOException
	 */
	@Override
	protected void init(String outputPath, Properties prop) throws IOException
	{
		putterMap = new HashMap<String, HTablePutter>();
				
		config = HBaseConfiguration.create();
	    
	    for (Entry<Object, Object> entry: prop.entrySet())
	    {
	    	String key = entry.getKey().toString();
	    	
	    	if (key != null && key.startsWith(CONF_TABLE_PREFIX))
	    	{
	    		String[] keySplit = pattern.split(key);
	    		if (keySplit.length > 3 )
	    		{
	    			HTablePutter putter = putterMap.get(keySplit[3]);
	    			if (putter == null)
	    			{
	    				putter = new HTablePutter();
	    				putterMap.put(keySplit[3], putter);
	    				
	    				HTable table = new HTable(config, keySplit[3]);
		    			putter.setTable(table);
	    			}
	    			
	    			if (keySplit.length == 5 && keySplit[4].equals("key"))
	    			{
	    				
	    				putter.setFormatKeyStr((String)entry.getValue());
	    				
	    			}else if (keySplit.length == 6)
	    			{
	    				String columnFamily = keySplit[4];
	    				String column = keySplit[5];
	    				String valueFormatStr = (String)entry.getValue();
	    				PutPojo putPojo = new PutPojo(columnFamily, column, valueFormatStr);
	    				putter.addPutPojo(putPojo);
	    			}
	    		}
	    	}
	    }
	}
	
	

	@Override
	public void writeRow(String rowType, String[] columns) throws IOException
	{
		for (Entry<String, HTablePutter> entry: putterMap.entrySet())
		{
			entry.getValue().put(columns);
		}
		
		
	}

	@Override
	public void close() throws IOException
	{
		for (Entry<String, HTablePutter> entry: putterMap.entrySet())
		{
			entry.getValue().close();
		}
		
	}
	
	/**
	 * Everything you need to write to a HBase table
	 * @author ted.malaska
	 *
	 */
	protected class HTablePutter
	{
		HTable table;
		String formatKeyStr;
		ArrayList<PutPojo> putPojoList = new ArrayList<PutPojo>();
		ArrayList<Put> putList = new ArrayList<Put>();
		
		public static final int PUT_BATCH_SIZE = 10;
		
		public HTablePutter()
		{
			super();
		}
		
		public void put(String[] columns) throws IOException
		{
			String key = String.format(formatKeyStr, (Object[])columns);
			
			Put put = new Put(Bytes.toBytes(key));
			
			for (PutPojo putPojo: putPojoList)
			{
				String columnFamily = putPojo.getColumnFamily();
				String columnFormatString = putPojo.getColumnFormatStr();
				String column = String.format(columnFormatString, (Object[])columns);
				String valueFormatString = putPojo.getValueFormatStr();
				String value = String.format(valueFormatString, (Object[])columns);
				
				put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
			}
			
			putList.add(put);
			if (putList.size() > PUT_BATCH_SIZE)
			{
				flush();
			}
		}
		
		
		
		public void setTable(HTable table)
		{
			this.table = table;
		}

		public void setFormatKeyStr(String formatKeyStr)
		{
			this.formatKeyStr = formatKeyStr;
		}

		public void addPutPojo(PutPojo putPojo)
		{
			putPojoList.add(putPojo);
		}
		
		
		private void flush() throws IOException
		{
			table.put(putList);
			putList.clear();
		}
		
		public void close() throws IOException
		{
			table.put(putList);
		}
		
		
		
	}
	
	protected class PutPojo
	{
		String columnFamily;
		String columnFormatStr;
		String valueFormatStr;
		
		public PutPojo(String columnFamily, String columnFormatStr, String valueFormatStr)
		{
			super();
			this.columnFamily = columnFamily;
			this.columnFormatStr = columnFormatStr;
			this.valueFormatStr = valueFormatStr;
		}

		public String getColumnFamily()
		{
			return columnFamily;
		}

		public String getColumnFormatStr()
		{
			return columnFormatStr;
		}

		public String getValueFormatStr()
		{
			return valueFormatStr;
		}
		
	}

}
