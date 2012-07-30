package com.cloudera.sa.hcu.env2.arvo.io.examples;

import java.io.IOException;
import java.util.StringTokenizer;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.Reporter;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroMultipleOutputs;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.avro.util.Utf8;
import org.apache.avro.mapred.AvroKey;
public class AvroMapReduceExample {

  private static final String UTF8 = "UTF-8";

  AvroKey a;
  
  public static class MapImpl extends AvroMapper<Utf8, Pair<Utf8, Long>> {
    private AvroMultipleOutputs amos;

    public void configure(JobConf Job) {
      this.amos = new AvroMultipleOutputs(Job);
    }

    @Override
      public void map(Utf8 text, AvroCollector<Pair<Utf8,Long>> collector,
                      Reporter reporter) throws IOException {
      StringTokenizer tokens = new StringTokenizer(text.toString());
      while (tokens.hasMoreTokens()) {
        String tok = tokens.nextToken();
        collector.collect(new Pair<Utf8,Long>(new Utf8(tok),1L));
        amos.getCollector("myavro2",reporter)
          .collect(new Pair<Utf8,Long>(new Utf8(tok),1L).toString());
      }
        
    }
    public void close() throws IOException {
      amos.close();
    }

  }
  
  public static class ReduceImpl
    extends AvroReducer<Utf8, Long, Pair<Utf8, Long> > {
    private AvroMultipleOutputs amos;
    
    public void configure(JobConf Job)
    {
    	AvroKey a;
        amos=new AvroMultipleOutputs(Job);
    }    

    @Override
    public void reduce(Utf8 word, Iterable<Long> counts,
                       AvroCollector<Pair<Utf8,Long>> collector,
                       Reporter reporter) throws IOException {
      long sum = 0;
      for (long count : counts)
        sum += count;
      Pair<Utf8,Long> outputvalue= new Pair<Utf8,Long>(word,sum);
      amos.getCollector("myavro",reporter).collect(outputvalue);
      amos.getCollector("myavro1",reporter).collect(outputvalue.toString());
      collector.collect(new Pair<Utf8,Long>(word, sum));
    }
    public void close() throws IOException
    {
      amos.close();
    }
  }    
/*
  @Test public void runTestsInOrder() throws Exception {
    testJob();
    testProjection();
    testProjection1();
    testJob_noreducer();
    testProjection_noreducer();
  }
  */
  @SuppressWarnings("deprecation")
  public static void main(String[] args) throws Exception {
    JobConf job = new JobConf();
    
//    private static final String UTF8 = "UTF-8";
    Path outputPath = new Path(args[1]);
    
    outputPath.getFileSystem(job).delete(outputPath);
    
    job.setJobName("AvroMultipleOutputs");
    
    AvroJob.setInputSchema(job, Schema.create(Schema.Type.STRING));
    AvroJob.setOutputSchema(job,
                            new Pair<Utf8,Long>(new Utf8(""), 0L).getSchema());
    
    AvroJob.setMapperClass(job, MapImpl.class);        
    AvroJob.setReducerClass(job, ReduceImpl.class);
    
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, outputPath);
    FileOutputFormat.setCompressOutput(job, false);
    AvroMultipleOutputs.addNamedOutput(job,"myavro",AvroOutputFormat.class, new Pair<Utf8,Long>(new Utf8(""), 0L).getSchema());
    AvroMultipleOutputs.addNamedOutput(job,"myavro1",AvroOutputFormat.class, Schema.create(Schema.Type.STRING));
    AvroMultipleOutputs.addNamedOutput(job,"myavro2",AvroOutputFormat.class, Schema.create(Schema.Type.STRING));   
    

    JobClient.runJob(job);
    
  }
 }
