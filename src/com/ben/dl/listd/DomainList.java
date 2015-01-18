package com.ben.dl.listd;


//import java.util.logging.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.cmu.lemurproject.WarcFileInputFormat;



public class DomainList extends Configured implements Tool{

	/**
	 * Contains Amazon S3 bucket holding the commoncrawl data
	 */
	private static final String CC_BUCKET="s3://aws-publicdatasets/";
	
	//private static final Logger LOG = Logger.getLogger(DomainList.class);
	/*private static class CSVOutputFormat extends TextOutputFormat<Text, LongWritable>{
		public RecordWriter<Text, LongWritable> getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress)
		throws IOException{
			
			Path file = FileOutputFormat.getTaskOutputPath(job, name);
			FileSystem fs= file.getFileSystem(job);
			FSDataOutputStream fileOut=fs.create(file, progress);
			return new CSVRecordWriter(fileOut);
			
		}
	}
	protected static class CSVRecordWriter 
	implements RecordWriter<Text, LongWritable>{
		protected DataOutputStream outStream;
		
		public CSVRecordWriter(DataOutputStream out){
			this.outStream = out;
		}

		public void close(Reporter arg0) throws IOException {
			// TODO Auto-generated method stub
			outStream.close();
		}

		public synchronized void write(Text key, LongWritable value) throws IOException {
			// TODO Auto-generated method stub
			CsvRecordOutput csvOutput = new CsvRecordOutput(outStream);
			csvOutput.writeString(key.toString(), "word");
			csvOutput.writeLong(value.get(),"occurences");
		}
	}*/
	public static void main(String[] args) throws Exception {
		//"https://s3-us-west-2.amazonaws.com/commoncrawloutput/wat.list.gz"
		URL url = new URL(args[2]);
		
		URLConnection conn = url.openConnection();
		
		InputStream is = conn.getInputStream();
		GZIPInputStream gis = new GZIPInputStream(is);
		InputStreamReader r = new InputStreamReader(gis);// gis = new GZIPInputStream(fn);
		
		BufferedReader br = new BufferedReader(r);
		
		String line="";
		int lineNumber=0;
		
		while ((line = br.readLine()) != null) {
	        args[2] = CC_BUCKET.concat(line);
	        System.out.println(args[2]);
	        int res = ToolRunner.run(new Configuration(), new DomainList(), args);
	        
	    }
	}
	
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub

		String AccessKey = args[0]; //1
		String SecretKey = args[1]; //2
		String InputPath = args[2]; //3
		String OutputPath = args[3]; //4
		
		Configuration conf = getConf();
		conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem");
		conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem");
		conf.set("fs.s3.awsAccessKeyId",AccessKey);
		conf.set("fs.s3.awsSecretAccessKey",SecretKey);
		
		JobConf job = new JobConf(conf, DomainList.class);
		job.setJarByClass(getClass());
		job.setNumReduceTasks(1);
		
		
		//Path in = new Path(args[2]);
		//Path out = new Path(args[3]);
		
		// Specify job specific parameters
		// Specify various job-specific parameters     
        job.setJobName("my-app");
        FileInputFormat.setInputPaths(job, new Path(InputPath));
        FileOutputFormat.setOutputPath(job, new Path(OutputPath));
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        
        job.setInputFormat(WarcFileInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        
        job.setMapperClass(DomainListMapper.class);
        job.setCombinerClass(DomainListReducer.class);
        job.setReducerClass(DomainListReducer.class);
		
        RunningJob rj=JobClient.runJob(job);
        System.out.println(rj.getJobStatus());
		return 0;
	}

}
