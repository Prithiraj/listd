package com.ben.dl.listd;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.commoncrawl.api.DomainLister;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

enum MAPPERCOUNTER{
	RECORDS_IN,
	NO_SERVER,
	EXCEPTIONS
}
public class DomainListMapper extends MapReduceBase 
	implements Mapper<Text, ArchiveReader, Text, IntWritable>{
	
	private Text outKey = new Text();
	private LongWritable outVal = new LongWritable();
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	public void map(Text arg0, ArchiveReader ar,
			OutputCollector<Text, IntWritable> output, Reporter repo)
			throws IOException {
			System.out.println("Entered map operation");
			for(ArchiveRecord r : ar){
				
				// Skip any records that are not JSON
				if(!r.getHeader().getMimetype().equals("application/json")){
					continue;
				}
				try{
					repo.getCounter(MAPPERCOUNTER.RECORDS_IN).increment(1);
					
					byte[] rawData = IOUtils.toByteArray(r, r.available());
					String content = new String(rawData);
					JSONObject json = new JSONObject(content);
					
					try{
						JSONArray links = json.getJSONObject("Envelope").getJSONObject("Payload-Metadata").getJSONObject("HTTP-Response-Metadata").getJSONObject("HTML-Metadata").getJSONArray("links");
						
						
						for(int i=0; i<links.length(); i++){
							String rootDomainName = DomainLister.getRootDomainName(links.getJSONObject(i).getString("url"));
							System.out.println(rootDomainName);
							if(rootDomainName != null){
								outKey.set(rootDomainName);	
								output.collect(new Text(rootDomainName), new IntWritable(1));//(key, outVal);
							}
							
							
						}
						
						
					}catch(JSONException ex){
						
					}
					
					
				}catch(Exception ex){
					
				}
			}
		
	}



}
