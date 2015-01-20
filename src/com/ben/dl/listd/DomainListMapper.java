package com.ben.dl.listd;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
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
	private static final Log LOG = LogFactory.getLog(DomainListMapper.class);

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	public void map(Text arg0, ArchiveReader ar,
			OutputCollector<Text, IntWritable> output, Reporter repo)
			throws IOException {
			LOG.info("Entered map operation");
		int j=1;
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
					//if(j==1){LOG.info("JSON:"+ json.toString());j=0;}
					try{
						JSONArray jsArray = null;
						if(json.has("Envelope")){
							if(json.getJSONObject("Envelope").has("Payload-Metadata")){
								if(json.getJSONObject("Envelope").getJSONObject("Payload-Metadata").has("HTTP-Response-Metadata")){
									if(json.getJSONObject("Envelope").getJSONObject("Payload-Metadata").getJSONObject("HTTP-Response-Metadata").has("HTML-Metadata")){
										if(json.getJSONObject("Envelope").getJSONObject("Payload-Metadata").getJSONObject("HTTP-Response-Metadata").getJSONObject("HTML-Metadata").has("Links")){
												jsArray = json.getJSONObject("Envelope").getJSONObject("Payload-Metadata").getJSONObject("HTTP-Response-Metadata").getJSONObject("HTML-Metadata").getJSONArray("Links");
												if(jsArray==null)
												{
													continue;
												}
											}else {continue;}
										}else {continue;}
									}else {continue;}
								}else {continue;}
							}else {continue;}
						
						
						for(int i=0; i<jsArray.length(); i++){
							if(jsArray.getJSONObject(i).has("url")){
								String rootDomainName = DomainLister.getRootDomainName(jsArray.getJSONObject(i).getString("url"));
								if(rootDomainName != null){
									LOG.info("Domain:"+rootDomainName);
									outKey.set(rootDomainName);	
									output.collect(new Text(rootDomainName), new IntWritable(1));//(key, outVal);
								}
							}
							
						}

					}catch(JSONException ex){
						
					}
					
					
				}catch(Exception ex){
					
				}
			}
		
	}



}
