package org.commoncrawl.api;

//import org.glassfish.json;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
//import javax.json.*;

//import com.sun.java.util.jar.pack.Instruction.Switch;

public class DomainLister {

	public static HashMap<String, List<String>> domainMap = new HashMap<String, List<String>>();
	public DomainLister() throws FileNotFoundException{
		File f = new File("/home/bigcat/clients/Ben/eclipse-java-mp/CommonCrawlDomainLister/src/tld1.json");
		
		InputStream is = new FileInputStream(f);
		JSONTokener tokener = new JSONTokener(is);
		
		JSONObject jObj = new JSONObject(tokener);
		
		domainMap=(HashMap<String, List<String>>) JsonUtils.jsonToMap(jObj);
	}
	public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, FileNotFoundException, JSONException {
		// parse command line arguments
		
		// echoes back command line arguments
		
		// create a new job configuration for this hadoop job
		
		// configure this job with the amazon aws credentials
		File f = new File("/home/bigcat/clients/Ben/eclipse-java-mp/CommonCrawlDomainLister/src/tld1.json");
		
		InputStream is = new FileInputStream(f);
		JSONTokener tokener = new JSONTokener(is);
		
		JSONObject jObj = new JSONObject(tokener);
		
		
		domainMap=(HashMap<String, List<String>>) JsonUtils.jsonToMap(jObj);
		
		//JsonReader jsonReader = Json.createReader(new FileInputStream(f));	
		
		//JsonObject jsonObject = (JsonObject) jsonReader.readObject();
		
		//domainMap=(HashMap<String, List<String>>) JsonUtils.jsonToMap(jsonObject);
		
		////System.out.println(domainMap.get("au"));
		System.out.println(DomainLister.getFullDomainName("www.blog.google.com/helloworld"));
		System.out.println(DomainLister.getRootDomainName("www.blog.google.com/helloworld"));

		
	}
	
	/**
	 * @param fullDomain e.g blog.google.com
	 * @return basedomain e.g google.com
	 * @throws URISyntaxException 
	 */
	public static String getRootDomainName(String url) throws URISyntaxException{
		String strBasedomain="";
		
		
		URI uri = new URI(url);
	    String fullDomain = uri.getHost();
	    if (fullDomain==null){
	    	return fullDomain;
	    }
		// split web address by dots
		String[] wordArray = fullDomain.split("\\.");
		
		int wordCount = wordArray.length;
		int lastArrayNumber = wordCount - 1;
		int secondLastArrayNumber = wordCount - 2;
		
		
		switch (wordCount){
		
		// for word count in domain address
			case 2:
				
				// if word count is only 2 the domain is already in root format.
				
				strBasedomain = fullDomain;
				break;
			
			default:
				
				// The domain may or may not be in root format as the detected word count is more than 2.
				int countChars = wordArray[lastArrayNumber].length();
				String lastWord = wordArray[lastArrayNumber];
				String secondLastWord = wordArray[secondLastArrayNumber];
				
				switch (countChars){
					case 2:
						
						// if the characters in the TLD is 2, there is a chance of having an SLD as well.
						if (domainMap.containsKey(lastWord))
						{
							// get the list of sld
							List<String> ls = getSldList(lastWord);
							
							
							if(ls.contains('"'+secondLastWord+'"')){
								
								// if there is a matching sld then third from the last is also added to the root domain
								strBasedomain = wordArray[lastArrayNumber - 2]+"."+secondLastWord+"."+lastWord;
								return strBasedomain;
								
							}else{
								
								strBasedomain = secondLastWord+"."+lastWord;
								return strBasedomain;
							}
							
							
							
						}else{
							
							strBasedomain = secondLastWord+"."+lastWord;
							return strBasedomain;
							
						}
						
					default:
							
							// It means, the character count is more than two. Representing TLDs like .info, .com.
							// They stay attached to the main domain. e.g. blog.example.com - exceptions like blog.example.in.com isn't possible.
							// If it's so, then the root domain will be in.com.
							
							strBasedomain = secondLastWord+"."+lastWord;
							
							break;
					
				}
				break;
			
		}
		
		return strBasedomain;
	}
	
	public static List<String> getSldList(String tld){
		List<String> ls = new ArrayList<String>();
		ls=domainMap.get(tld);
		return ls;
	}
	
	public static String getFullDomainName(String url) throws URISyntaxException {
	    URI uri = new URI(url);
	    String domain = uri.getHost();
	    return domain;
	}
	
	
}
