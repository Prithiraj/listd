package org.commoncrawl.api;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

public class DomainFilter {
	public static HashMap<String, List<String>> domainMap = new HashMap<String, List<String>>();
	public static JSONObject jObj;
	public DomainFilter() throws IOException{
		
		URL url = new URL("https://s3-us-west-2.amazonaws.com/commoncrawloutput/domainfilter.json");
		URLConnection conn = url.openConnection();
		
		InputStream is = conn.getInputStream();
		
		JSONTokener tokener = new JSONTokener(is);
		
		jObj = new JSONObject(tokener);
		
		System.out.println(jObj);
		
		domainMap=(HashMap<String, List<String>>) JsonUtils.jsonToMap(jObj);
		
		
	}
	public static void main(String[] args) throws URISyntaxException, IOException {
		// parse command line arguments
		//DomainFilter df = new DomainFilter();	
			URL url = new URL("https://s3-us-west-2.amazonaws.com/commoncrawloutput/domainfilter.json");
			URLConnection conn = url.openConnection();
		
			InputStream is = conn.getInputStream();
		
			JSONTokener tokener = new JSONTokener(is);
		
			jObj = new JSONObject(tokener);
		
			//System.out.println(jObj.keySet());
			//String lastWord = "uk";
			//boolean isExist = jObj.has(lastWord);
			//JSONArray itemsSLD = jObj.getJSONArray(lastWord).getJSONArray(1);
			//System.out.println(itemsSLD);
			//System.out.println(itemsSLD.has("com"));
			
				
				//domainMap=(HashMap<String, List<String>>) JsonUtils.jsonToMap(jObj);
				
				//JsonReader jsonReader = Json.createReader(new FileInputStream(f));	
				
				//JsonObject jsonObject = (JsonObject) jsonReader.readObject();
				
				//domainMap=(HashMap<String, List<String>>) JsonUtils.jsonToMap(jsonObject);
				
				////System.out.println(domainMap.get("au"));
			System.out.println(DomainFilter.getRootDomainName("http://blog.google.eu/helloworld"));
			System.out.println(DomainFilter.getRootDomainName("http://blog.google.net/helloworld"));
	}
	
	/**
	 * @param fullDomain e.g blog.google.com
	 * @return basedomain e.g google.com
	 * @throws URISyntaxException 
	 */
	public static String getRootDomainName(String url) throws URISyntaxException{
		String strBasedomain=null;
		
		
		URI uri = new URI(url);
	    String fullDomain = uri.getHost();
	    if (fullDomain==null){
	    	return strBasedomain;
	    }
		// split web address by dots
		String[] wordArray = fullDomain.split("\\.");
		
		int wordCount = wordArray.length;
		int n = wordCount - 1;
		int secondLastArrayNumber = wordCount - 2;
		
		String lastWord = wordArray[n];
		String secondLastWord = wordArray[secondLastArrayNumber];
		
		if(wordCount==2){
			if(jObj.has(lastWord)){
				strBasedomain=fullDomain;
				return strBasedomain;
			}else{
				return strBasedomain;
			}
		}else if(wordCount > 2){
			if(jObj.has(lastWord)){
				if(lastWord.length()>2){
					strBasedomain=secondLastWord+"."+lastWord;
					return strBasedomain;
				}else{
					if(isValidSld(lastWord, secondLastWord)){
						strBasedomain=wordArray[n-2]+"."+secondLastWord+"."+lastWord;
						return strBasedomain;
					}else{
						if(isInvalidSld(lastWord, secondLastWord)){
							return strBasedomain;
						}else{
							strBasedomain=secondLastWord+"."+lastWord;
							return strBasedomain;
						}
					}
				}
			}else{
				return strBasedomain;
			}
		}else{
			return strBasedomain;
		}
		
		
		//return strBasedomain;
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
	
	public static boolean isValidSld(String lastWord, String secondLastWord){
		
		JSONArray jsArray = jObj.getJSONArray(lastWord).getJSONArray(1);
		
		for(int i=0; i<jsArray.length(); i++){
			if(secondLastWord.equals(jsArray.getString(i))){
				return true;
			}
		}
		
		return false;
	}
	public static boolean isInvalidSld(String lastWord, String secondLastWord){
		
		JSONArray jsArray = jObj.getJSONArray(lastWord).getJSONArray(0);
		
		for(int i=0; i<jsArray.length(); i++){
			if(secondLastWord.equals(jsArray.getString(i))){
				return true;
			}
		}
		
		return false;
	}
}
