package com.yahoo.ycsb.db;

import com.yahoo.ycsb.config.PropertiesConfig;
import java.util.Properties;

public class CloudSearchConfig extends PropertiesConfig{

	//Search endpoint for your search domain
	public static final String SEARCH_ENDPOINT = "cloudsearch.searchEndpoint";
	
	//Doc endpoint for your search domain
	public static final String DOC_ENDPOINT = "cloudsearch.docEndpoint";
	
	//Full URL pointing the the cloudsearch endpoint  e.g https://cloudsearch.us-east-1.amazonaws.com/
	public static final String CLOUDSEARCH_ENDPOINT = "cloudsearch.endpoint";
	
	public static final String AWS_ACCESS_KEY_ID = "aws.accessKeyId";
	
	public static final String AWS_SECRET_ACCESS_KEY = "aws.secretKey";
	
	public CloudSearchConfig(Properties properties){
		super(properties);
		declareProperty(SEARCH_ENDPOINT, true);
		declareProperty(DOC_ENDPOINT, true);
		declareProperty(CLOUDSEARCH_ENDPOINT, true);
		declareProperty(AWS_ACCESS_KEY_ID, null, false);
		declareProperty(AWS_SECRET_ACCESS_KEY, null, false);
	}
	
	public String getSearchEndpoint(){
		return getString(SEARCH_ENDPOINT);
	}
	
	public String getDocEndpoint(){
		return getString(DOC_ENDPOINT);
	}
	
	public String getCloudSearchEndpoint(){
		return getString(CLOUDSEARCH_ENDPOINT);
	}
	
	public String getAccessKeyId(){
		return getString(AWS_ACCESS_KEY_ID);
	}
	
	public String getSecretKeyId(){
		return getString(AWS_SECRET_ACCESS_KEY);
	}
}
