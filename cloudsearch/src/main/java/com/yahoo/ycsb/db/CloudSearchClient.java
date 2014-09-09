package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.Set;
import java.util.Vector;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.cloudsearchv2.AmazonCloudSearch;
import com.amazonaws.services.cloudsearchdomain.AmazonCloudSearchDomain;
import com.amazonaws.services.cloudsearchv2.AmazonCloudSearchClient;
import com.amazonaws.services.cloudsearchdomain.AmazonCloudSearchDomainClient;

/**
 *	CloudSearch client for YCSB
 * @author Chuka Okoye
 *
 */
public class CloudSearchClient extends DB {

	private AmazonCloudSearch configClient = null; //Configuration Service
	private AmazonCloudSearchDomain searchClient = null; //Search and Doc Service
	private AWSCredentials credentials = null; 
	private CloudSearchConfig csConfig;
	
	/**
	 * Are the necessary credentials available in our config file
	 * if yes, instantiate the credentials object.
	 * @return true if credentials were supplied in config file
	 */
	private boolean haveCredentials(){
		if (csConfig.getAccessKeyId() != null && csConfig.getSecretKeyId() != null){
			credentials = new BasicAWSCredentials(csConfig.getAccessKeyId(), csConfig.getSecretKeyId());
			return true;
		}
		else{
			return false;
		}
	}
	
    /**
     * Initialize any state for this DB. Called once per DB instance; there is
     * one DB instance per client thread.
     */
    @Override
    public void init() throws DBException {
    	csConfig = new CloudSearchConfig(getProperties());
    	if (haveCredentials()){ //otherwise assume creds in env vars
    		configClient = new AmazonCloudSearchClient(credentials);
    		searchClient = new AmazonCloudSearchDomainClient(credentials);
    	}
    	else{
    		configClient = new AmazonCloudSearchClient();
    		searchClient = new AmazonCloudSearchDomainClient();
    	}
    	configClient.setEndpoint(csConfig.getCloudSearchEndpoint());
    }

    @Override
    public void cleanup() throws DBException {
        //No cleanup tasks necessary.
    }

    /**
     * Insert a record in the database. Any field/value pairs in the specified
     * values HashMap will be written into the record with the specified record
     * key.
     *
     * @param table The name of the table
     * @param key The record key of the record to insert.
     * @param values A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error. See this class's
     * description for a discussion of error codes.
     */
    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        return 1;
    }

    /**
     * Delete a record from the database.
     *
     * @param table The name of the table
     * @param key The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error. See this class's
     * description for a discussion of error codes.
     */
    @Override
    public int delete(String table, String key) {
        return 1;
    }

    /**
     * Read a record from the database. Each field/value pair from the result
     * will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param key The record key of the record to read.
     * @param fields The list of fields to read, or null for all of them
     * @param result A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    @Override
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        return 1;
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified
     * values HashMap will be written into the record with the specified record
     * key, overwriting any existing values with the same field name.
     *
     * @param table The name of the table
     * @param key The record key of the record to write.
     * @param values A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error. See this class's
     * description for a discussion of error codes.
     */
    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        return 1;
    }

    /**
     * Perform a range scan for a set of records in the database. Each
     * field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param startkey The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param fields The list of fields to read, or null for all of them
     * @param result A Vector of HashMaps, where each HashMap is a set
     * field/value pairs for one record
     * @return Zero on success, a non-zero error code on error. See this class's
     * description for a discussion of error codes.
     */
    @Override
    public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        return 1;
    }

	@Override
	public int read(String table, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int scan(String table, String startkey, int recordcount,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int update(String table, String key,
			HashMap<String, ByteIterator> values) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int insert(String table, String key,
			HashMap<String, ByteIterator> values) {
		// TODO Auto-generated method stub
		return 0;
	}
}
