/*******************************************************************************
 * Copyright (C) 2016, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/

package com.ibm.streamsx.weather;

import java.io.IOException;
import java.util.Arrays;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Form;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.log4j.Logger;

import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;
import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.TupleAttribute;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.meta.TupleType;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPortSet.WindowMode;
import com.ibm.streams.operator.model.InputPortSet.WindowPunctuationInputMode;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streamsx.json.converters.JSONToTupleConverter;
import com.ibm.streamsx.json.converters.TupleTypeVerifier;


/**
 * 
 * Abstract implementation of operators calling into the Watson Weather API
 *
 */
@InputPorts({@InputPortSet(description="Port that ingests tuples", cardinality=1, optional=false, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious), @InputPortSet(description="Optional input ports", optional=true, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious)})
@OutputPorts({@OutputPortSet(description="Port that produces tuples", cardinality=1, optional=false, windowPunctuationOutputMode=WindowPunctuationOutputMode.Generating), @OutputPortSet(description="Optional output ports", optional=true, windowPunctuationOutputMode=WindowPunctuationOutputMode.Generating)})
@Libraries({"lib/*", "opt/downloaded/*"})
public abstract class AbstractWeatherOperator extends AbstractOperator {

	private static final Logger logger = Logger.getLogger(AbstractWeatherOperator.class);

	private static final Integer MAX_RETRIES = 10;
	
	private Type targetAttrType;
	
	/* Parameters */
	private String url;
	private String username;
	private String password;
	private String units;
	private TupleAttribute<Tuple, Double> latitidue;
	private TupleAttribute<Tuple, Double> longitude;
	private String targetAttribute;
	
	public abstract String getURLSuffix();
	
	@Parameter(name="username")
	public void setUsername(String username) {
		this.username = username;
	}
	
	@Parameter(name="password")
	public void setPassword(String password) {
		this.password = password;
	}
	
	@Parameter(name="url")
	public void setUrl(String url) {
		this.url = url;
	}
	
	@Parameter(name="units")
	public void setUnit(String units) {
		this.units = units;
	}
	
	@Parameter(name="latitidue")
	public void setLatitidue(TupleAttribute<Tuple, Double> latitidue) {
		this.latitidue = latitidue;
	}
	
	@Parameter(name="longitude")
	public void setLongitude(TupleAttribute<Tuple, Double> longitude) {
		this.longitude = longitude;
	}
	
	@Parameter(name="targetAttribute")
	public void setTargetAttribute(String targetAttribute) {
		this.targetAttribute = targetAttribute;
	}
	
    /**
     * Initialize this operator. Called once before any tuples are processed.
     * @param context OperatorContext for this operator.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
	@Override
	public synchronized void initialize(OperatorContext context)
			throws Exception {
    	// Must call super.initialize(context) to correctly setup an operator.
		super.initialize(context);
        Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " initializing in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );
        
        StreamSchema ss = getOutput(0).getStreamSchema();
        targetAttrType = TupleTypeVerifier.verifyAttributeType(ss, targetAttribute, 
    			Arrays.asList(MetaType.TUPLE, MetaType.LIST, MetaType.BLIST, MetaType.SET, MetaType.BSET));
        
        
	}

    /**
     * Notification that initialization is complete and all input and output ports 
     * are connected and ready to receive and submit tuples.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public synchronized void allPortsReady() throws Exception {
    	// This method is commonly used by source operators. 
    	// Operators that process incoming tuples generally do not need this notification. 
        OperatorContext context = getOperatorContext();
        Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " all ports are ready in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );
    }

    /**
     * Process an incoming tuple that arrived on the specified port.
     * <P>
     * Copy the incoming tuple to a new output tuple and submit to the output port. 
     * </P>
     * @param inputStream Port the tuple is arriving on.
     * @param tuple Object representing the incoming tuple.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public final void process(StreamingInput<Tuple> inputStream, Tuple tuple)
            throws Exception {

    	Double lat = latitidue.getValue(tuple);
    	Double lng = longitude.getValue(tuple);
    	String jsonInput = null;
    	
    	int retries = MAX_RETRIES ; // TODO
    	while(retries-- > 0) {
    		try {
        		jsonInput = getWeather(lat, lng);
        		break;
        	} catch(Exception e) {
        		logger.error(e.getMessage() + " -- RETRIES REMAINING: " + retries, e);
        		continue; // retry
        	}
    	}
    	
    	// unable to get weather, dropping tuple
    	if(jsonInput == null)
    		return;
    	
    	// Create a new tuple for output port 0
        StreamingOutput<OutputTuple> outStream = getOutput(0);
        OutputTuple outTuple = outStream.newTuple();
        
		try {
			// parse the JSON output and return the results as a tuple
			if(targetAttrType.getMetaType() != MetaType.TUPLE) {
				//in this mode, the incoming json string is expected to be an array
				JSONArray jsonArr = JSONArray.parse(jsonInput);
				Object collectionObj = JSONToTupleConverter.jsonToAttribute(targetAttribute, targetAttrType, jsonArr, null);
				if(collectionObj != null)
					outTuple.setObject(targetAttribute, collectionObj);
			}
			else {
				JSONObject jsonObj = JSONObject.parse(jsonInput);
				Tuple tup = JSONToTupleConverter.jsonToTuple(jsonObj, ((TupleType)targetAttrType).getTupleSchema());
				if(tup!=null)
					outTuple.setTuple(targetAttribute, tup);
			}
		} catch (Exception e) {
			logger.error("Error Converting String: " + jsonInput, e);
			e.printStackTrace();
		}
        
        // Copy across all matching attributes.
        outTuple.assign(tuple);

        // Submit new tuple to output port 0
        outStream.submit(outTuple);
    }
    
    private String getWeather(Double lat, Double lng) throws ClientProtocolException, IOException {
    	Form form = Form.form();
    	form.add("geocode", lat + "," + lng);
    	form.add("units", units);
    	form.add("language", "en-US");
    	
    	Executor executor = Executor.newInstance()
    			.auth(username, password);
    	
    	StringBuilder sb = new StringBuilder();
    	sb.append(url);
    	sb.append("/api/weather/v2/");
    	sb.append(getURLSuffix());
    	sb.append("?");
    	sb.append("units=" + units);
    	sb.append("&geocode=" + lat + "," + lng);
    	sb.append("&language=en-US");
    	
    	Request request = Request.Get(sb.toString());
    	Response response = executor.execute(request);
    	
    	return response.returnContent().asString();
	}

	/**
     * Process an incoming punctuation that arrived on the specified port.
     * @param stream Port the punctuation is arriving on.
     * @param mark The punctuation mark
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public void processPunctuation(StreamingInput<Tuple> stream,
    		Punctuation mark) throws Exception {
    	// For window markers, punctuate all output ports 
    	super.processPunctuation(stream, mark);
    }

    /**
     * Shutdown this operator.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    public synchronized void shutdown() throws Exception {
        OperatorContext context = getOperatorContext();
        Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " shutting down in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );

        // Must call super.shutdown()
        super.shutdown();
    }	
	
}
