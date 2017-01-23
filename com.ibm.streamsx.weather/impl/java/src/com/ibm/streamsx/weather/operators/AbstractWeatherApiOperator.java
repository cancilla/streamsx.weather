package com.ibm.streamsx.weather.operators;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPortSet.WindowMode;
import com.ibm.streams.operator.model.InputPortSet.WindowPunctuationInputMode;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streamsx.weather.internal.json.JSONToTupleConverter;
import com.ibm.streamsx.weather.internal.json.ReservedWordNamingStrategy;
import com.ibm.streamsx.weather.internal.json.TupleTypeVerifier;

/**
 * 
 * Abstract implementation of operators calling into the Weather Company Data REST API
 *
 */
@InputPorts({
		@InputPortSet(description = "Port that ingests tuples", cardinality = 1, optional = false, windowingMode = WindowMode.NonWindowed, windowPunctuationInputMode = WindowPunctuationInputMode.Oblivious)})
@OutputPorts({
		@OutputPortSet(description = "Port that produces tuples", cardinality = 1, optional = false, windowPunctuationOutputMode = WindowPunctuationOutputMode.Generating)})
@Libraries({ "lib/*", "opt/downloaded/*" })
public abstract class AbstractWeatherApiOperator extends AbstractOperator {

	protected static final String DEFAULT_TARGET_ATTR_NAME = "jsonString";
	private static final Integer MAX_RETRIES = 10;

	private static final Logger logger = Logger.getLogger(AbstractWeatherApiOperator.class);

	/* Params */
	private String url;
	private String username;
	private String password;
	private String targetAttr;

	protected Type targetAttrType;

	/* Abstract methods */
	protected abstract String getWeatherData(Tuple tuple) throws Exception;

	protected abstract void initWeatherApi(OperatorContext context) throws Exception;

	@Parameter(name = "username", optional = false, description = "Specifies the username needed to access the Weather Company Data REST APIs.")
	public void setUsername(String username) {
		this.username = username;
	}

	@Parameter(name = "password", optional = false, description = "Specifies the password needed to access the Weather Company Data REST APIs.")
	public void setPassword(String password) {
		this.password = password;
	}

	@Parameter(name = "url", optional = true, description = "Specifies the base URL for accessing the Weather Company Data REST APIs. By default, this value is set to 'https://twcservice.mybluemix.net:443/'.")
	public void setUrl(String url) {
		this.url = url;
	}

	@Parameter(name = "targetAttr", optional = true, description = "Specifies the target attribute name that will "
			+ "contain the results of the REST API call. This parameter can either point to a tuple type (see **Output** section for more details) "
			+ " or can point to an attribute named **jsonString** with a type of *rstring*. If pointing to an attribute named **jsonString**, the operator"
			+ " will populate the attribute with the full JSON result as returned by the REST API call. By default, if this parameter is not specified, "
			+ " then the operator will attempt to populate an attribute named **jsonString**.")
	public void setTargetAttr(String targetAttr) {
		this.targetAttr = targetAttr;
	}

	public String getPassword() {
		return password;
	}

	public String getUrl() {
		return url;
	}

	public String getUsername() {
		return username;
	}

	public String getTargetAttr() {
		return targetAttr;
	}

	@ContextCheck(compile = false, runtime = true)
	public static void checkTargetAttr(OperatorContextChecker checker) {
		StreamSchema streamSchema = checker.getOperatorContext().getStreamingOutputs().get(0).getStreamSchema();
		List<String> paramValues = checker.getOperatorContext().getParameterValues("targetAttr");
		if (paramValues.size() == 0) {
			if (streamSchema.getAttribute(DEFAULT_TARGET_ATTR_NAME) == null) {
				checker.setInvalidContext(
						"Either the 'targetAttr' parameter must be set or the output schema must contain an"
								+ " attribute named '" + DEFAULT_TARGET_ATTR_NAME + "'.",
						new Object[0]);
			}
		} else {
			String attrName = paramValues.get(0);
			if (streamSchema.getAttribute(attrName) == null) {
				checker.setInvalidContext("Output schema does not contain an attribute named '" + attrName + "'.",
						new Object[0]);
			}
		}
	}

	@Override
	public synchronized void initialize(OperatorContext context) throws Exception {
		super.initialize(context);
		if (targetAttr == null)
			targetAttr = DEFAULT_TARGET_ATTR_NAME;

		StreamSchema ss = getOutput(0).getStreamSchema();
		targetAttrType = TupleTypeVerifier.verifyAttributeType(ss, targetAttr,
				Arrays.asList(MetaType.RSTRING, MetaType.TUPLE));

		JSONToTupleConverter.setGlobalNamingStrategy(new ReservedWordNamingStrategy());

		initWeatherApi(context);
	}

	/**
	 * Notification that initialization is complete and all input and output
	 * ports are connected and ready to receive and submit tuples.
	 * 
	 * @throws Exception
	 *             Operator failure, will cause the enclosing PE to terminate.
	 */
	@Override
	public synchronized void allPortsReady() throws Exception {
		// This method is commonly used by source operators.
		// Operators that process incoming tuples generally do not need this
		// notification.
		OperatorContext context = getOperatorContext();
		logger.trace("Operator " + context.getName() + " all ports are ready in PE: " + context.getPE().getPEId()
				+ " in Job: " + context.getPE().getJobId());
	}

	/**
	 * Process an incoming tuple that arrived on the specified port.
	 * <P>
	 * Copy the incoming tuple to a new output tuple and submit to the output
	 * port.
	 * </P>
	 * 
	 * @param inputStream
	 *            Port the tuple is arriving on.
	 * @param tuple
	 *            Object representing the incoming tuple.
	 * @throws Exception
	 *             Operator failure, will cause the enclosing PE to terminate.
	 */
	@Override
	public final void process(StreamingInput<Tuple> inputStream, Tuple tuple) throws Exception {

		String weatherData = null;
		int retries = MAX_RETRIES; // TODO
		while (retries-- > 0) {
			try {
				weatherData = getWeatherData(tuple);
				break;
			} catch (Exception e) {
				logger.error(e.getMessage() + " -- RETRIES REMAINING: " + retries, e);
				continue; // retry
			}
		}

		// unable to get weather, dropping tuple
		if (weatherData == null) {
			logger.warn("Unable to retrieve weather...skipping tuple");
			return;
		}

		// Create a new tuple for output port 0
		StreamingOutput<OutputTuple> outStream = getOutput(0);
		OutputTuple outTuple = outStream.newTuple();
		outTuple.assign(tuple);

		if (targetAttrType.getMetaType() == MetaType.RSTRING) {
			outTuple.setString(getTargetAttr(), weatherData);
		} else if (targetAttrType.getMetaType() == MetaType.TUPLE) {
			Tuple jsonTuple = JSONToTupleConverter.jsonToTuple(weatherData,
					outTuple.getTuple(targetAttr).getStreamSchema());
			outTuple.setTuple(targetAttr, jsonTuple);
		} else {
			Exception e = new UnsupportedOperationException(
					String.format("Conversion to %s type net yet supported!", targetAttrType.getMetaType().name()));
			logger.error(e.getLocalizedMessage());
			throw e;
		}
		// Submit new tuple to output port 0
		outStream.submit(outTuple);
	}

	/**
	 * Process an incoming punctuation that arrived on the specified port.
	 * 
	 * @param stream
	 *            Port the punctuation is arriving on.
	 * @param mark
	 *            The punctuation mark
	 * @throws Exception
	 *             Operator failure, will cause the enclosing PE to terminate.
	 */
	@Override
	public void processPunctuation(StreamingInput<Tuple> stream, Punctuation mark) throws Exception {
		// For window markers, punctuate all output ports
		super.processPunctuation(stream, mark);
	}

	/**
	 * Shutdown this operator.
	 * 
	 * @throws Exception
	 *             Operator failure, will cause the enclosing PE to terminate.
	 */
	public synchronized void shutdown() throws Exception {
		OperatorContext context = getOperatorContext();
		Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " shutting down in PE: "
				+ context.getPE().getPEId() + " in Job: " + context.getPE().getJobId());

		// Must call super.shutdown()
		super.shutdown();
	}
}
