package com.ibm.streamsx.weather.operators;

import java.util.List;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streamsx.weather.internal.api.impl.AlmanacServices;
import com.ibm.streamsx.weather.internal.api.impl.AlmanacServices.AlmanacFrequency;

@PrimitiveOperator(name = "AlmanacServices", namespace="com.ibm.streamsx.weather")
public class AlmanacServicesOp extends AbstractForecastOperator {

	private static final AlmanacFrequency DEFAULT_FREQUENCY = AlmanacFrequency.Daily;

	private AlmanacServices almanac;

	/* Params */
	private AlmanacFrequency frequency = DEFAULT_FREQUENCY;
	private String start;
	private String end;
		
	@Parameter(name = "frequency", optional = true)
	public void setFrequency(AlmanacFrequency frequency) {
		this.frequency = frequency;
	}
	
	public AlmanacFrequency getFrequency() {
		return frequency;
	}

	@Parameter(name = "start", optional = false)
	public void setStart(String start) {
		this.start = start;
	}
	
	public String getStart() {
		return start;
	}

	@Parameter(name = "end", optional = true)
	public void setEnd(String end) {
		this.end = end;
	}
	
	public String getEnd() {
		return end;
	}
	
	@ContextCheck(runtime = true, compile = false)
	public static void checkFrequency(OperatorContextChecker checker) {
		List<String> paramValues = checker.getOperatorContext().getParameterValues("frequency");
		if(paramValues.size() > 0) {
			if(AlmanacFrequency.valueOf(paramValues.get(0)) == null) {
				checker.setInvalidContext("Invalid value for 'frequency' parameter: " + paramValues.get(0), new Object[0]);
			}
		}
	}
	
	@Override
	protected String getWeatherData(Tuple tuple) throws Exception {
		if (getLocationIdAttr() != null || tuple.getStreamSchema().getAttribute(DEFAULT_LOCATIONID_ATTR_NAME) != null) {
			String locationId = getLocationIdAttr() == null ? tuple.getString(DEFAULT_LOCATIONID_ATTR_NAME)
					: getLocationIdAttr().getValue(tuple);
			return almanac.getForecast(locationId, getFrequency(), getStart(), getEnd());
		} else {
			return almanac.getForecast(getLatitudeValue(tuple), getLongitudeValue(tuple),
					getFrequency(), getStart(), getEnd());
		}
	}
	
	@Override
	protected void initWeatherApi(OperatorContext context) throws Exception {
		almanac = new AlmanacServices(getUsername(), getPassword());
	}
}
