package com.ibm.streamsx.weather.operators;

import java.util.List;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streamsx.weather.internal.api.Days;
import com.ibm.streamsx.weather.internal.api.impl.IntradayForecast;

@PrimitiveOperator(name="IntradayForecast", namespace="com.ibm.streamsx.weather")
public class IntradayForecastOp extends AbstractForecastOperator {

	private static final Days DEFAULT_FORECAST_DAYS = Days.THREE_DAY;

	private IntradayForecast forecast;

	/* Params */
	private Integer forecastDays = DEFAULT_FORECAST_DAYS.getDays();

	@Parameter(name = "forecastDays", optional = true)
	public void setForecastDays(Integer forecastDays) {
		this.forecastDays = forecastDays;
	}

	public Integer getForecastDays() {
		return forecastDays;
	}

	@ContextCheck(runtime = true, compile = false)
	public static void checkNumDaysParamValue(OperatorContextChecker checker) {
		List<String> paramValues = checker.getOperatorContext().getParameterValues("forecastDays");
		if(paramValues.size() > 0) {
			if(Days.findDays(Integer.valueOf(paramValues.get(0))) == null) {
				checker.setInvalidContext("Invalid value for 'forecastDays' param: " + paramValues.get(0), new Object[0]);
			}
		}
	}
	
	@Override
	protected String getWeatherData(Tuple tuple) throws Exception {
		if (getLocationIdAttr() != null || tuple.getStreamSchema().getAttribute(DEFAULT_LOCATIONID_ATTR_NAME) != null) {
			String locationId = getLocationIdAttr() == null ? tuple.getString(DEFAULT_LOCATIONID_ATTR_NAME)
					: getLocationIdAttr().getValue(tuple);
			return forecast.getForecast(locationId, Days.findDays(getForecastDays()));
		} else {
			return forecast.getForecast(getLatitudeValue(tuple), getLongitudeValue(tuple),
					Days.findDays(getForecastDays()));
		}
	}

	@Override
	protected void initWeatherApi(OperatorContext context) throws Exception {
		forecast = new IntradayForecast(getUsername(), getPassword());
	}
	
}
