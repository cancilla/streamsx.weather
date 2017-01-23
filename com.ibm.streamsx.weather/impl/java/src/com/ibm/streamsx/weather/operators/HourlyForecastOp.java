package com.ibm.streamsx.weather.operators;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streamsx.weather.internal.api.impl.HourlyForecast;

@PrimitiveOperator(name = "HourlyForecast", namespace="com.ibm.streamsx.weather")
public class HourlyForecastOp extends AbstractForecastOperator {

	private HourlyForecast forecast;
	
	@Override
	protected String getWeatherData(Tuple tuple) throws Exception {
		if (getLocationIdAttr() != null || tuple.getStreamSchema().getAttribute(DEFAULT_LOCATIONID_ATTR_NAME) != null) {
			String locationId = getLocationIdAttr() == null ? tuple.getString(DEFAULT_LOCATIONID_ATTR_NAME)
					: getLocationIdAttr().getValue(tuple);
			return forecast.getForecast(locationId);
		} else {
			return forecast.getForecast(getLatitudeValue(tuple), getLongitudeValue(tuple));
		}
	}

	@Override
	protected void initWeatherApi(OperatorContext context) throws Exception {
		forecast = new HourlyForecast(getUsername(), getPassword());
	}
}
