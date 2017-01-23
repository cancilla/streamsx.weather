package com.ibm.streamsx.weather.operators;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streamsx.weather.internal.api.impl.HistoricalData;

@PrimitiveOperator(name="HistoricalData", namespace="com.ibm.streamsx.weather")
public class HistoricalDataOp extends AbstractForecastOperator {

	private HistoricalData historicalData;

	/* Params */
	private Integer hours;;
	
	@Parameter(name = "hours", optional = false)
	public void setHours(Integer hours) {
		this.hours = hours;
	}
	
	public Integer getHours() {
		return hours;
	}
	
	@Override
	protected String getWeatherData(Tuple tuple) throws Exception {
		if (getLocationIdAttr() != null || tuple.getStreamSchema().getAttribute(DEFAULT_LOCATIONID_ATTR_NAME) != null) {
			String locationId = getLocationIdAttr() == null ? tuple.getString(DEFAULT_LOCATIONID_ATTR_NAME)
					: getLocationIdAttr().getValue(tuple);
			return historicalData.getForecast(locationId, getHours());
		} else {
			return historicalData.getForecast(getLatitudeValue(tuple), getLongitudeValue(tuple), getHours());
		}
	}

	@Override
	protected void initWeatherApi(OperatorContext context) throws Exception {
		historicalData = new HistoricalData(getUsername(), getPassword());
	}

}
