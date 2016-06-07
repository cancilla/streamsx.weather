/*******************************************************************************
 * Copyright (C) 2016, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.weather;


import com.ibm.streams.operator.model.PrimitiveOperator;

/**
 * Get weather condition from the last 24 hours.
 */
@PrimitiveOperator(name="HistoricalWeather", namespace="com.ibm.streamsx.weather",
description="Java Operator HistoricalWeather")
public class HistoricalWeather extends AbstractWeatherOperator {

	@Override
	public String getURLSuffix() {
		return "/observations/timeseries/24hour";
	}
	
		
}
