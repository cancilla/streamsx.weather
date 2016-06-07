/*******************************************************************************
 * Copyright (C) 2016, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.weather;


import com.ibm.streams.operator.model.PrimitiveOperator;

/**
 * Get the daily forecast condition
 */
@PrimitiveOperator(name="ForecastDaily", namespace="com.ibm.streamsx.weather",
description="Java Operator ForecastDaily")
public class ForecastDailyOperator extends AbstractWeatherOperator {

	@Override
	public String getURLSuffix() {
		return "/forecast/daily/10day";
	}
	
		
}
