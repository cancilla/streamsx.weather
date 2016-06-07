/*******************************************************************************
 * Copyright (C) 2016, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/

package com.ibm.streamsx.weather;


import com.ibm.streams.operator.model.PrimitiveOperator;

/**
 * Get the hourly forecast condition
 */
@PrimitiveOperator(name="ForecastHourly", namespace="com.ibm.streamsx.weather",
description="Java Operator ForecastHourly")
public class ForecastHourlyOperator extends AbstractWeatherOperator {

	@Override
	public String getURLSuffix() {
		return "/forecast/hourly/24hour";
	}
	
		
}
