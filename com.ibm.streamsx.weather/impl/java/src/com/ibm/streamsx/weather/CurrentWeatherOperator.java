/*******************************************************************************
 * Copyright (C) 2016, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/
package com.ibm.streamsx.weather;


import com.ibm.streams.operator.model.PrimitiveOperator;

/**
 * 
 * Get the current weather condition
 *
 */
@PrimitiveOperator(name="CurrentWeather", namespace="com.ibm.streamsx.weather",
description="Java Operator CurrentWeather")
public class CurrentWeatherOperator extends AbstractWeatherOperator {

	@Override
	public String getURLSuffix() {
		return "/observations/current";
	}
	
		
}
