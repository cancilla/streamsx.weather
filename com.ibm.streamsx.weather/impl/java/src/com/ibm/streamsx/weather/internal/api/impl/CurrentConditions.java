package com.ibm.streamsx.weather.internal.api.impl;

import com.ibm.streamsx.weather.internal.api.AbstractWeatherApi;
import com.ibm.streamsx.weather.internal.api.Units;

public class CurrentConditions extends AbstractWeatherApi {

	private static final String URL_GEOCODE_SUFFIX = "geocode/%f/%f/observations.json" ;
	private static final String URL_LOCATION_SUFFIX = "location/%s/observations.json";
	
	public CurrentConditions(String url, String username, String password, String language, Units units) {
		super(url, username, password, language, units);
	}

	public CurrentConditions(String username, String password) {
		super(username, password);
	}

	public String getForecast(Double latitude, Double longitude) throws Exception {
		return execute(String.format(URL_GEOCODE_SUFFIX, latitude, longitude));
	}

	public String getForecast(String locationId) throws Exception {
		return execute(String.format(URL_LOCATION_SUFFIX, locationId));
	}
}
