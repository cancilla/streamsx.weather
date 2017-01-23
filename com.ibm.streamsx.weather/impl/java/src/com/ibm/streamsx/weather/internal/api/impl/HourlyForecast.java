package com.ibm.streamsx.weather.internal.api.impl;

import com.ibm.streamsx.weather.internal.api.AbstractWeatherApi;
import com.ibm.streamsx.weather.internal.api.Units;

public class HourlyForecast extends AbstractWeatherApi {

	private static final String URL_GEOCODE_SUFFIX = "geocode/%f/%f/forecast/hourly/48hour.json" ;
	private static final String URL_LOCATION_SUFFIX = "location/%s/forecast/hourly/48hour.json";
	
	public HourlyForecast(String username, String password) {
		super(username, password);
	}

	public HourlyForecast(String url, String username, String password, String language, Units units) {
		super(url, username, password, language, units);
	}

	public String getForecast(Double latitude, Double longitude) throws Exception {
		return execute(String.format(URL_GEOCODE_SUFFIX, latitude, longitude));
	}

	public String getForecast(String locationId) throws Exception {
		return execute(String.format(URL_LOCATION_SUFFIX, locationId));
	}
}
