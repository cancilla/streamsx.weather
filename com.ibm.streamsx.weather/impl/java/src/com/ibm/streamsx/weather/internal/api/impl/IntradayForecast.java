package com.ibm.streamsx.weather.internal.api.impl;

import com.ibm.streamsx.weather.internal.api.AbstractWeatherApi;
import com.ibm.streamsx.weather.internal.api.Days;
import com.ibm.streamsx.weather.internal.api.Units;

public class IntradayForecast extends AbstractWeatherApi {

	private static final String URL_GEOCODE_SUFFIX = "geocode/%f/%f/forecast/intraday/%sday.json" ;
	private static final String URL_LOCATION_SUFFIX = "location/%s/forecast/intraday/%sday.json";
	
	public IntradayForecast(String url, String username, String password, String language, Units units) {
		super(url, username, password, language, units);
	}

	public IntradayForecast(String username, String password) {
		super(username, password);
	}

	public String getForecast(Double latitude, Double longitude, Days day) throws Exception {
		return execute(String.format(URL_GEOCODE_SUFFIX, latitude, longitude, day.getDays()));
	}

	public String getForecast(String locationId, Days day) throws Exception {
		return execute(String.format(URL_LOCATION_SUFFIX, locationId, day.getDays()));
	}
	
}
