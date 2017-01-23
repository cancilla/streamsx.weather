package com.ibm.streamsx.weather.internal.api.impl;

import java.net.MalformedURLException;

import com.ibm.streamsx.weather.internal.api.AbstractWeatherApi;
import com.ibm.streamsx.weather.internal.api.Days;
import com.ibm.streamsx.weather.internal.api.Units;

public class DailyForecast extends AbstractWeatherApi {

	private static final String URL_GEOCODE_SUFFIX = "geocode/%f/%f/forecast/daily/%sday.json" ;
	private static final String URL_LOCATION_SUFFIX = "location/%s/forecast/daily/%sday.json";
	
	public DailyForecast(String username, String password) throws MalformedURLException {
		super(username, password);
	}

	public DailyForecast(String url, String username, String password, String language, Units units)
			throws MalformedURLException {
		super(url, username, password, language, units);
	}

	public String getForecast(Double latitude, Double longitude, Days day) throws Exception {
		return execute(String.format(URL_GEOCODE_SUFFIX, latitude, longitude, day.getDays()));
	}

	public String getForecast(String locationId, Days day) throws Exception {
		return execute(String.format(URL_LOCATION_SUFFIX, locationId, day.getDays()));
	}
}
