package com.ibm.streamsx.weather.internal.api.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;

import com.ibm.streamsx.weather.internal.api.AbstractWeatherApi;
import com.ibm.streamsx.weather.internal.api.Units;

public class HistoricalData extends AbstractWeatherApi {

	private static final String URL_GEOCODE_SUFFIX = "geocode/%f/%f/observations/timeseries.json";
	private static final String URL_LOCATION_SUFFIX = "location/%s/observations/timeseries.json";
	
	public HistoricalData(String url, String username, String password, String language, Units units) {
		super(url, username, password, language, units);
	}

	public HistoricalData(String username, String password) {
		super(username, password);
	}

	public String getForecast(Double latitude, Double longitude, Integer hours) throws Exception {
		List<NameValuePair> parameters = new ArrayList<NameValuePair>();
		parameters.add(new BasicNameValuePair("hours", hours.toString()));
		return execute(String.format(URL_GEOCODE_SUFFIX, latitude, longitude), parameters);
	}

	public String getForecast(String locationId, Integer hours) throws Exception {
		List<NameValuePair> parameters = new ArrayList<NameValuePair>();
		parameters.add(new BasicNameValuePair("hours", hours.toString()));
		return execute(String.format(URL_LOCATION_SUFFIX, locationId), parameters);
	}
	
}
