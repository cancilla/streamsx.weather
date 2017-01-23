package com.ibm.streamsx.weather.internal.api.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;

import com.ibm.streamsx.weather.internal.api.AbstractWeatherApi;
import com.ibm.streamsx.weather.internal.api.Units;

public class AlmanacServices extends AbstractWeatherApi {

	private static final String URL_GEOCODE_SUFFIX = "geocode/%f/%f/almanac/%s.json" ;
	private static final String URL_LOCATION_SUFFIX = "location/%s/almanac/%s.json";
	
	public static enum AlmanacFrequency {
		Daily,
		Monthly;
	}
	
	public AlmanacServices(String url, String username, String password, String language, Units units) {
		super(url, username, password, language, units);
	}

	public AlmanacServices(String username, String password) {
		super(username, password);
	}

	public String getForecast(Double latitude, Double longitude, AlmanacFrequency frequency, String start, String end) throws Exception {
		List<NameValuePair> parameters = new ArrayList<NameValuePair>();
		parameters.add(new BasicNameValuePair("start", start));
		if(end != null)
			parameters.add(new BasicNameValuePair("end", end));
		
		return execute(String.format(URL_GEOCODE_SUFFIX, latitude, longitude, frequency.name().toLowerCase()), parameters);
	}
	
	public String getForecast(String locationId, AlmanacFrequency frequency, String start, String end) throws Exception {
		List<NameValuePair> parameters = new ArrayList<NameValuePair>();
		parameters.add(new BasicNameValuePair("start", start));
		if(end != null)
			parameters.add(new BasicNameValuePair("end", end));
		
		return execute(String.format(URL_LOCATION_SUFFIX, locationId, frequency.name().toLowerCase()), parameters);
	}
}
