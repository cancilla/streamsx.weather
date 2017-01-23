package com.ibm.streamsx.weather.internal.api.impl;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;

import com.ibm.streamsx.weather.internal.api.AbstractWeatherApi;
import com.ibm.streamsx.weather.internal.api.Units;

public class LocationServices extends AbstractWeatherApi {

	public static enum LocationLookupType {
		Search,
		Geocode,
		PostalKey,
		IATACode,
		ICAOCode;		
	}
	
	
	private static final String LOCATION_SERVICES_BASE_API_PATH = "api/weather/v3/";
	private static final String URL_SEARCH_SUFFIX = "location/search";
	private static final String URL_POINT_SUFFIX = "location/point";
	
	
	public LocationServices(String url, String username, String password, String language) {
		super(url, username, password, language, Units.NONE);
	}

	public LocationServices(String username, String password) {
		super(DEFAULT_BASE_URL, username, password, DEFAULT_LANGUAGE, Units.NONE);
	}

	public String search(String query, String locationType, String countryCode, String adminDistrictCode) throws Exception {
		List<NameValuePair> parameters = new ArrayList<NameValuePair>();
		parameters.add(new BasicNameValuePair("query", URLEncoder.encode(query, StandardCharsets.UTF_8.displayName())));
		
		if(locationType != null)
			parameters.add(new BasicNameValuePair("locationType", URLEncoder.encode(locationType, StandardCharsets.UTF_8.displayName())));
		
		if(countryCode != null)
			parameters.add(new BasicNameValuePair("countryCode", URLEncoder.encode(countryCode, StandardCharsets.UTF_8.displayName())));
		
		if(adminDistrictCode != null)
			parameters.add(new BasicNameValuePair("adminDistrictCode", URLEncoder.encode(adminDistrictCode, StandardCharsets.UTF_8.displayName())));
		
		return execute(URL_SEARCH_SUFFIX, parameters);
	}
	
	public String getPointFromGeocode(Double latitude, Double longitude) throws Exception {
		List<NameValuePair> parameters = new ArrayList<NameValuePair>();
		parameters.add(new BasicNameValuePair("geocode", String.valueOf(latitude) + "," + String.valueOf(longitude)));
		
		return execute(URL_POINT_SUFFIX, parameters);
	}
	
	public String getPointFromPostalKey(String postalKey) throws Exception {
		List<NameValuePair> parameters = new ArrayList<NameValuePair>();
		parameters.add(new BasicNameValuePair("postalKey", URLEncoder.encode(postalKey, StandardCharsets.UTF_8.displayName())));
		
		return execute(URL_POINT_SUFFIX, parameters);
	}

	public String getPointFromIataCode(String iataCode) throws Exception {
		List<NameValuePair> parameters = new ArrayList<NameValuePair>();
		parameters.add(new BasicNameValuePair("iataCode", URLEncoder.encode(iataCode, StandardCharsets.UTF_8.displayName())));
		
		return execute(URL_POINT_SUFFIX, parameters);
	}
	
	public String getPointFromIcao(String icaoCode) throws Exception {
		List<NameValuePair> parameters = new ArrayList<NameValuePair>();
		parameters.add(new BasicNameValuePair("icaoCode", URLEncoder.encode(icaoCode, StandardCharsets.UTF_8.displayName())));
		
		return execute(URL_POINT_SUFFIX, parameters);
	}
	
	@Override
	protected String getBaseApiPath() {
		return LOCATION_SERVICES_BASE_API_PATH;
	}

}
