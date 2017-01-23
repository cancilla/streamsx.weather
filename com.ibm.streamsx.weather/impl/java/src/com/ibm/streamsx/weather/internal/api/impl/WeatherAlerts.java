package com.ibm.streamsx.weather.internal.api.impl;

import com.ibm.streamsx.weather.internal.api.AbstractWeatherApi;
import com.ibm.streamsx.weather.internal.api.Units;

public class WeatherAlerts extends AbstractWeatherApi {

	private static final String URL_DETAILS_SUFFIX = "alert/%s/details.json";
	private static final String URL_ALERTS_LATLONG = "geocode/%f/%f/alerts.json";
	private static final String URL_ALERTS_COUNTRY = "country/%s/alerts.json";
	private static final String URL_ALERTS_COUNTRY_STATE = "country/%s/state/%s/alerts.json";
	private static final String URL_ALERTS_COUNTRY_AREAID = "country/%s/area/%s/alerts.json";
	
	
	public WeatherAlerts(String url, String username, String password, String language, Units units) throws Exception {
		super(url, username, password, language, units);
	}

	public WeatherAlerts(String username, String password) throws Exception {
		super(username, password);
	}

	/*
	 * Calls the /v1/alert/{default_key}/defaults.json API
	 */
	public String getDetails(String detailKey) throws Exception {
		return execute(String.format(URL_DETAILS_SUFFIX, detailKey));
	}
	
	/*
	 * Calls the /v1/geocode/{latitude}/{longitude}/alerts.json API
	 */
	public String getAlertsByGeocode(Double latitude, Double longitude) throws Exception {
		return execute(String.format(URL_ALERTS_LATLONG, latitude, longitude));
	}
	
	/*
	 * Calls the /v1/country/{countryCode}/alerts.json API
	 */
	public String getAlertsByCountry(String countryCode) throws Exception {
		return execute(String.format(URL_ALERTS_COUNTRY, countryCode));
	}
	
	/*
	 * Calls the /v1/country/{countryCode}/state/{stateCode}/alerts.json API
	 */
	public String getAlertsByCountryAndState(String countryCode, String stateCode) throws Exception {
		return execute(String.format(URL_ALERTS_COUNTRY_STATE, countryCode, stateCode));
	}

	/*
	 * Calls the /v1/country/{countryCode}/area/{areaid}/alerts.json API
	 */
	public String getAlertsByCountryAndArea(String countryCode, String areaId) throws Exception {
		return execute(String.format(URL_ALERTS_COUNTRY_AREAID, countryCode, areaId));
	}
}
