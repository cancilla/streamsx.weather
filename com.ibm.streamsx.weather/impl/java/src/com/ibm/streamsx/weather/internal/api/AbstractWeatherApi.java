package com.ibm.streamsx.weather.internal.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

public abstract class AbstractWeatherApi {

	private static final Logger logger = Logger.getLogger(AbstractWeatherApi.class);
	
	public static final String DEFAULT_BASE_URL = "https://twcservice.mybluemix.net:443/" ;
	public static final String DEFAULT_LANGUAGE = "en-US";
	public static final Units DEFAULT_UNITS = Units.M;
	public static final String DEFAULT_BASE_API_PATH = "api/weather/v1/";
	
	protected String url;
	private Executor executor;
	private String urlParamsStr;
	
	public AbstractWeatherApi(String username, String password) {
		this(DEFAULT_BASE_URL, username, password, DEFAULT_LANGUAGE, DEFAULT_UNITS);
	}
		
	public AbstractWeatherApi(String url, String username, String password, String language, Units units) {
		this.url = url;

		this.executor = Executor.newInstance()
				.auth(username, password);
		
		this.urlParamsStr = "?language=" + language;
		if(units != Units.NONE)
			this.urlParamsStr += "&units=" + units.toString();
	}

	protected String getUrl() {
		return this.url;
	}
	
	protected String getBaseApiPath() {
		return DEFAULT_BASE_API_PATH;
	}
	
	protected String execute(String apiPath) throws Exception {
		return execute(apiPath, new ArrayList<NameValuePair>());
	}
	
	protected String execute(String apiPath, List<NameValuePair> parameters) throws ClientProtocolException, IOException {
		String restUrl = getUrl() + getBaseApiPath() + apiPath + urlParamsStr;
		
		for(NameValuePair nvp : parameters) {
			restUrl += "&" + nvp.getName() + "=" + nvp.getValue();			
		}
		
		Request req = Request.Get(restUrl);
		Response resp = this.executor.execute(req);
		logger.debug("url=" + restUrl);
		
		HttpResponse httpResp = resp.returnResponse();
		StatusLine status = httpResp.getStatusLine();
		
		if(status.getStatusCode() != 200) {
			logger.warn("status=" + status.getStatusCode() + ": " + status.getReasonPhrase());
			return null;
		}

		return EntityUtils.toString(httpResp.getEntity());
	}
	
}
