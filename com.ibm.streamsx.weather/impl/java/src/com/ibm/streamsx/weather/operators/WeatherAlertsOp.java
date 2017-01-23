package com.ibm.streamsx.weather.operators;

import java.util.Set;

import org.apache.log4j.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.TupleAttribute;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streamsx.weather.internal.api.impl.WeatherAlerts;

@PrimitiveOperator(name = "WeatherAlerts", namespace = "com.ibm.streamsx.weather", description="Returns weather alerts.")
public class WeatherAlertsOp extends AbstractWeatherLocationOperator {

	public enum Type {
		Details,
		GeocodeAlert,
		CountryAlert,
		CountryStateAlert,
		CountryAreaAlert;
	}
	
//	/*
//	 * Supported API Types
//	 */
//	private static final String DETAILS = "details";
//	private static final String LAT_LON_ALERT = "lat_lon_alert";
//	private static final String COUNTRY_ALERT = "country_alert";
//	private static final String COUNTRY_STATE_ALERT = "country_state_alert";
//	private static final String COUNTRY_AREA_ALERT = "country_area_alert";

	public static final Type DEFAULT_API_TYPE = Type.CountryAlert;
//	public static final String DEFAULT_API_TYPE = COUNTRY_ALERT;

	/*
	 * Default attribute names
	 */
	private static final String DEFAULT_DETAILKEY_ATTR_NAME = "detailKey";
	private static final String DEFAULT_COUNTRY_ATTR_NAME = "countryCode";
	private static final String DEFAULT_STATE_ATTR_NAME = "stateCode";
	private static final String DEFAULT_AREA_ATTR_NAME = "areaId";

	private static final Logger logger = Logger.getLogger(AbstractWeatherLocationOperator.class);
	private WeatherAlerts weather;

	/* Params */
	private TupleAttribute<Tuple, String> detailKeyAttr;
	private TupleAttribute<Tuple, String> countryCodeAttr;
	private TupleAttribute<Tuple, String> stateCodeAttr;
	private TupleAttribute<Tuple, String> areaIdAttr;
	private Type apiType = DEFAULT_API_TYPE;

	@Parameter(name = "apiType", optional = true)
	public void setApiType(Type apiType) {
		this.apiType = apiType;
	}

	@Parameter(name = "detailKeyAttr", optional = true)
	public void setDetailKeyAttr(TupleAttribute<Tuple, String> detailKeyAttr) {
		this.detailKeyAttr = detailKeyAttr;
	}

	@Parameter(name = "countryCodeAttr", optional = true)
	public void setCountryCode(TupleAttribute<Tuple, String> countryCode) {
		this.countryCodeAttr = countryCode;
	}

	@Parameter(name = "stateCodeAttr", optional = true)
	public void setStateCode(TupleAttribute<Tuple, String> stateCode) {
		this.stateCodeAttr = stateCode;
	}

	@Parameter(name = "areaIdAttr", optional = true)
	public void setAreaId(TupleAttribute<Tuple, String> areaId) {
		this.areaIdAttr = areaId;
	}

	public Type getApiType() {
		return apiType;
	}

	public TupleAttribute<Tuple, String> getDetailKeyAttr() {
		return detailKeyAttr;
	}

	public TupleAttribute<Tuple, String> getCountryCodeAttr() {
		return countryCodeAttr;
	}

	public TupleAttribute<Tuple, String> getStateCodeAttr() {
		return stateCodeAttr;
	}

	public TupleAttribute<Tuple, String> getAreaIdAttr() {
		return areaIdAttr;
	}

	@ContextCheck(compile = false, runtime = true)
	public static void checkParameters(OperatorContextChecker checker) {
		Set<String> params = checker.getOperatorContext().getParameterNames();
		Type apiType = (params.contains("apiType"))
				? Type.valueOf(checker.getOperatorContext().getParameterValues("apiType").get(0)) : DEFAULT_API_TYPE;
		StreamingInput<Tuple> inputPort0 = checker.getOperatorContext().getStreamingInputs().get(0);

		switch (apiType) {
		case Details:
			if (!params.contains("detailKeyAttr")
					&& !checker.checkRequiredAttributes(inputPort0, DEFAULT_DETAILKEY_ATTR_NAME)) {
				checker.setInvalidContext(
						"When using the \"Details\" API, either the 'detailKeyAttr' parameter must be set "
								+ "or an attribute named 'detailKey' must exist on the input port.",
						new Object[0]);
			}
			break;
		case CountryAlert:
			if (!params.contains("countryCodeAttr")
					&& !checker.checkRequiredAttributes(inputPort0, DEFAULT_COUNTRY_ATTR_NAME)) {
				checker.setInvalidContext(
						"When using the \"CountryAlert\" API, either the 'countryCodeAttr' parameter must be set "
								+ "or an attribute named 'countryCode' must exist on the input port.",
						new Object[0]);
			}
			break;
		case CountryStateAlert:
			if ((!params.contains("countryCodeAttr")
					&& !checker.checkRequiredAttributes(inputPort0, DEFAULT_COUNTRY_ATTR_NAME))
					|| !params.contains("stateCodeAttr")
							&& !checker.checkRequiredAttributes(inputPort0, DEFAULT_STATE_ATTR_NAME)) {
				checker.setInvalidContext(
						"When using the \"CountryStateAlert\" API, either the 'countryCodeAttr' and 'stateCodeAttr' parameters must be set, "
								+ "or attributes named 'countryCode' and 'stateCode' must exist on the input port.",
						new Object[0]);
			}
			break;
		case CountryAreaAlert:
			if ((!params.contains("countryAttr")
					&& !checker.checkRequiredAttributes(inputPort0, DEFAULT_COUNTRY_ATTR_NAME))
					|| !params.contains("areaAttr")
							&& !checker.checkRequiredAttributes(inputPort0, DEFAULT_AREA_ATTR_NAME)) {
				checker.setInvalidContext(
						"When using the \"CountryAreaAlert\" API, either the 'countryCodeAttr' and 'areaIdAttr' parameters must be set "
								+ "or attributes named 'countryCode' and 'areaId' must exist on the input port.",
						new Object[0]);
			}
			break;
		case GeocodeAlert:
			if(!isLatLonPresent(checker)) {
				checker.setInvalidContext(
						"When using the \"GeocodeAlert\" API, either the 'latitudeAttr' and 'longitudeAttr' parameters must be set, "
								+ "or attributes named 'latitude' and 'longitude' must exist on the input port.",
						new Object[0]);
			}
			break;
		default:
			checker.setInvalidContext("Unknown apiType: " + apiType, new Object[0]);
		}
	}

	@Override
	protected void initWeatherApi(OperatorContext context) throws Exception {
		weather = new WeatherAlerts(getUsername(), getPassword());
	}

	@Override
	protected String getWeatherData(Tuple tuple) throws Exception {
		Type apiType = getApiType();
		String weatherData = null;

		switch (apiType) {
		case Details:
		{
			String detailKey = detailKeyAttr == null ? tuple.getString(DEFAULT_DETAILKEY_ATTR_NAME) : getDetailKeyAttr().getValue(tuple); 
			weatherData = weather.getDetails(detailKey);
			break;
		}
		case CountryAlert:
		{
			String countryCode = countryCodeAttr == null ? tuple.getString(DEFAULT_COUNTRY_ATTR_NAME) : getCountryCodeAttr().getValue(tuple);
			weatherData = weather.getAlertsByCountry(countryCode);
			break;
		}
		case CountryStateAlert:
		{
			String countryCode = countryCodeAttr == null ? tuple.getString(DEFAULT_COUNTRY_ATTR_NAME) : getCountryCodeAttr().getValue(tuple);
			String stateCode = stateCodeAttr == null ? tuple.getString(DEFAULT_STATE_ATTR_NAME) : getStateCodeAttr().getValue(tuple);
			weatherData = weather.getAlertsByCountryAndState(countryCode, stateCode);
			break;
		}
		case CountryAreaAlert:
		{
			String countryCode = countryCodeAttr == null ? tuple.getString(DEFAULT_COUNTRY_ATTR_NAME) : getCountryCodeAttr().getValue(tuple);
			String areaId = areaIdAttr == null ? tuple.getString(DEFAULT_AREA_ATTR_NAME) : getAreaIdAttr().getValue(tuple);
			weatherData = weather.getAlertsByCountryAndArea(countryCode, areaId);
			break;
		}
		case GeocodeAlert:
		{
			weatherData = weather.getAlertsByGeocode(getLatitudeValue(tuple), getLongitudeValue(tuple));
			break;	
		}
		default:
			logger.error("Unknown apiType: " + apiType);
		}

		return weatherData;
	}

}
