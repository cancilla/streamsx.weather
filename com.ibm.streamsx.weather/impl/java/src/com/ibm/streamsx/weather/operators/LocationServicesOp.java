package com.ibm.streamsx.weather.operators;

import java.util.List;
import java.util.Set;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.TupleAttribute;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streamsx.weather.internal.api.impl.LocationServices;
import com.ibm.streamsx.weather.internal.api.impl.LocationServices.LocationLookupType;

@PrimitiveOperator(name = "LocationServices", namespace = "com.ibm.streamsx.weather")
public class LocationServicesOp extends AbstractWeatherLocationOperator {

	private static final LocationLookupType DEFAULT_LOC_LOOKUP_TYPE = LocationLookupType.Search;
	private static final String DEFAULT_QUERY_ATTR_NAME = "query";
	private static final String DEFAULT_LOCATION_TYPE_ATTR_NAME = "locationType";
	private static final String DEFAULT_COUNTRY_CODE_ATTR_NAME = "countryCode";
	private static final String DEFAULT_ADMIN_DISTRICT_CODE_ATTR_NAME = "adminDistrictCode";
	private static final String DEFAULT_POSTAL_KEY_ATTR_NAME = "postalKey";
	private static final String DEFAULT_IATA_CODE_ATTR_NAME = "iataCode";
	private static final String DEFAULT_ICAO_CODE_ATTR_NAME = "icaoCode";
	
	private LocationServices locServices;
	private LocationLookupType locType;
	
	/* Params */
	private LocationLookupType lookupType = DEFAULT_LOC_LOOKUP_TYPE;
	private TupleAttribute<Tuple, String> queryAttr;
	private TupleAttribute<Tuple, String> locationTypeAttr;
	private TupleAttribute<Tuple, String> countryCodeAttr;
	private TupleAttribute<Tuple, String> adminDistrictCodeAttr;
	private TupleAttribute<Tuple, String> postalKeyAttr;
	private TupleAttribute<Tuple, String> iataCodeAttr;
	private TupleAttribute<Tuple, String> icaoCodeAttr;

	@Parameter(name = "postalKey", optional = true)
	public void setPostalKeyAttr(TupleAttribute<Tuple, String> postalKeyAttr) {
		this.postalKeyAttr = postalKeyAttr;
	}
	
	@Parameter(name = "iataCode", optional = true)
	public void setIataCodeAttr(TupleAttribute<Tuple, String> iataCodeAttr) {
		this.iataCodeAttr = iataCodeAttr;
	}

	@Parameter(name = "icaoCode", optional = true)
	public void setIcaoCodeAttr(TupleAttribute<Tuple, String> icaoCodeAttr) {
		this.icaoCodeAttr = icaoCodeAttr;
	}
	
	@Parameter(name = "lookupType", optional = true)
	public void setLookupType(LocationLookupType lookupType) {
		this.lookupType = lookupType;
	}
	
	@Parameter(name = "adminDistrictCodeAttr", optional = true)
	public void setAdminDistrictCodeAttr(TupleAttribute<Tuple, String> adminDistrictCodeAttr) {
		this.adminDistrictCodeAttr = adminDistrictCodeAttr;
	}

	@Parameter(name = "countryCodeAttr", optional = true)
	public void setCountryCodeAttr(TupleAttribute<Tuple, String> countryCodeAttr) {
		this.countryCodeAttr = countryCodeAttr;
	}

	@Parameter(name = "locationTypeAttr", optional = true)
	public void setLocationTypeAttr(TupleAttribute<Tuple, String> locationTypeAttr) {
		this.locationTypeAttr = locationTypeAttr;
	}

	@Parameter(name = "queryAttr", optional = true)
	public void setQueryAttr(TupleAttribute<Tuple, String> queryAttr) {
		this.queryAttr = queryAttr;
	}

	public TupleAttribute<Tuple, String> getPostalKeyAttr() {
		return postalKeyAttr;
	}
	
	public TupleAttribute<Tuple, String> getIataCodeAttr() {
		return iataCodeAttr;
	}
	
	public TupleAttribute<Tuple, String> getIcaoCodeAttr() {
		return icaoCodeAttr;
	}
	
	public TupleAttribute<Tuple, String> getAdminDistrictCodeAttr() {
		return adminDistrictCodeAttr;
	}

	public TupleAttribute<Tuple, String> getCountryCodeAttr() {
		return countryCodeAttr;
	}

	public TupleAttribute<Tuple, String> getLocationTypeAttr() {
		return locationTypeAttr;
	}

	public TupleAttribute<Tuple, String> getQueryAttr() {
		return queryAttr;
	}
	
	public LocationLookupType getLookupType() {
		return lookupType;
	}
	
	@ContextCheck(runtime = true, compile = false)
	public static void checkParams(OperatorContextChecker checker) {
		LocationLookupType type = DEFAULT_LOC_LOOKUP_TYPE;

		List<String> paramValues = checker.getOperatorContext().getParameterValues("lookupType");
		if(!paramValues.isEmpty()) {
			type = LocationLookupType.valueOf(paramValues.get(0));
		}
		
		switch(type) {
		case Search:
			checkParam(checker, "queryAttr", DEFAULT_QUERY_ATTR_NAME);
			break;
		case Geocode:
			checkParam(checker, "latitudeAttr", DEFAULT_LATITUDE_ATTR_NAME);
			checkParam(checker, "longitudeAttr", DEFAULT_LONGITUDE_ATTR_NAME);
			break;
		case PostalKey:
			checkParam(checker, "postalKeyAttr", DEFAULT_POSTAL_KEY_ATTR_NAME);
			break;
		case IATACode:
			checkParam(checker, "iataCodeAttr", DEFAULT_IATA_CODE_ATTR_NAME);
			break;
		case ICAOCode:
			checkParam(checker, "icaoCodeAttr", DEFAULT_ICAO_CODE_ATTR_NAME);
			break;
		}
	}
	
	private static void checkParam(OperatorContextChecker checker, String paramName, String defaultAttrName) {
		Set<String> paramNames = checker.getOperatorContext().getParameterNames();
		StreamSchema streamSchema = checker.getOperatorContext().getStreamingInputs().get(0).getStreamSchema();
		String invalidMsg = "Either the '%s' parameter must be specified or the input port must contain an attribute named '%s'.";
		if(!paramNames.contains(paramName) &&
				streamSchema.getAttribute(defaultAttrName) == null) {
			checker.setInvalidContext(String.format(invalidMsg, paramName, defaultAttrName), new Object[0]);
		}
	}
	
	@Override
	public synchronized void initialize(OperatorContext context) throws Exception {
		super.initialize(context);
		
		locType = getLookupType();
	}
	
	@Override
	protected String getWeatherData(Tuple tuple) throws Exception {
		if(locType == LocationLookupType.Search) {
			String query = queryAttr != null ? queryAttr.getValue(tuple) : tuple.getString(DEFAULT_QUERY_ATTR_NAME);;
			String locationType = null;
			String countryCode = null;
			String adminDistrictCode = null;
			
			if(locationTypeAttr != null) {
				locationType = locationTypeAttr.getValue(tuple);
			} else if(tuple.getStreamSchema().getAttribute(DEFAULT_LOCATION_TYPE_ATTR_NAME) != null) {
				locationType = tuple.getString(DEFAULT_LOCATION_TYPE_ATTR_NAME);
			}
			
			if(countryCodeAttr != null) {
				countryCode = countryCodeAttr.getValue(tuple);
			} else if(tuple.getStreamSchema().getAttribute(DEFAULT_COUNTRY_CODE_ATTR_NAME) != null) {
				countryCode = tuple.getString(DEFAULT_COUNTRY_CODE_ATTR_NAME);
			}

			if(adminDistrictCodeAttr != null) {
				adminDistrictCode = adminDistrictCodeAttr.getValue(tuple);
			} else if(tuple.getStreamSchema().getAttribute(DEFAULT_ADMIN_DISTRICT_CODE_ATTR_NAME) != null) {
				adminDistrictCode = tuple.getString(DEFAULT_ADMIN_DISTRICT_CODE_ATTR_NAME);
			}
			
			return locServices.search(query, locationType, countryCode, adminDistrictCode);	
		} else if(locType == LocationLookupType.Geocode) {
			return locServices.getPointFromGeocode(getLatitudeValue(tuple), getLongitudeValue(tuple));
		} else if(locType == LocationLookupType.PostalKey) {
			String code = getPostalKeyAttr() != null ? getPostalKeyAttr().getValue(tuple) : tuple.getString(DEFAULT_POSTAL_KEY_ATTR_NAME);
			return locServices.getPointFromPostalKey(code);
		} else if(locType == LocationLookupType.IATACode) {
			String code = getIataCodeAttr() != null ? getIataCodeAttr().getValue(tuple) : tuple.getString(DEFAULT_IATA_CODE_ATTR_NAME);
			return locServices.getPointFromIataCode(code);
		} else if(locType == LocationLookupType.ICAOCode) {
			String code = getIcaoCodeAttr() != null ? getIcaoCodeAttr().getValue(tuple) : tuple.getString(DEFAULT_ICAO_CODE_ATTR_NAME);
			return locServices.getPointFromIcao(code);
		}
		
		return null;
	}

	@Override
	protected void initWeatherApi(OperatorContext context) throws Exception {
		locServices = new LocationServices(getUsername(), getPassword());
	}
	
}
