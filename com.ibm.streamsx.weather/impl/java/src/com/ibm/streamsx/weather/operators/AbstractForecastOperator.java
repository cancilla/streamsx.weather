package com.ibm.streamsx.weather.operators;

import java.util.Set;

import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.TupleAttribute;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.model.Parameter;

public abstract class AbstractForecastOperator extends AbstractWeatherLocationOperator {

	protected static final String DEFAULT_LOCATIONID_ATTR_NAME = "locationId";

	private TupleAttribute<Tuple, String> locationIdAttr;

	@Parameter(name = "locationIdAttr", optional = true, description = "Specifies the name of the input attribute containing the value for the"
			+ " location ID. If this value is not specified, then by default the operator will attempt to retrieve the location ID value"
			+ " from an attribute named **locationId**")
	public void setLocationIdAttr(TupleAttribute<Tuple, String> locationIdAttr) {
		this.locationIdAttr = locationIdAttr;
	}

	public TupleAttribute<Tuple, String> getLocationIdAttr() {
		return locationIdAttr;
	}

	@ContextCheck(compile = true)
	public static void checkParams(OperatorContextChecker checker) {
		Set<String> params = checker.getOperatorContext().getParameterNames();
		StreamingInput<Tuple> inputPort0 = checker.getOperatorContext().getStreamingInputs().get(0);
		if (!isLatLonPresent(checker) && !params.contains("locationIdAttr")
				&& inputPort0.getStreamSchema().getAttribute(DEFAULT_LOCATIONID_ATTR_NAME) == null) {
			checker.setInvalidContext(
					"You must specify either latitude and longitude values, or you must specify a locationId.",
					new Object[0]);
		}

		if (isLatLonPresent(checker) && (params.contains("locationIdAttr")
				|| inputPort0.getStreamSchema().getAttribute(DEFAULT_LOCATIONID_ATTR_NAME) != null)) {
			checker.setInvalidContext(
					"You cannot specify values for latitude, longitude and locationId. You must speicyf either latitude and longitude values, or you must specify a locationId.",
					new Object[0]);
		}
	}

}
