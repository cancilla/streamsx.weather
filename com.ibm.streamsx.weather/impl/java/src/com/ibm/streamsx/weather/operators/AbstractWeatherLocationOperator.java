/*******************************************************************************
 * Copyright (C) 2016, International Business Machines Corporation
 * All Rights Reserved
 *******************************************************************************/

package com.ibm.streamsx.weather.operators;

import java.util.Set;

import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.TupleAttribute;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.model.Parameter;

public abstract class AbstractWeatherLocationOperator extends AbstractWeatherApiOperator {

	protected static final String DEFAULT_LATITUDE_ATTR_NAME = "latitude";
	protected static final String DEFAULT_LONGITUDE_ATTR_NAME = "longitude";

//	private static final Logger logger = Logger.getLogger(AbstractWeatherLocationOperator.class);
	
	/* Parameters */
	private String units;
	private TupleAttribute<Tuple, Double> latitudeAttr;
	private TupleAttribute<Tuple, Double> longitudeAttr;



	@Parameter(name = "units", optional = true, description = "Specifies the units to return the response in."
			+ " Valid values are: **e** (English), **m** (Metric), **h** (UK-Hybrid). The default value is *m*.")
	public void setUnit(String units) {
		this.units = units;
	}

	@Parameter(name = "latitudeAttr", optional = true, description = "Specifies the name of the attribute on the input port that contains the value for latitude."
			+ " If this parameter is not specified, the operator will attempting to look for an attribute named **latitude**.")
	public void setLatitudeAttr(TupleAttribute<Tuple, Double> latitudeAttr) {
		this.latitudeAttr = latitudeAttr;
	}

	@Parameter(name = "longitudeAttr", optional = true, description = "Specifies the name of the attribute on the input port that contains the value for longitude."
			+ " If this parameter is not specified, the operator will attempting to look for an attribute named **longitude**.")
	public void setLongitudeAttr(TupleAttribute<Tuple, Double> longitudeAttr) {
		this.longitudeAttr = longitudeAttr;
	}



	public String getUnits() {
		return units;
	}


	public TupleAttribute<Tuple, Double> getLatitudeAttr() {
		return latitudeAttr;
	}

	public TupleAttribute<Tuple, Double> getLongitudeAttr() {
		return longitudeAttr;
	}

	protected Double getLatitudeValue(Tuple tuple) {
		return getLatitudeAttr() == null ? tuple.getDouble(DEFAULT_LATITUDE_ATTR_NAME)
				: getLatitudeAttr().getValue(tuple);
	}

	protected Double getLongitudeValue(Tuple tuple) {
		return getLongitudeAttr() == null ? tuple.getDouble(DEFAULT_LONGITUDE_ATTR_NAME)
				: getLongitudeAttr().getValue(tuple);
	}

	protected static boolean isLatLonPresent(OperatorContextChecker checker) {
		Set<String> params = checker.getOperatorContext().getParameterNames();
		StreamingInput<Tuple> inputPort0 = checker.getOperatorContext().getStreamingInputs().get(0);
		if ((!params.contains("latitudeAttr")
				&& inputPort0.getStreamSchema().getAttribute(DEFAULT_LATITUDE_ATTR_NAME) == null)
				|| (!params.contains("longitudeAttr")
						&& inputPort0.getStreamSchema().getAttribute(DEFAULT_LONGITUDE_ATTR_NAME) == null)) {
			return false;
		}

		return true;
	}
}
