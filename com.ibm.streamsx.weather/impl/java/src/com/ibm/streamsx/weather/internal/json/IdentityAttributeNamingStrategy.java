package com.ibm.streamsx.weather.internal.json;

public class IdentityAttributeNamingStrategy implements AttributeNamingStrategy {

	@Override
	public String transform(String name) {
		return name;
	}

}
