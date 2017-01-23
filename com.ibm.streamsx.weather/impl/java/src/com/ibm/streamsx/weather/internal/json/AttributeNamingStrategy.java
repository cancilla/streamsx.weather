package com.ibm.streamsx.weather.internal.json;

public interface AttributeNamingStrategy {

	public static final AttributeNamingStrategy IDENTITY = new IdentityAttributeNamingStrategy();
	
	public String transform(String name);
	
}
