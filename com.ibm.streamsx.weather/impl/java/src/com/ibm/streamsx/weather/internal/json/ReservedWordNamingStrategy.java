package com.ibm.streamsx.weather.internal.json;

import java.util.HashMap;
import java.util.Map;

public class ReservedWordNamingStrategy implements AttributeNamingStrategy {

	private Map<String /* safe name */, String /* reserved keyword */> keywordMap;
	
	public ReservedWordNamingStrategy() {
		keywordMap = new HashMap<String, String>();
		
		// initialize map with a base set of reserved keywords (not exhaustive)
		keywordMap.put("type_", "type");
		keywordMap.put("class_", "class");	
	}
	
	/*
	 * If the name exists in the map, return 
	 * the mapped value. Otherwise, return the 
	 * name unmodified;
	 */
	@Override
	public String transform(String name) {
		return keywordMap.containsKey(name) ? keywordMap.get(name) : name;
	}

}
