package com.ibm.streamsx.weather.internal.api;

public enum Units {

	E,
	M,
	H,
	NONE;
	
	@Override
	public String toString() {
		return this.name().toLowerCase();
	}
}
