package com.ibm.streamsx.weather.internal.api;

public enum Days {
	THREE_DAY(3),
	FIVE_DAY(5),
	SEVEN_DAY(7),
	TEN_DAY(10);
	
	private Integer days;
	
	private Days(Integer days) {
		this.days = days;
	}
	
	public Integer getDays() {
		return this.days;
	}
	
	public static Days findDays(Integer days) {
		switch(days) {
		case 3:
			return THREE_DAY;
		case 5:
			return FIVE_DAY;
		case 7:
			return SEVEN_DAY;
		case 10:
			return TEN_DAY;
			default:
				return null;
		}
	}
}
