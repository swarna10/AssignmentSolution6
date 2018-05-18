package com.stackroute.datamunger.query.parser;

/*
 * This class is used for storing name of field, condition and value for 
 * each conditions
 * */
public class Restriction {
	
	public String propertyName=null;
	public String propertyValue=null;
	public String condition=null;
	public void setPropertyName(String propertyName) {
		this.propertyName = propertyName;
	}
	public void setPropertyValue(String propertyValue) {
		this.propertyValue = propertyValue;
	}
	public void setCondition(String condition) {
		this.condition = condition;
	}
	public String getPropertyName() {
		return propertyName;
	}
	public String getPropertyValue() {
		return propertyValue;
	}
	public String getCondition() {
		return condition;
	}
	

}