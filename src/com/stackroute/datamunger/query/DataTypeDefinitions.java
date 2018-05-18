package com.stackroute.datamunger.query;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/*
 * implementation of DataTypeDefinitions class. This class contains a method getDataTypes() 
 * which will contain the logic for getting the datatype for a given field value. This
 * method will be called from QueryProcessors.   
 * In this assignment, we are going to use Regular Expression to find the 
 * appropriate data type of a field. 
 * Integers: should contain only digits without decimal point 
 * Double: should contain digits as well as decimal point 
 * Date: Dates can be written in many formats in the CSV file. 
 * However, in this assignment,we will test for the following date formats('dd/mm/yyyy',
 * 'mm/dd/yyyy','dd-mon-yy','dd-mon-yyyy','dd-month-yy','dd-month-yyyy','yyyy-mm-dd')
 */
public class DataTypeDefinitions {

	public static Object getDataType(String input) {
		
			final Pattern patternInt= Pattern.compile("[0-9]+");
			final Pattern patternDouble= Pattern.compile("[0-9[.]]+");
			//final Pattern patternDate= Pattern.compile("[0-9[/-]]+");
			final Pattern patternDate= Pattern.compile("^(0?[1-9]|1[012])[\\/\\-](0?[1-9]|[12][0-9]|3[01])[\\/\\-]\\d{4}$");
			final Pattern patternObject= Pattern.compile("\\s+");
			final Matcher matchInt=patternInt.matcher(input);
			final Matcher matchDouble=patternDouble.matcher(input);
			final Matcher matchDate=patternDate.matcher(input);
			final Matcher matchObj=patternObject.matcher(input);
			if(matchInt.matches()) {
				final Integer val=0;
				return val.getClass().getName();
			}
			else if(matchDouble.matches()) {
				final Double val=0.00;
				return val.getClass().getName();
			}
			else if(matchDate.matches()){
				 final Date val=new Date();
				return val.getClass().getName();
			}
			else if(matchObj.matches()) {
				final	Object val= new Object();
				return val.getClass().getName();
				
			}
			else{
				final String val="test";
				return val.getClass().getName();
			}

			}
	}
	
	
