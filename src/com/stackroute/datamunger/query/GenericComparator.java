package com.stackroute.datamunger.query;

import java.util.Comparator;

/*
 * The GenericComparator class implements Comparator Interface. This class is used to 
 * compare row objects which will be used for sorting the dataSet
 */
public class GenericComparator implements Comparator<Row> {
	String columnName=null;
	String dataType=null;
	int order;
	public GenericComparator(String columnName,String dataType,int order){
		this.columnName=columnName;
		this.dataType=dataType;
		this.order=order;
	}
	@Override
	public int compare(Row o1, Row o2) {
		String a=null;
		Integer b=0;
		if(dataType.equals("java.lang.String")) {
			return order*(o1.get(columnName).compareTo(o2.get(columnName)));
		}
		else  {
			return order*(Integer.compare(Integer.valueOf(o1.get(columnName)),Integer.valueOf(o2.get(columnName))));
		}
	}
	
		
}