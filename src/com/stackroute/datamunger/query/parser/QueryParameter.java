package com.stackroute.datamunger.query.parser;

import java.util.List;
import java.util.Map;

/* 
 * This class will contain the elements of the parsed Query String such as conditions,
 * logical operators,aggregate functions, file name, fields group by fields, order by
 * fields, Query Type
 * */
public class QueryParameter {

	public String file=null;
	public List<Restriction> restrictions=null;
	public List<String> logicalOperators=null;
	public List<String> fields=null;
	public List<AggregateFunction> aggregateFunction=null;
	public List<String> groupByFields=null;
	public List<String> orderByFields=null;
	public String QUERY_TYPE=null;
	public String baseQuery=null;
	
	public List<AggregateFunction> getAggregateFunction() {
		return aggregateFunction;
	}

	public void setAggregateFunction(List<AggregateFunction> aggregateFunction) {
		this.aggregateFunction = aggregateFunction;
	}

	public String getBaseQuery() {
		return baseQuery;
	}

	public void setBaseQuery(String baseQuery) {
		this.baseQuery = baseQuery;
	}

	public List<AggregateFunction> getAggregateFunctions() {
		return aggregateFunction;
	}

	public void setAggregateFunctions(List<AggregateFunction> aggregateFunction) {
		this.aggregateFunction = aggregateFunction;
	}

	public void setFile(String file) {
		this.file = file;
	}

	public void setRestrictions(List<Restriction> restrictions) {
		this.restrictions = restrictions;
	}

	public void setLogicalOperators(List<String> logicalOperators) {
		this.logicalOperators = logicalOperators;
	}

	public void setFields(List<String> fields) {
		this.fields = fields;
	}

	public void setGroupByFields(List<String> groupByFields) {
		this.groupByFields = groupByFields;
	}

	public void setOrderByFields(List<String> orderByFields) {
		this.orderByFields = orderByFields;
	}

	public String getFile() {
		return file;
	}

	public List<Restriction> getRestrictions() {
		return restrictions;
	}

	public List<String> getLogicalOperators() {
		return logicalOperators;
	}

	public List<String> getFields() {
		return fields;
	}

	public List<String> getGroupByFields() {
		return groupByFields;
	}

	public List<String> getOrderByFields() {
		return orderByFields;
	}


	public String getQUERY_TYPE() {
		// TODO Auto-generated method stub
		return QUERY_TYPE;
	}

	public void  setQUERY_TYPE(String QUERY_TYPE) {
		// TODO Auto-generated method stub
		 this.QUERY_TYPE=QUERY_TYPE;
	}
		

	
}