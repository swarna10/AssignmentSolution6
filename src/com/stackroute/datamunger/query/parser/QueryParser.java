package com.stackroute.datamunger.query.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class QueryParser {

	private QueryParameter queryParameter = new QueryParameter();

	/*
	 * this method will parse the queryString and will return the object of
	 * QueryParameter class
	 */
	/*
	 * public String file=null; public List<Restriction> restrictions=null; public
	 * List<String> logicalOperators=null; public List<String> fields=null; public
	 * List<AggregateFunction> aggregateFunction=null; public List<String>
	 * groupByFields=null; public List<String> orderByFields=null;
	 */

	@SuppressWarnings("null")
	public QueryParameter parseQuery(String queryString) {
		
		String file = null;
		List<Restriction> restrictions = new ArrayList<Restriction>();
		List<String> logicalOperators = new ArrayList<String>();
		List<String> fields = new ArrayList<String>();;
		List<AggregateFunction> aggregateFunction = new ArrayList<AggregateFunction>();
		List<String> groupByFields = new ArrayList<String>();;
		List<String> orderByFields = new ArrayList<String>();;
		String baseQuery=null;
		// getting file name
		file = getFile(queryString);

		// setting restrictions
		String[] conditions = getConditions(queryString);
		if(conditions!=null) {
		Restriction[] restriction = new Restriction[conditions.length];
		
		for (int i = 0; i < conditions.length; i++) {
			restriction[i] = new Restriction();
			
			String operator=null;
			String value=null;
			String property=null;
			 if(conditions[i].contains("<=")) {
				String[] split = conditions[i].split("<=");
				operator="<=";
				 value=split[1].trim();
				 property=split[0].trim();
				
			}
			else if(conditions[i].contains(">=")) {
				String[] split = conditions[i].split(">=");
				operator=">=";
				 value=split[1].trim();
				 property=split[0].trim();
				
			}
			else if(conditions[i].contains(">")) {
				String[] split = conditions[i].split(">");
				operator=">";
				 value=split[1].trim();
				 property=split[0].trim();
			}
			else if(conditions[i].contains("!=")) {
				String[] split = conditions[i].split("!=");
				operator="!=";
				 value=split[1].trim();
				 property=split[0].trim();
			}
			else if(conditions[i].contains("=")) {
				String[] split = conditions[i].split("=");
				operator="=";
				 value=split[1].trim();
				 property=split[0].trim();
				 if(value.contains("'")) {
					 value= value.replaceAll("'","").trim();
				 }
				
			}
			else if(conditions[i].contains("<")) {
				String[] split = conditions[i].split("<");
				operator="<";
				 value=split[1].trim();
				 property=split[0].trim();
			}
			 
			
			restriction[i].setCondition(operator);
			restriction[i].setPropertyName(property);
			restriction[i].setPropertyValue(value);
			restrictions.add(restriction[i]);

		}
		}
		// getting logicaloperators
		String[] operators = getLogicalOperators(queryString);
		if(operators!=null) {
		for (String op : operators) {
			logicalOperators.add(op);
		}
		}
		// getting field values
		String[] filds = getFields(queryString);
		if(filds!=null) {
		for (String field : filds) {
			fields.add(field);
		}
		}
		// setting Aggregations
		String[] aggregationVal = getAggregateFunctions(queryString);
		if(aggregationVal!=null) {
		AggregateFunction[] aggregation = new AggregateFunction[aggregationVal.length];
		for (int i = 0; i < aggregationVal.length; i++) {
			aggregation[i] = new AggregateFunction();
			String[] split = (aggregationVal[i].replace("(", " ")).split(" ");
			System.out.println(split[0]);
			System.out.println(split[1].replace(")", "").trim());
			
			aggregation[i].setFunction(split[0]);
			aggregation[i].setField(split[1].replace(")", "").trim());
			aggregateFunction.add(aggregation[i]);

		}
		}
		
			
		// getting group by fields
		String[] groupBy = getGroupByFields(queryString);
		if(groupBy!=null) {
		for (String group : groupBy) {
			groupByFields.add(group);
		}
		}
		// getting order by fields
		String[] orderBy = getOrderByFields(queryString);
		if(orderBy!=null) {
		for (String order : orderBy) {
			orderByFields.add(order);
		}
		}
		queryParameter.setFile(file);
		if(restrictions.size()!=0) {
			queryParameter.setRestrictions(restrictions);
		}
		else {
			queryParameter.setRestrictions(null);
		}
		if(logicalOperators.size()!=0) {
		queryParameter.setLogicalOperators(logicalOperators);
		}
		else {
			queryParameter.setLogicalOperators(null);
		}
		baseQuery=getBaseQuery(queryString);
		
		queryParameter.setFields(fields);
		queryParameter.setAggregateFunctions(aggregateFunction);
		queryParameter.setGroupByFields(groupByFields);
		queryParameter.setOrderByFields(orderByFields);
		queryParameter.setBaseQuery(baseQuery.trim());
		return queryParameter;

	}

	/*
	 * this method will split the query string based on space into an array of words
	 * and display it on console
	 */
	public static String[] getSplitStrings(String queryString) {
		//queryString = queryString.toLowerCase();
		String[] split = queryString.split("\\s+");
		System.out.println("query strings are:");
		for (String s : split) {
			System.out.println(s);
		}
		return split;

	}

	/*
	 * extract the name of the file from the query. File name can be found after a
	 * space after "from" clause. Note: ----- CSV file can contain a field that
	 * contains from as a part of the column name. For eg: from_date,from_hrs etc.
	 * 
	 * Please consider this while extracting the file name in this method.
	 */
	public static String getFile(String queryString) {
		////queryString = queryString.toLowerCase();
		String file = null;
		String[] split = queryString.split("\\s+");
		for (String s : split) {
			if (s.endsWith("csv")) {
				file = s;
			}
		}
		System.out.println("file is:" + file);
		return file;

	}

	/*
	 * This method is used to extract the baseQuery from the query string. BaseQuery
	 * contains from the beginning of the query till the where clause
	 * 
	 * Note: ------- 1. the query might not contain where clause but contain order
	 * by or group by clause 2. the query might not contain where, order by or group
	 * by clause 3. the query might not contain where, but can contain both group by
	 * and order by clause
	 */
	public static String getBaseQuery(String queryString) {
		//queryString = queryString.toLowerCase();
		String file = null;
		String baseQuery = null;
		String[] split = null;
		if (queryString.contains(" where ")) {
			split = queryString.split("where");
			baseQuery = split[0];

		} else if (queryString.contains("group by")) {
			split = queryString.split("group by");
			baseQuery = split[0];
		} else if (queryString.contains("order by")) {
			split = queryString.split("order by");
			baseQuery = split[0];
		} else {
			baseQuery = queryString;
		}
		System.out.println("base query:" + baseQuery);
		return baseQuery;
	}

	/*
	 * This method is used to extract the conditions part from the query string. The
	 * conditions part contains starting from where keyword till the next keyword,
	 * which is either group by or order by clause. In case of absence of both group
	 * by and order by clause, it will contain till the end of the query string.
	 * Note: ----- 1. The field name or value in the condition can contain keywords
	 * as a substring. For eg: from_city,job_order_no,group_no etc. 2. The query
	 * might not contain where clause at all.
	 */
	public static String getConditionsPartQuery(String queryString) {
		//queryString = queryString.toLowerCase();
		if (queryString.contains(" where ")) {
			String[] split = queryString.split("where");
			String conditionalPart = split[1];

			if (split[1].contains("group")) {
				conditionalPart = (split[1].split("group by"))[0];
			}
			if (split[1].contains("order")) {
				conditionalPart = (split[1].split("order by"))[0];
			}
			System.out.println("getConditionsPartQuery:" + split[1]);
			return conditionalPart;
		} else {
			return null;
		}
	}

	/*
	 * This method will extract condition(s) from the query string. The query can
	 * contain one or multiple conditions. In case of multiple conditions, the
	 * conditions will be separated by AND/OR keywords. for eg: Input: select
	 * city,winner,player_match from ipl.csv where season > 2014 and city
	 * ='Bangalore'
	 * 
	 * This method will return a string array ["season > 2014","city ='Bangalore'"]
	 * and print the array
	 * 
	 * Note: ----- 1. The field name or value in the condition can contain keywords
	 * as a substring. For eg: from_city,job_order_no,group_no etc. 2. The query
	 * might not contain where clause at all.
	 */
	public static String[] getConditions(String queryString) {

		//queryString = queryString.toLowerCase();
		String condtionalPart = getConditionsPartQuery(queryString);

		String[] conditions = null;

		if (condtionalPart != null) {

			if (condtionalPart.contains(" and ") && condtionalPart.contains(" or ")) {
				List<String> list = new ArrayList<String>();
				if (condtionalPart.indexOf(" and ") < condtionalPart.indexOf(" or ")) {
					list.add(condtionalPart.split(" and ")[0]);
					list.add((condtionalPart.split(" and ")[1]).split(" or ")[0]);
					list.add((condtionalPart.split(" and ")[1]).split(" or ")[1]);
				} else {
					list.add(condtionalPart.split(" or ")[0]);
					list.add((condtionalPart.split(" or ")[1]).split(" and ")[0]);
					list.add((condtionalPart.split(" or ")[1]).split(" and ")[1]);
				}
				conditions = new String[list.size()];
				for (int i = 0; i < list.size(); i++) {
					conditions[i] = list.get(i).trim();
				}
			} else if (condtionalPart.contains(" and ")) {
				conditions = condtionalPart.split(" and ");
				// return conditions;
			}

			else if (condtionalPart.contains(" or ")) {
				conditions = condtionalPart.split(" or ");
				// return conditions;
			}

			else {
				conditions = new String[1];
				conditions[0] = condtionalPart;
				// return conditions;
			}

			for (int i = 0; i < conditions.length; i++) {
				conditions[i] = conditions[i].trim();
			}
			System.out.println("conditions are:" + Arrays.toString(conditions));
			return conditions;

		} else {
			return null;
		}
	}

	/*
	 * This method will extract logical operators(AND/OR) from the query string. The
	 * extracted logical operators will be stored in a String array which will be
	 * returned by the method and the same will be printed Note: ------- 1. AND/OR
	 * keyword will exist in the query only if where conditions exists and it
	 * contains multiple conditions. 2. AND/OR can exist as a substring in the
	 * conditions as well. For eg: name='Alexander',color='Red' etc. Please consider
	 * these as well when extracting the logical operators.
	 * 
	 */
	public static String[] getLogicalOperators(String queryString) {
		//queryString = queryString.toLowerCase();
		List<String> logicalOp = new ArrayList<String>();
		if (queryString.contains(" or ")) {
			logicalOp.add("or");
		}
		if (queryString.contains(" OR ")) {
			logicalOp.add("OR");
		}
		if (queryString.contains(" AND ")) {
			logicalOp.add("AND");
		}
		if (queryString.contains(" and ")) {
			logicalOp.add("and");
		}
		String[] logicalOperators = new String[logicalOp.size()];

		if (queryString.contains(" or ") && queryString.contains(" and ")) {
			if (queryString.indexOf(" and ") < queryString.indexOf(" or ")) {
				Collections.reverse(logicalOp);
			}

		}
		for (int i = 0; i < logicalOp.size(); i++) {
			logicalOperators[i] = logicalOp.get(i);
		}
		System.out.println("logical operators are:" + Arrays.toString(logicalOperators));
		if (logicalOperators.length != 0) {
			return logicalOperators;

		} else {
			return null;
		}
	}

	/*
	 * This method will extract the fields to be selected from the query string. The
	 * query string can have multiple fields separated by comma. The extracted
	 * fields will be stored in a String array which is to be printed in console as
	 * well as to be returned by the method
	 * 
	 * Note: ------ 1. The field name or value in the condition can contain keywords
	 * as a substring. For eg: from_city,job_order_no,group_no etc. 2. The field
	 * name can contain '*'
	 * 
	 */
	public static String[] getFields(String queryString) {
		//queryString = queryString.toLowerCase();
		String baseQuery = getBaseQuery(queryString);

		if (((baseQuery.split("select|from"))[1]).contains(",")) {
			String[] baseFields = ((baseQuery.split("select|from")[1])).split(",");

			System.out.println("Base fields:" + Arrays.toString(baseFields));
			for(int i=0;i<baseFields.length;i++){
				
			
					baseFields[i]=baseFields[i].trim();
				
			}
			return baseFields;
		} else {
			String[] baseFields = new String[1];
			
			baseFields[0] = ((baseQuery.split("select|from"))[1]).trim();
			System.out.println("Base fields:" + Arrays.toString(baseFields));
			return baseFields;
		}
	}

	/*
	 * This method extracts the order by fields from the query string. Note: ------
	 * 1. The query string can contain more than one order by fields. 2. The query
	 * string might not contain order by clause at all. 3. The field names,condition
	 * values might contain "order" as a substring. For eg:order_number,job_order
	 * Consider this while extracting the order by fields
	 */
	public static String[] getOrderByFields(String queryString) {
		//queryString = queryString.toLowerCase();
		if (queryString.contains("order by")) {
			String[] orderBy = { (queryString.split("order by"))[1] };
			for (int i = 0; i < orderBy.length; i++) {
				orderBy[i] = orderBy[i].trim();
			}
			return orderBy;
		} else {
			return null;
		}

	}

	/*
	 * This method extracts the group by fields from the query string. Note: ------
	 * 1. The query string can contain more than one group by fields. 2. The query
	 * string might not contain group by clause at all. 3. The field names,condition
	 * values might contain "group" as a substring. For eg: newsgroup_name
	 * 
	 * Consider this while extracting the group by fields
	 */
	public static String[] getGroupByFields(String queryString) {
		//queryString = queryString.toLowerCase();
		if (queryString.contains("group by")) {
			String[] groupBy = { (queryString.split("group by"))[1] 
					
			
			};
			
			if(groupBy[0].contains("order by")){
				groupBy[0] = (groupBy[0].split("order by"))[0] ;	
			}
			for (int i = 0; i < groupBy.length; i++) {
				groupBy[i] = groupBy[i].trim();
			}
			System.out.println("group by fields are:" + Arrays.toString(groupBy));
			return groupBy;
		} else {
			return null;
		}

	}

	/*
	 * This method extracts the aggregate functions from the query string. Note:
	 * ------ 1. aggregate functions will start with "sum"/"count"/"min"/"max"/"avg"
	 * followed by "(" 2. The field names might
	 * contain"sum"/"count"/"min"/"max"/"avg" as a substring. For eg:
	 * account_number,consumed_qty,nominee_name
	 * 
	 * Consider this while extracting the aggregate functions
	 */
	public static String[] getAggregateFunctions(String queryString) {
		//queryString = queryString.toLowerCase();
		String baseQuery = getBaseQuery(queryString);
		if (((baseQuery.split("\\s+"))[1]).contains("sum") || ((baseQuery.split("\\s+"))[1]).contains("max")
				|| ((baseQuery.split("\\s+"))[1]).contains("min") || ((baseQuery.split("\\s+"))[1]).contains("count") || ((baseQuery.split("\\s+"))[1]).contains("avg")) {
			String[] agregateFields = ((baseQuery.split("\\s+"))[1]).split(",");
			List<String> agFunctions=new ArrayList<>();
			for(String a:agregateFields) {
				if (a.contains("sum") || a.contains("max")
						|| a.contains("min") || a.contains("count") || a.contains("avg")) {
					agFunctions.add(a);
			
				}
		}
			String[] filteredAg=new String[agFunctions.size()];
			for(int i=0;i<agFunctions.size();i++) {
				filteredAg[i]=agFunctions.get(i);
			}
			System.out.println("Aggregate functions:" + Arrays.toString(filteredAg));
			return filteredAg;
		}else {
			return null;
		}
	}
	/*
	 * extract the order by fields from the query string. Please note that we will
	 * need to extract the field(s) after "order by" clause in the query, if at all
	 * the order by clause exists. For eg: select city,winner,team1,team2 from
	 * data/ipl.csv order by city from the query mentioned above, we need to extract
	 * "city". Please note that we can have more than one order by fields.
	 */

	/*
	 * extract the group by fields from the query string. Please note that we will
	 * need to extract the field(s) after "group by" clause in the query, if at all
	 * the group by clause exists. For eg: select city,max(win_by_runs) from
	 * data/ipl.csv group by city from the query mentioned above, we need to extract
	 * "city". Please note that we can have more than one group by fields.
	 */

	/*
	 * extract the selected fields from the query string. Please note that we will
	 * need to extract the field(s) after "select" clause followed by a space from
	 * the query string. For eg: select city,win_by_runs from data/ipl.csv from the
	 * query mentioned above, we need to extract "city" and "win_by_runs". Please
	 * note that we might have a field containing name "from_date" or "from_hrs".
	 * Hence, consider this while parsing.
	 */

	/*
	 * extract the conditions from the query string(if exists). for each condition,
	 * we need to capture the following: 1. Name of field 2. condition 3. value
	 * 
	 * For eg: select city,winner,team1,team2,player_of_match from data/ipl.csv
	 * where season >= 2008 or toss_decision != bat
	 * 
	 * here, for the first condition, "season>=2008" we need to capture: 1. Name of
	 * field: season 2. condition: >= 3. value: 2008
	 * 
	 * the query might contain multiple conditions separated by OR/AND operators.
	 * Please consider this while parsing the conditions.
	 * 
	 */

	/*
	 * extract the logical operators(AND/OR) from the query, if at all it is
	 * present. For eg: select city,winner,team1,team2,player_of_match from
	 * data/ipl.csv where season >= 2008 or toss_decision != bat and city =
	 * bangalore
	 * 
	 * the query mentioned above in the example should return a List of Strings
	 * containing [or,and]
	 */

	/*
	 * extract the aggregate functions from the query. The presence of the aggregate
	 * functions can determined if we have either "min" or "max" or "sum" or "count"
	 * or "avg" followed by opening braces"(" after "select" clause in the query
	 * string. in case it is present, then we will have to extract the same. For
	 * each aggregate functions, we need to know the following: 1. type of aggregate
	 * function(min/max/count/sum/avg) 2. field on which the aggregate function is
	 * being applied
	 * 
	 * Please note that more than one aggregate function can be present in a query
	 * 
	 * 
	 */

}