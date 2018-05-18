package com.stackroute.datamunger.query;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import com.stackroute.datamunger.query.parser.AggregateFunction;
import com.stackroute.datamunger.query.parser.Restriction;

//this class contains methods to evaluate expressions
public class Filter {
	
	/* 
	 * the evaluateExpression() method of this class is responsible for evaluating 
	 * the expressions mentioned in the query. It has to be noted that the process 
	 * of evaluating expressions will be different for different data types. there 
	 * are 6 operators that can exist within a query i.e. >=,<=,<,>,!=,= This method 
	 * should be able to evaluate all of them. 
	 * Note: while evaluating string expressions, please handle uppercase and lowercase 
	 * 
	 */
	
	public static Boolean filter (List<Restriction> restriction,List<String> logicalOperators,String[] row,Header header,RowDataTypeDefinitions rowDataType) {
		//JexlEngine engine= new JexlBuilder().create();
		String[] javaLogics=null;
		
		if(logicalOperators!=null) {
			if((logicalOperators.size()>1 ) && (logicalOperators.get(logicalOperators.size()-1)).equals("and") ) {
				Collections.reverse(logicalOperators);
				Collections.reverse(restriction);
				
			}
			javaLogics=new String[logicalOperators.size()];
		
			
		for(int i=0;i<logicalOperators.size();i++) {
			if(logicalOperators.get(i).equals("and")) {
				javaLogics[i]="&&";
			}
			else {
				javaLogics[i]="||";
			}
		}
		
		}
	
	
		String expression=null;
		for(int i=0;i<restriction.size();i++) {
			String property=restriction.get(i).getPropertyName();
			String op=restriction.get(i).getCondition();
			if(op=="=") {
				op="==";
			}
			String value=restriction.get(i).getPropertyValue();
			String propertyVal=row[(header.get(property))-1];
		Boolean isString=rowDataType.get((header.get(property))).equals("java.lang.String");
			if(i==0) {
				if(isString) {
				expression= "(\""+propertyVal.toLowerCase()+"\""+" "+op+" "+"\""+value.toLowerCase()+"\")";
				}
				else {
					expression= "("+propertyVal+" "+op+" "+value+")";
				}
			}
			else  {
				String logic=javaLogics[i-1];
				if(isString) {
				expression=expression+" "+logic+" "+"(\""+propertyVal.toLowerCase()+"\""+" "+op+" "+"\""+value.toLowerCase()+"\")";
				
				}
				else {
					expression=expression+" "+logic+" ("+propertyVal+" "+op+" "+value+")";
				}
				
				}
			if(((i+1)%2)==0) {
				expression="("+expression+")";
			}
			
		}
	//	System.out.println("expression is:"+expression);
		ScriptEngineManager mgr = new ScriptEngineManager();
	    ScriptEngine engine = mgr.getEngineByName("JavaScript");
	    try {
	    	return (Boolean) engine.eval(expression);
		} catch (ScriptException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	
			
	}
	
	
	
	/*if(restriction.size()==1)
	{
	String property=restriction.get(0).getPropertyName();
	String op=restriction.get(0).getCondition();
	String value=restriction.get(0).getPropertyValue();
	String propertyVal=row[(header.get(property))-1];
	
	String expression="property operator value";
	JexlExpression e=engine.createExpression(expression);	
	JexlContext ctx= new MapContext();
	ctx.set("property", propertyVal);
	ctx.set("operator", op);
	ctx.set("value", value);
	
	return (Boolean) e.evaluate(ctx);
	}
	else if(restriction.size()==2)
	{
	String property=restriction.get(0).getPropertyName();
	String op=restriction.get(0).getCondition();
	String value=restriction.get(0).getPropertyValue();
	String propertyVal=row[(header.get(property))-1];
	
	String property1=restriction.get(1).getPropertyName();
	String op1=restriction.get(1).getCondition();
	String value1=restriction.get(1).getPropertyValue();
	String propertyVal1=row[(header.get(property1))-1];
	
	String expression="property1 operator1 value1 logic1 property2 operator2 value2";
	JexlExpression e=engine.createExpression(expression);	
	JexlContext ctx= new MapContext();
	ctx.set("property", propertyVal);
	ctx.set("operator", op);
	ctx.set("value", value);
	ctx.set("property2", propertyVal1);
	ctx.set("operator2", op1);
	ctx.set("value2", value1);
	ctx.set("logic1", javaLogics[0]);
	return (Boolean) e.evaluate(ctx);
	}*/
	//method containing implementation of equalTo operator
	
	
	
	
	
	//method containing implementation of notEqualTo operator
	
	
	
	
	
	
	
	//method containing implementation of greaterThan operator
	
	
	
	
	
	
	
	//method containing implementation of greaterThanOrEqualTo operator
	
	
	
	
	
	
	//method containing implementation of lessThan operator
	  
	
	
	
	
	//method containing implementation of lessThanOrEqualTo operator
	
}