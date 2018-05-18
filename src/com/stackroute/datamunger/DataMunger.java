package com.stackroute.datamunger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Scanner;
import java.util.function.DoubleFunction;

import com.stackroute.datamunger.query.Query;
import com.stackroute.datamunger.writer.JsonWriter;


public class DataMunger {
	
	public static void main(String[] args){
		int totalRecordsExpected = 577;
		Query query=new Query();
		HashMap dataSet = query.executeQuery("select player_of_match,city,team1,team2 from data/ipl.csv order by city");
		boolean totalColumnsExpected = dataSet.entrySet().iterator().next().toString().split(",").length == 4;
		assertNotNull("testWithOrderBy() : Empty Dataset returned", dataSet);
		assertEquals("testWithOrderBy() : Total number of records should be 577", totalRecordsExpected, dataSet.size());
		assertEquals("testWithOrderBy() : Total number of columns should be 4", true, totalColumnsExpected);

		assertTrue("testWithOrderBy() : Count for the city Bangalore does not match the expected value",
				dataSet.toString().contains(
						"1={player_of_match=PA Patel, city=, team1=Mumbai Indians, team2=Royal Challengers Bangalore}"));
		assertTrue("testWithOrderBy() : Count for the city Bangalore does not match the expected value",
				dataSet.toString().contains(
						"288={player_of_match=DJ Bravo, city=Hyderabad, team1=Mumbai Indians, team2=Deccan Chargers}"));
		assertTrue(
				"testWithOrderBy() : Total number of records are matching but the records returned does not match the expected data",
				dataSet.toString().contains(
						"577={player_of_match=MS Dhoni, city=Visakhapatnam, team1=Kings XI Punjab, team2=Rising Pune Supergiants}"));

		//read the query from the user
		
		
		/*
		 * Instantiate Query class. This class is responsible for: 1. Parsing the query
		 * 2. Select the appropriate type of query processor 3. Get the resultSet which
		 * is populated by the Query Processor
		 */
		
		
		/*
		 * Instantiate JsonWriter class. This class is responsible for writing the
		 * ResultSet into a JSON file
		 */
		
		/*
		 * call executeQuery() method of Query class to get the resultSet. Pass this
		 * resultSet as parameter to writeToJson() method of JsonWriter class to write
		 * the resultSet into a JSON file
		 */
		

	}
}