package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IntSummaryStatistics;
import java.util.LinkedHashMap;
import java.util.List;

import com.stackroute.datamunger.query.DataSet;
import com.stackroute.datamunger.query.DataTypeDefinitions;
import com.stackroute.datamunger.query.Filter;
import com.stackroute.datamunger.query.Header;
import com.stackroute.datamunger.query.Row;
import com.stackroute.datamunger.query.RowDataTypeDefinitions;
import com.stackroute.datamunger.query.parser.AggregateFunction;
import com.stackroute.datamunger.query.parser.QueryParameter;

/* this is the CsvAggregateQueryProcessor class used for evaluating queries with 
 * aggregate functions without group by clause*/
public class CsvAggregateQueryProcessor implements QueryProcessingEngine {
	/*
	 * This method will take QueryParameter object as a parameter which contains the
	 * parsed query and will process and populate the ResultSet
	 */
	public HashMap getResultSet(QueryParameter queryParameter) {
		DataSet dataSet = new DataSet();
		/*
		 * initialize BufferedReader to read from the file which is mentioned in
		 * QueryParameter. Consider Handling Exception related to file reading.
		 */

		String fileName = queryParameter.getFile();
		Header headerClass = new Header();
		RowDataTypeDefinitions rowDataType = new RowDataTypeDefinitions();
		FileReader file;
		String header1 = null;
		String row1 = null;
		List<String> rows = new ArrayList<>();
		/*
		 * read the first line which contains the header. Please note that the headers
		 * can contain spaces in between them. For eg: city, winner
		 */

		try {
			file = new FileReader(fileName);
			final BufferedReader br = new BufferedReader(file);
			while ((row1 = br.readLine()) != null) {

				rows.add(row1);
			}

			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Reading header
		header1 = rows.get(0);
		String[] headers = header1.split(",");
		// HashMap<String, Integer> header = new HashMap<String, Integer>();
		for (int i = 0; i < headers.length; i++) {
			headerClass.put(headers[i].trim(), i + 1);
		}
		// headerClass.setHeader(header);

		System.out.println("---headers----");
		System.out.println(Arrays.toString(headers));

		// Reading data type and assigning to RowDataTypeDefinition
		row1 = rows.get(1);
		HashMap<Integer, String> rowDataTypeMap = new HashMap<Integer, String>();
		if (row1 != null) {
			if (row1.lastIndexOf(",") == (row1.length() - 1)) {
				row1 = row1 + " ";
			}
			final String[] data = row1.split(",");
			System.out.println("row:" + row1);

			for (int i = 0; i < data.length; i++) {

				rowDataType.put(i + 1, (DataTypeDefinitions.getDataType(data[i])).toString());
			}
		}
		// rowDataType.setRowDataTypeDefinitions(rowDataTypeMap);
		// Evaluating the row

		LinkedHashMap<Long, Row> dataSetMap = new LinkedHashMap<Long, Row>();
		int rowCount = 1;
		for (int i = 1; i < rows.size(); i++) {
			Row r = new Row();
			String line = rows.get(i);
			// System.out.println("aa");
			if (line.lastIndexOf(",") == (line.length() - 1)) {
				line = line + " ";
			}
			String[] row = line.split(",");
			for (int l = 0; l < row.length; l++) {
				row[l] = row[l].trim();
			}
			// Filtering row
			Boolean flag = null;
			if (queryParameter.getRestrictions() != null) {
				flag = Filter.filter(queryParameter.getRestrictions(), queryParameter.getLogicalOperators(), row,
						headerClass, rowDataType);
			} else {
				flag = true;
			}
			// Initializing Row class
			if (flag) {
				List<Integer> colIndexes = new ArrayList<>();
				List<String> fields = queryParameter.getFields();
				String[] fieldVal=new String[fields.size()];
				for(int m=0;m<fields.size();m++) {
				if (fields.get(m).contains("sum") || fields.get(m).contains("max")
						|| fields.get(m).contains("min") || fields.get(m).contains("count") || fields.get(m).contains("avg")) {
					
					String[] split = (fields.get(m).replace("(", " ")).trim().split(" ");
					System.out.println(split[1].replace(")", "").trim());
					fieldVal[m]=split[1].replace(")", "").trim();
				}
				else {
					fieldVal[m]=fields.get(m);
				}
					}
				fields.clear();
				for(int m=0;m<fieldVal.length;m++) {
					fields.add(fieldVal[m]);
				}
				
				
				if (fields.get(0).equals("*")) {

					for (int k = 0; k < headers.length; k++) {
						r.put(headers[k], row[k]);
					}
				} else {
					HashSet<String> set= new HashSet();
					for(int l=0;l<fields.size();l++) {
						set.add(fields.get(l));
					}
					fields.clear();
					fields.addAll(set);
					// setting rows for fields
					for (int k = 0; k < fields.size(); k++) {

						colIndexes.add(headerClass.get(fields.get(k)));
					}
					Collections.sort(colIndexes);
					for (int k = 0; k < colIndexes.size(); k++) {
						r.put(headers[colIndexes.get(k) - 1], row[colIndexes.get(k) - 1]);
					}

				}
				// r.setRow(rowData);
				// System.out.println("adding row data:"+r);
				dataSet.put((long) (rowCount), r);
				// System.out.println("DataSetMap:"+dataSetMap);
				rowCount++;
			}

		}

		return getAgregateRowValues(dataSet,queryParameter.getAggregateFunction(),headerClass,rowDataType);

	
	}

	public static HashMap getAgregateRowValues(DataSet dataset, List<AggregateFunction> aggregateFunction,
			Header header, RowDataTypeDefinitions rowDataType) {
		HashMap<String ,Object> map= new HashMap<String ,Object>();
		for (int j = 0; j < aggregateFunction.size(); j++) {
			String field = aggregateFunction.get(j).getField();
			String function = aggregateFunction.get(j).getFunction();
			Integer num=0;
			Double d=0.00;
			if ((rowDataType.get(header.get(field))).equals(num.getClass().getName())) {
				DecimalFormat formatter = new DecimalFormat("0.000");
				List<Integer> values = new ArrayList<>();
				for (int i = 1; i <=dataset.size(); i++) {

					values.add(Integer.valueOf((dataset.get((long) i)).get(field)));
				}
				IntSummaryStatistics summaryStatistics = values.stream().mapToInt(m -> m).summaryStatistics();
				if (function.equals("min")) {
					map.put(function+"("+field+")",summaryStatistics.getMin());
				} else if (function.equals("max")) {
					map.put(function+"("+field+")",summaryStatistics.getMax());
				}
				else if (function.equals("sum")) {
					Double val= Double.valueOf(summaryStatistics.getSum());
					formatter.format(val);
					map.put(function+"("+field+")",val);
				}
				else if (function.equals("avg")) {
					Double val= Double.valueOf(summaryStatistics.getAverage());
					formatter.format(val);
					map.put(function+"("+field+")",val);
				}
				else if (function.equals("count")) {
					map.put(function+"("+field+")",summaryStatistics.getCount());
				}
			}
			else if((rowDataType.get(header.get(field))).equals(d.getClass().getName())) {
				DecimalFormat formatter = new DecimalFormat("0.000");
				List<Double> values = new ArrayList<>();
				for (int i = 1; i <= dataset.size(); i++) {

					values.add(Double.valueOf((dataset.get((long) i)).get(field)));
				}
				DoubleSummaryStatistics summaryStatistics = values.stream().mapToDouble(m -> m).summaryStatistics();
				
				if (function.equals("min")) {
					
					map.put(function+"("+field+")",summaryStatistics.getMin());
				} else if (function.equals("max")) {
					map.put(function+"("+field+")",summaryStatistics.getMax());
				}
				else if (function.equals("sum")) {
					Double val=Double.valueOf(summaryStatistics.getSum());
					formatter.format(val);
					map.put(function+"("+field+")",val);
				}
				else if (function.equals("avg")) {
					Double val=Double.valueOf(summaryStatistics.getAverage());
					formatter.format(val);
					map.put(function+"("+field+")",val);
					map.put(function+"("+field+")",val);
				}
				else if (function.equals("count")) {
					map.put(function+"("+field+")",summaryStatistics.getCount());
				}
			}
			else {
				List<String> values = new ArrayList<>();
				for (int i = 1; i <= dataset.size(); i++) {

					values.add((dataset.get((long) i)).get(field));
				}
				
				List<Integer> nullValueIndex=new ArrayList<>();
				for(int k=0;k<values.size();k++) {
					if(values.get(k).equals("")) {
						nullValueIndex.add(k);
					}
				}
				for(int nulVal:nullValueIndex) {
					values.remove(nulVal);
				}
				
				 if (function.equals("count")) {
					map.put(function+"("+field+")",values.size());
				}
			}
		}
		return map;
	}
	
}