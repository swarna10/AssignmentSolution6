package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IntSummaryStatistics;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import com.stackroute.datamunger.query.DataSet;
import com.stackroute.datamunger.query.DataTypeDefinitions;
import com.stackroute.datamunger.query.Filter;
import com.stackroute.datamunger.query.GroupedDataSet;
import com.stackroute.datamunger.query.Header;
import com.stackroute.datamunger.query.Row;
import com.stackroute.datamunger.query.RowDataTypeDefinitions;
import com.stackroute.datamunger.query.parser.AggregateFunction;
import com.stackroute.datamunger.query.parser.QueryParameter;

/* this is the CsvGroupByAggregateQueryProcessor class used for evaluating queries with 
 * aggregate functions and group by clause*/
public class CsvGroupByAggregateQueryProcessor implements QueryProcessingEngine {
	/*
	 * This method will take QueryParameter object as a parameter which contains the
	 * parsed query and will process and populate the ResultSet
	 */
	public HashMap getResultSet(QueryParameter queryParameter) {
		
		DataSet dataSet=new DataSet();
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
		
		//Reading header
		header1 = rows.get(0);
		String[] headers = header1.split(",");
		//HashMap<String, Integer> header = new HashMap<String, Integer>();
		for (int i = 0; i < headers.length; i++) {
			headerClass.put(headers[i].trim(), i + 1);
		}
		//headerClass.setHeader(header);
		
		System.out.println("---headers----");
		System.out.println(Arrays.toString(headers));
		
		//Reading data type and assigning to RowDataTypeDefinition
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
		//rowDataType.setRowDataTypeDefinitions(rowDataTypeMap);
		// Evaluating the row
	
		LinkedHashMap<Long, Row> dataSetMap=new LinkedHashMap<Long, Row>(); 
		int rowCount=1;
		for (int i = 1; i < rows.size(); i++) {
			Row r= new Row();
			String line = rows.get(i);
			//System.out.println("aa");
			if (line.lastIndexOf(",") == (line.length() - 1)) {
				line = line + " ";
			}
			String[] row = line.split(",");
			for(int l=0;l<row.length;l++) {
				row[l]=row[l].trim();
			}
			//Filtering row
			Boolean flag =null;
			if(queryParameter.getRestrictions()!=null) {
			 flag = Filter.filter(queryParameter.getRestrictions(), queryParameter.getLogicalOperators(), row,
					headerClass,rowDataType);
			}
			else {
				flag=true;
			}
			//Initializing Row class 
			 if(flag) {
			List<Integer> colIndexes=new ArrayList<>();
			List<String> fields=queryParameter.getFields();
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
			
			boolean addAllRows=false;
			if(fields.get(0).equals("*")) {
				
				
				for(int k=0;k<headers.length;k++ ) {
					r.put(headers[k],row[k] );
				}
			}
			else {
				//setting rows for fields
				for(int k=0;k<fields.size();k++) {
					if(fields.get(k).equals("*")) {
						addAllRows=true;
					}
					else {
					colIndexes.add(headerClass.get(fields.get(k)));
					}
				}
				try {
				Collections.sort(colIndexes);
				}
				catch (Exception e) {
					// TODO: handle exception
				}
				for(int k=0;k<colIndexes.size();k++ ) {
					r.put(headers[colIndexes.get(k)-1],row[colIndexes.get(k)-1] );
				}
				if(addAllRows) {
				for(int k=0;k<headers.length;k++ ) {
					r.put(headers[k],row[k] );
				}
				}
			}
			
			
			//r.setRow(rowData);
			//System.out.println("adding row data:"+r);
			dataSet.put((long)(rowCount), r)	;
			//System.out.println("DataSetMap:"+dataSetMap);
			rowCount++;
			 }
		}
		GroupedDataSet groupedDataSet=getgroupedData(dataSet,queryParameter.getAggregateFunction(),headerClass,rowDataType,queryParameter.getGroupByFields());
		
		return groupedDataSet;
	}
	
	public static GroupedDataSet getgroupedData(DataSet dataset, List<AggregateFunction> aggregateFunction,
			Header header, RowDataTypeDefinitions rowDataType,List<String> groupByFields) {
		GroupedDataSet groupedDataSet=new GroupedDataSet();
		
		for(int k=0;k<groupByFields.size();k++) {
			HashMap<String, List<Row>> groupedDataMap=new LinkedHashMap<String, List<Row>>();
			String groupByField=groupByFields.get(k);
			int groupByFieldIndex=header.get(groupByField);
			int distictRows=getDistinctValuesOfColumnFromDataSet(dataset,groupByField).size();
			List<List<Row>> rowLists=new ArrayList<List<Row>>();
			List<String> distinctValues=getDistinctValuesOfColumnFromDataSet(dataset,groupByField);
			for(int i=0;i<distictRows;i++) {
				rowLists.add(i, new ArrayList<Row>());
			}
			for(int i=0;i<dataset.size();i++) {
				int ListIndex=distinctValues.indexOf(dataset.get((long) (i+1)).get(groupByField));
				rowLists.get(ListIndex).add(dataset.get((long) (i+1)));
			}
			for(int i=0;i<rowLists.size();i++) {
				groupedDataMap.put(distinctValues.get(i), rowLists.get(i));
			}
			setValuesToGroupedDataSet(groupedDataSet,groupedDataMap,aggregateFunction,header,rowDataType);	
			
	}
		return groupedDataSet;
	}
	
	public static List<String> getDistinctValuesOfColumnFromDataSet(DataSet dataset,String field) {
		HashSet<String> hashset= new HashSet<String>();
		List<String> distinctValues=new ArrayList<>();
		for(int i=0;i<dataset.size();i++) {
			hashset.add(dataset.get((long)(i+1)).get(field));
			
		}
		distinctValues.addAll(hashset);
		return distinctValues;
	}
	
	public static GroupedDataSet setValuesToGroupedDataSet(GroupedDataSet groupeddataset,HashMap<String, List<Row>> groupedDataMap, List<AggregateFunction> aggregateFunction,
			Header header, RowDataTypeDefinitions rowDataType) {
		HashMap<String ,Object> map= new HashMap<String ,Object>();
		for (int j = 0; j < aggregateFunction.size(); j++) {
			String field = aggregateFunction.get(j).getField();
			String function = aggregateFunction.get(j).getFunction();
			List<String> distinctGroupedValues=new ArrayList<>();
			distinctGroupedValues.addAll(groupedDataMap.keySet());
			Set<String> headers=header.keySet();
			List<String> headersValues=new ArrayList<>();
			headersValues.addAll(headers);
			
			Integer num=0;
			Double d=0.00;
			if(field.equals("*")) {
				for(int k=0;k<groupedDataMap.size();k++) {
					for(int l=0;l<header.size();l++) {
						
					if ((rowDataType.get(header.get(headersValues.get(l)))).equals(num.getClass().getName())) {
						List<Integer> values = new ArrayList<>();
						
						for (int i = 0; i <groupedDataMap.get(distinctGroupedValues.get(k)).size(); i++) {

							values.add(Integer.valueOf(groupedDataMap.get(distinctGroupedValues.get(k)).get(i).get(headersValues.get(l))));
						}
						IntSummaryStatistics summaryStatistics = values.stream().mapToInt((x) -> x).summaryStatistics();
						if (function.equals("min")) {
							groupeddataset.put(distinctGroupedValues.get(k),summaryStatistics.getMin());
						} else if (function.equals("max")) {
							groupeddataset.put(distinctGroupedValues.get(k),summaryStatistics.getMax());
						}
						else if (function.equals("sum")) {
							groupeddataset.put(distinctGroupedValues.get(k),summaryStatistics.getSum());
						}
						else if (function.equals("avg")) {
							groupeddataset.put(distinctGroupedValues.get(k),summaryStatistics.getAverage());
						}
						else if (function.equals("count")) {
							groupeddataset.put(distinctGroupedValues.get(k),summaryStatistics.getCount());
						}
					}
					else	if ((rowDataType.get(header.get(headersValues.get(l)))).equals(d.getClass().getName())){
						List<Double> values = new ArrayList<>();
						for (int i = 0; i <groupedDataMap.get(distinctGroupedValues.get(k)).size(); i++) {
							values.add(Double.valueOf(groupedDataMap.get(distinctGroupedValues.get(k)).get(i).get(headersValues.get(l))));
						}
						DoubleSummaryStatistics summaryStatistics = values.stream().mapToDouble(m -> m).summaryStatistics();
						if (function.equals("min")) {
							groupeddataset.put(distinctGroupedValues.get(k),summaryStatistics.getMin());
						} else if (function.equals("max")) {
							groupeddataset.put(distinctGroupedValues.get(k),summaryStatistics.getMax());
						}
						else if (function.equals("sum")) {
							groupeddataset.put(distinctGroupedValues.get(k),summaryStatistics.getSum());
						}
						else if (function.equals("avg")) {
							groupeddataset.put(distinctGroupedValues.get(k),summaryStatistics.getAverage());
						}
						else if (function.equals("count")) {
							groupeddataset.put(distinctGroupedValues.get(k),summaryStatistics.getCount());
						}
					}
					else {
						 if (function.equals("count")) {
							 groupeddataset.put(distinctGroupedValues.get(k),groupedDataMap.get(distinctGroupedValues.get(k)).size());
						}
					}
				}	
				}
				
			}
			else {
			for(int k=0;k<groupedDataMap.size();k++) {
			if ((rowDataType.get(header.get(field))).equals(num.getClass().getName())) {
				List<Integer> values = new ArrayList<>();
				for (int i = 0; i <groupedDataMap.get(distinctGroupedValues.get(k)).size(); i++) {

					values.add(Integer.valueOf(groupedDataMap.get(distinctGroupedValues.get(k)).get(i).get(field)));
				}
				IntSummaryStatistics summaryStatistics = values.stream().mapToInt((x) -> x).summaryStatistics();
				if (function.equals("min")) {
					groupeddataset.put(distinctGroupedValues.get(k),summaryStatistics.getMin());
				} else if (function.equals("max")) {
					groupeddataset.put(distinctGroupedValues.get(k),summaryStatistics.getMax());
				}
				else if (function.equals("sum")) {
					groupeddataset.put(distinctGroupedValues.get(k),summaryStatistics.getSum());
				}
				else if (function.equals("avg")) {
					groupeddataset.put(distinctGroupedValues.get(k),summaryStatistics.getAverage());
				}
				else if (function.equals("count")) {
					groupeddataset.put(distinctGroupedValues.get(k),summaryStatistics.getCount());
				}
			}
			else if((rowDataType.get(header.get(field))).equals(d.getClass().getName())) {
				List<Double> values = new ArrayList<>();
				for (int i = 0; i <groupedDataMap.get(distinctGroupedValues.get(k)).size(); i++) {

					values.add(Double.valueOf(groupedDataMap.get(distinctGroupedValues.get(k)).get(i).get(field)));
				}
				DoubleSummaryStatistics summaryStatistics = values.stream().mapToDouble(m -> m).summaryStatistics();
				if (function.equals("min")) {
					groupeddataset.put(distinctGroupedValues.get(k),summaryStatistics.getMin());
				} else if (function.equals("max")) {
					groupeddataset.put(distinctGroupedValues.get(k),summaryStatistics.getMax());
				}
				else if (function.equals("sum")) {
					groupeddataset.put(distinctGroupedValues.get(k),summaryStatistics.getSum());
				}
				else if (function.equals("avg")) {
					groupeddataset.put(distinctGroupedValues.get(k),summaryStatistics.getAverage());
				}
				else if (function.equals("count")) {
					groupeddataset.put(distinctGroupedValues.get(k),summaryStatistics.getCount());
				}
			}
			else {
				 if (function.equals("count")) {
					 groupeddataset.put(distinctGroupedValues.get(k),groupedDataMap.get(distinctGroupedValues.get(k)).size());
				}
			}
		}
		}
		}
		return groupeddataset;
	}
}