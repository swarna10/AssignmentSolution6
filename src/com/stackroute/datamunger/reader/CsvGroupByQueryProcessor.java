package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;

import com.stackroute.datamunger.query.DataSet;
import com.stackroute.datamunger.query.DataTypeDefinitions;
import com.stackroute.datamunger.query.Filter;
import com.stackroute.datamunger.query.GroupedDataSet;
import com.stackroute.datamunger.query.Header;
import com.stackroute.datamunger.query.Row;
import com.stackroute.datamunger.query.RowDataTypeDefinitions;
import com.stackroute.datamunger.query.parser.AggregateFunction;
import com.stackroute.datamunger.query.parser.QueryParameter;

/* this is the CsvGroupByQueryProcessor class used for evaluating queries without 
 * aggregate functions but with group by clause*/
public class CsvGroupByQueryProcessor implements QueryProcessingEngine {
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
		List<Integer> rowIndexes=new ArrayList<>();
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
				if(i==243) {
					System.out.println("aa");
				}
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
			if(fields.get(0).equals("*")) {
				
				
				for(int k=0;k<headers.length;k++ ) {
					r.put(headers[k],row[k] );
				}
			}
			else {
				//setting rows for fields
				for(int k=0;k<fields.size();k++) {
					
					colIndexes.add(headerClass.get(fields.get(k)));
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
				
			}
			rowIndexes.add(i);
			//r.setRow(rowData);
			//System.out.println("adding row data:"+r);
			dataSet.put((long)(rowCount), r)	;
			//System.out.println("DataSetMap:"+dataSetMap);
			rowCount++;
			 }
		}
		HashMap<String, HashMap<Long, Row>> groupedHashMap=	getgroupedData(dataSet,queryParameter.getAggregateFunction(),headerClass,rowDataType,queryParameter.getGroupByFields(),rowIndexes);
		return groupedHashMap;
	
		
		
	}
	
	public static HashMap<String, HashMap<Long, Row>>  getgroupedData(DataSet dataset, List<AggregateFunction> aggregateFunction,
			Header header, RowDataTypeDefinitions rowDataType,List<String> groupByFields,List<Integer> filteredRowIndexes) {
		
		HashMap<String, HashMap<Long, Row>> groupedDataMap=new LinkedHashMap<String, HashMap<Long, Row>>();
		for(int k=0;k<groupByFields.size();k++) {
			
			String groupByField=groupByFields.get(k);
			int groupByFieldIndex=header.get(groupByField);
			int distictRows=getDistinctValuesOfColumnFromDataSet(dataset,groupByField).size();
			//List<List<Row>> rowLists=new ArrayList<List<Row>>();
			List<LinkedHashMap<Long, Row>> dataMaps=new ArrayList<LinkedHashMap<Long,Row>>();
		
			List<String> distinctValues=getDistinctValuesOfColumnFromDataSet(dataset,groupByField);
			for(int i=0;i<distictRows;i++) {
				dataMaps.add(new LinkedHashMap<Long, Row>());
			}
			for(int i=0;i<dataset.size();i++) {
				int ListIndex=distinctValues.indexOf(dataset.get((long) (i+1)).get(groupByField));
				dataMaps.get(ListIndex).put((long)(filteredRowIndexes.get(i)),dataset.get((long) (i+1)));
			}
			for(int i=0;i<dataMaps.size();i++) {
				groupedDataMap.put(distinctValues.get(i), dataMaps.get(i));
			}
				
		
	}
		return 	groupedDataMap;
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
	
	
	
}