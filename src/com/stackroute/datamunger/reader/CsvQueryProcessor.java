package com.stackroute.datamunger.reader;

import java.io.BufferedReader;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import com.stackroute.datamunger.query.DataSet;
import com.stackroute.datamunger.query.DataTypeDefinitions;
import com.stackroute.datamunger.query.Filter;
import com.stackroute.datamunger.query.GenericComparator;
import com.stackroute.datamunger.query.Header;
import com.stackroute.datamunger.query.Row;
import com.stackroute.datamunger.query.RowDataTypeDefinitions;
import com.stackroute.datamunger.query.parser.QueryParameter;
import com.stackroute.datamunger.query.parser.Restriction;

public class CsvQueryProcessor implements QueryProcessingEngine {
	/*
	 * This method will take QueryParameter object as a parameter which contains the
	 * parsed query and will process and populate the ResultSet
	 */
	public  DataSet getResultSet(QueryParameter queryParameter) {
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
		List<Row> filteredRows=new ArrayList<Row>();
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
				Collections.sort(colIndexes);
				for(int k=0;k<colIndexes.size();k++ ) {
					r.put(headers[colIndexes.get(k)-1],row[colIndexes.get(k)-1] );
				}
				
			}
			//r.setRow(rowData);
			//System.out.println("adding row data:"+r);
			//dataSet.put((long)(rowCount), r)	;
			filteredRows.add(r);
			//System.out.println("DataSetMap:"+dataSetMap);
			rowCount++;
			 }
		}
		if(queryParameter.getOrderByFields().size()!=0) {
			String orderByField=queryParameter.getOrderByFields().get(0);
			String dataType=rowDataType.get(headerClass.get(orderByField));
			Collections.sort(filteredRows,new GenericComparator(orderByField,dataType,1));
		}
		for(int i=0;i<filteredRows.size();i++) {
			dataSet.put((long)(i+1), filteredRows.get(i));
		}
		return dataSet;
	}
	
	
	

}