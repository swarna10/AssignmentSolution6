package com.stackroute.datamunger.writer;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
public class JsonWriter {
	/*
	 * this method will write the resultSet object into a JSON file. On successful
	 * writing, the method will return true, else will return false
	 */
	public boolean writeToJson(Map resultSet) {
		Boolean flag=false;
		/*
		 * Gson is a third party library to convert Java object to JSON. We will use
		 * Gson to convert resultSet object to JSON
		 */
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		String result = gson.toJson(resultSet);
		
		try (BufferedWriter bufferwritter = new BufferedWriter(new FileWriter("data/result.json"))) {
            bufferwritter.write(result);
            bufferwritter.close();
            flag=true;
        } catch (IOException exception) 
        {
            System.out.println(exception);
        }
     
		return flag;
	}
}