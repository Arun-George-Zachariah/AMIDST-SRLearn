package edu.missouri.util;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import edu.missouri.constants.Constants;

import java.util.List;
import java.util.Map;

public class CSV2JSON {
    public static CSV2JSON instance = null;

    private CSV2JSON() {

    }

    public static CSV2JSON getInstance() {
        if(instance == null) {
            instance = new CSV2JSON();
        }
        return instance;
    }


    public static String convertCSV2JSON(String csvInPath) throws Exception {
        String[] splits = csvInPath.split("\\.");

        // Validating the input.
        if(!splits[splits.length-1].equals(Constants.CSV_EXTENSION)) {
            System.out.println("CSV2JSON :: convertCSV2JSON :: Extension not ." + Constants.CSV_EXTENSION);
            return null;
        }

        try {
            CsvSchema csv = CsvSchema.emptySchema().withHeader();
            CsvMapper csvMapper = new CsvMapper();
            MappingIterator<Map<?, ?>> mappingIterator =  csvMapper.reader().forType(Map.class).with(csv).readValues(csvInPath);
            List<Map<?, ?>> list = mappingIterator.readAll();
            System.out.println(list);
        } catch(Exception e) {
            e.printStackTrace();
        }

        return null;

    }
}
