package edu.missouri.util;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.dataformat.csv.*;
import edu.missouri.constants.Constants;

import java.io.File;
import java.io.FileWriter;
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

        // Creating an out path based on the input.
        StringBuffer sb = new StringBuffer();
        for(int i=0; i<splits.length - 1; i++) {
            sb.append(splits[i]);
        }
        sb.append(Constants.DOT + Constants.JSON_EXTENSION);

        try (FileWriter fileWriter = new FileWriter(new File(sb.toString()))) {
            // Reading the contents of the CSV and converting it to a JSON.
            CsvSchema csvSchema = CsvSchema.emptySchema().withHeader();
            CsvMapper csvMapper = new CsvMapper();
            MappingIterator<Map<?, ?>> mappingIterator =  csvMapper.reader().forType(Map.class).with(csvSchema).readValues(new File(csvInPath));
            List<Map<?, ?>> list = mappingIterator.readAll();

            // Writing the output to a file.
            fileWriter.write(String.valueOf(list));
        } catch(Exception e) {
            System.out.println("CSV2JSON :: convertCSV2JSON :: Exception encountered converting to a JSON.");
            e.printStackTrace();
        }

        System.out.println("CSV2JSON :: convertCSV2JSON :: Successfully converted " + sb.toString());
        return sb.toString();

    }

    public static void main(String[] args) {
        if(args.length != 1) {
            System.out.println("edu.missouri.util.CSV2JSON <INPUT>");
            System.exit(-1);
        }

        // Obtaining the input file.
        String input = args[0];

        // If the input is a CSV, we convert it to a JSON.
        String[] fileSplits = input.split("\\.");
        if(fileSplits[fileSplits.length-1].equals(Constants.CSV_EXTENSION)) {
            try {
                input = CSV2JSON.getInstance().convertCSV2JSON(input);
            } catch(Exception e) {
                System.out.println("CSV2JSON :: main :: Exception encountered while converting.");
                e.printStackTrace();
            }

        }

        // Validating the conversion.
        if(input == null) {
            System.out.println("CSV2JSON :: main :: Invalid input provided.");
            System.exit(-1);
        }

    }

}
