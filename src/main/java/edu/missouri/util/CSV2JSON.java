package edu.missouri.util;

import edu.missouri.constants.Constants;
import java.io.*;

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

        try(BufferedReader br = new BufferedReader(new FileReader(new File(csvInPath)));
            BufferedWriter bw = new BufferedWriter(new FileWriter(new File(sb.toString())))) {
            // Obtaining the header.
            String line = br.readLine();
            String[] header = line.split(",");

            // Iterating over the contents.
            while((line = br.readLine()) != null) {
                String[] content = line.split(",");

                // Creating the json.
                StringBuffer jsonBuffer = new StringBuffer();
                jsonBuffer.append("{");
                for(int i=0; i<header.length-1; i++) {
                    jsonBuffer.append("\"" + header[i] + "\":\"" + content[i] + "\",");
                }
                jsonBuffer.append("\"" + header[header.length-1] + "\":\"" + content[header.length-1] + "\"}\n");

                // Writing to the file.
                bw.write(jsonBuffer.toString());
                bw.flush();
            }

        } catch (Exception e) {
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
        }  else {
            System.out.println("CSV2Arff :: main :: Invalid input provided. The input should be .csv");
            System.exit(-1);
        }

        // Validating the conversion.
        if(input == null) {
            System.out.println("CSV2JSON :: main :: Invalid input provided.");
            System.exit(-1);
        }

    }

}
