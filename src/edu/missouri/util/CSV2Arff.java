// Ref: https://waikato.github.io/weka-wiki/formats_and_processing/converting_csv_to_arff/
package edu.missouri.util;

import edu.missouri.Constants.Constants;
import weka.core.Instances;
import weka.core.converters.ArffSaver;
import weka.core.converters.CSVLoader;

import java.io.File;

public class CSV2Arff {

    public static CSV2Arff instance = null;

    private CSV2Arff() {

    }

    public static CSV2Arff getInstance() {
        if(instance == null) {
            instance = new CSV2Arff();
        }
        return instance;
    }


    public static String convertCSV2Arff(String csvInPath) throws Exception {
        String[] splits = csvInPath.split(csvInPath);

        // Validating the input.
        if(splits[splits.length - 1] != Constants.CSV_EXTENSION) {
            System.out.println("CSV2Arff :: convertCSV2Arff :: Extension not ." + Constants.CSV_EXTENSION);
            return null;
        }

        // Creating an out path based on the input.
        StringBuffer sb = new StringBuffer();
        for(int i=0; i<splits.length - 1; i++) {
            sb.append(splits[i]);
        }
        sb.append(Constants.ARFF_EXTENSION);


        // Loading the CSV.
        CSVLoader loader = new CSVLoader();
        loader.setSource(new File(csvInPath));
        Instances data = loader.getDataSet();

        // Converting it to ARFF and saving the file.
        ArffSaver saver = new ArffSaver();
        saver.setInstances(data);
        saver.setFile(new File(sb.toString()));
        saver.setDestination(new File(sb.toString()));
        saver.writeBatch();

        System.out.println("CSV2Arff :: convertCSV2Arff :: Successfully converted to ." + Constants.CSV_EXTENSION);

        return sb.toString();
    }
}
