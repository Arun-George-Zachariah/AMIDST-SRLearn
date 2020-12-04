package edu.missouri;

import edu.missouri.constants.Constants;
import edu.missouri.util.CSV2JSON;
import eu.amidst.core.models.BayesianNetwork;
import eu.amidst.core.utils.DAGGenerator;
import eu.amidst.sparklink.core.io.DataSparkLoader;
import eu.amidst.sparklink.core.learning.ParallelMaximumLikelihood;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;
import eu.amidst.sparklink.core.data.DataSpark;


public class LearnBayesianNetworkSpark {

    public static void main(String[] args) throws Exception {
        if(args.length != 1) {
            System.out.println("edu.missouri.LearnBayesianNetworkSpark <INPUT>");
            System.exit(-1);
        }

        // Obtaining the input file.
        String input = args[0];

        // Validating the input.
        String[] fileSplits = input.split("\\.");
        if(!fileSplits[fileSplits.length-1].equals(Constants.JSON_EXTENSION)) {
            System.out.println("LearnBayesianNetworkSpark :: main :: Invalid input provided. Input needs to be a .json");
        }

        // Creating the Spark context.
        SparkConf conf = new SparkConf().setAppName(Constants.APP_NAME).setMaster(Constants.MASTER);
        SparkContext sc = new SparkContext(conf);

        // Creating the Spark SQL context.
        SQLContext sqlContext = new SQLContext(sc);

        // Opening the data stream.
        DataSpark dataSpark = DataSparkLoader.open(sqlContext, input);

        // Creating a ParallelMaximumLikelihood object.
        ParallelMaximumLikelihood parameterLearningAlgorithm = new ParallelMaximumLikelihood();

        // Fixing the DAG structure.
        parameterLearningAlgorithm.setDAG(DAGGenerator.getNaiveBayesStructure(dataSpark.getAttributes(), "W"));

        // Setting the batch size which will be employed to learn the model in parallel.
        parameterLearningAlgorithm.setBatchSize(100);

        // Setting the data to be used for leaning the parameters.
        parameterLearningAlgorithm.setDataSpark(dataSpark);

        // Performing the learning.
        parameterLearningAlgorithm.runLearning();

        // Obtaining the model.
        BayesianNetwork bn = parameterLearningAlgorithm.getLearntBayesianNetwork();

        System.out.println(bn);
    }

}
