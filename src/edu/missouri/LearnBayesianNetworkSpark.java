package edu.missouri;

import edu.missouri.constants.Constants;
import edu.missouri.util.CSV2Arff;
import eu.amidst.core.datastream.DataInstance;
import eu.amidst.core.datastream.DataStream;
import eu.amidst.core.models.BayesianNetwork;
import eu.amidst.core.models.DAG;
import eu.amidst.core.utils.DAGGenerator;
import eu.amidst.core.variables.Variable;
import eu.amidst.core.variables.Variables;
import eu.amidst.sparklink.core.io.DataSparkLoader;
import eu.amidst.sparklink.core.learning.ParallelMaximumLikelihood;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;
import eu.amidst.sparklink.core.data.DataSpark;


public class LearnBayesianNetworkSpark {
    public static DAG getNaiveBayesStructure(DataStream<DataInstance> dataStream, int classIndex){

        // Creating a Variables object from the attributes of the data stream
        Variables modelHeader = new Variables(dataStream.getAttributes());

        // Define the predictive class variable
        Variable classVar = modelHeader.getVariableById(classIndex);

        // Creating a DAG object with the defined model header
        DAG dag = new DAG(modelHeader);

        //We set the links of the DAG.
        dag.getParentSets().stream().filter(w -> w.getMainVar() != classVar).forEach(w -> w.addParent(classVar));

        return dag;
    }

    public static void main(String[] args) throws Exception {
        if(args.length != 1) {
            System.out.println("edu.missouri.LearnBayesianNetwork <INPUT>");
            System.exit(-1);
        }

        // Obtaining the input file.
        String input = args[0];

        // If the input is a CSV, we convert it to as ARFF.
        String[] fileSplits = input.split("\\.");
        if(fileSplits[fileSplits.length-1].equals(Constants.CSV_EXTENSION)) {
            input = CSV2Arff.getInstance().convertCSV2Arff(input);
        }

        // Validating the conversion.
        if(input == null) {
            System.out.println("LearnBayesianNetwork :: main :: Invalid input provided.");
            System.exit(-1);
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
