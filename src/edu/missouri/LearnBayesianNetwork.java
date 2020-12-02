package edu.missouri;

import edu.missouri.constants.Constants;
import edu.missouri.util.CSV2Arff;
import eu.amidst.core.datastream.DataInstance;
import eu.amidst.core.datastream.DataStream;
import eu.amidst.core.io.DataStreamLoader;
import eu.amidst.core.learning.parametric.ParallelMaximumLikelihood;
import eu.amidst.core.models.BayesianNetwork;
import eu.amidst.core.models.DAG;
import eu.amidst.core.variables.Variable;
import eu.amidst.core.variables.Variables;

public class LearnBayesianNetwork {
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

        // Open the data stream.
        DataStream<DataInstance> data = DataStreamLoader.open(input);

        // Create a ParallelMaximumLikelihood object.
        ParallelMaximumLikelihood parameterLearningAlgorithm = new ParallelMaximumLikelihood();

        // Activating parallel mode.
        parameterLearningAlgorithm.setParallelMode(true);

        // Deactivating debug mode.
        parameterLearningAlgorithm.setDebug(false);

        // Fixing the DAG structure.
        parameterLearningAlgorithm.setDAG(getNaiveBayesStructure(data, 0));

        // Setting the batch size which will be employed to learn the model in parallel.
        parameterLearningAlgorithm.setWindowsSize(100);

        // Setting the data to be used for leaning the parameters.
        parameterLearningAlgorithm.setDataStream(data);

        // Perform the learning.
        parameterLearningAlgorithm.runLearning();

        // Obtaining the model.
        BayesianNetwork bnModel = parameterLearningAlgorithm.getLearntBayesianNetwork();
        System.out.println(bnModel.toString());

    }

}
