package edu.missouri;

import eu.amidst.core.datastream.DataInstance;
import eu.amidst.core.datastream.DataOnMemory;
import eu.amidst.core.datastream.DataStream;
import eu.amidst.core.io.DataStreamLoader;
import eu.amidst.core.learning.parametric.ParallelMaximumLikelihood;
import eu.amidst.core.learning.parametric.ParameterLearningAlgorithm;
import eu.amidst.core.models.BayesianNetwork;
import eu.amidst.core.models.DAG;
import eu.amidst.core.variables.Variable;
import eu.amidst.core.variables.Variables;

public class LearnBayesianNetwork {
    public static DAG getNaiveBayesStructure(DataStream<DataInstance> dataStream, int classIndex){

    //We create a Variables object from the attributes of the data stream
    Variables modelHeader = new Variables(dataStream.getAttributes());

    //We define the predicitive class variable
    Variable classVar = modelHeader.getVariableById(classIndex);

    //Then, we create a DAG object with the defined model header
    DAG dag = new DAG(modelHeader);

    //We set the linkds of the DAG.
    dag.getParentSets().stream().filter(w -> w.getMainVar() != classVar).forEach(w -> w.addParent(classVar));

    return dag;
}


    public static void main(String[] args) throws Exception {

        //We can open the data stream using the static class DataStreamLoader
        DataStream<DataInstance> data = DataStreamLoader.open("datasets/simulated/WasteIncineratorSample.arff");

        //We create a ParameterLearningAlgorithm object with the MaximumLikehood builder
        ParameterLearningAlgorithm parameterLearningAlgorithm = new ParallelMaximumLikelihood();

        //We fix the DAG structure
        parameterLearningAlgorithm.setDAG(getNaiveBayesStructure(data,0));

        //We should invoke this method before processing any data
        parameterLearningAlgorithm.initLearning();


        //Then we show how we can perform parameter learnig by a sequential updating of data batches.
        for (DataOnMemory<DataInstance> batch : data.iterableOverBatches(100)){
            parameterLearningAlgorithm.updateModel(batch);
        }

        //And we get the model
        BayesianNetwork bnModel = parameterLearningAlgorithm.getLearntBayesianNetwork();

        //We print the model
        System.out.println(bnModel.toString());

    }
}
