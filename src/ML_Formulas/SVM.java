/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ML_Formulas;

import MatrixOps.MatrixValues;
import MatrixOps.RecordType;
import libsvm.svm;
import libsvm.svm_model;
import libsvm.svm_node;
import libsvm.svm_parameter;
import libsvm.svm_problem;

/**
 *
 * @author Matt Jones
 */
public class SVM {
    
    public static svm_node[][] convertToNodeMatrix(double[][] inputMatrix) {
        //Convert to svm_nodes
        int rows = inputMatrix.length;
        int cols = inputMatrix[0].length;
        svm_node[][] nodeMatrix = new svm_node[rows][cols];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                nodeMatrix[i][j] = new svm_node();
                nodeMatrix[i][j].index = i + 1;
                nodeMatrix[i][j].value = inputMatrix[i][j];
            }
        }
        
        return nodeMatrix;
    }
    
    public static svm_model createModel(final MatrixValues MATRIX_VAL) {
        
        //Get values from the MatrixValues object
        final RecordType REC_TYPE = RecordType.TRAINING;
        double[][] inputMatrix = MATRIX_VAL.getFeatures(REC_TYPE);

        //Set any 0 values to -1
        double[] results = MATRIX_VAL.getOutputValues(REC_TYPE);
        /*for (int i = 0; i < results.length; i++)
            if (results[i] == 0.0)
                results[i] = -1;
        */

        svm_node[][] nodeMatrix = convertToNodeMatrix(inputMatrix);
        
        svm_problem prob = new svm_problem();
        prob.l = nodeMatrix.length;
        prob.y = results;
        prob.x = nodeMatrix;
        
        //Define model parameters
        svm_parameter param = new svm_parameter();
        param.svm_type = 0;
        param.kernel_type = svm_parameter.RBF;
        param.degree = 3;
        
        //Model fitting params
        param.gamma = 0; //Gamma = 1 / (2 * sigma^2)
        param.C = 0.1;
        
        param.coef0 = 0;
        param.nu = 0.5;
        param.cache_size = 40;
        param.eps = 1e-5;
        param.p = 0.1;
        param.shrinking = 0;
        param.probability = 0;
        param.nr_weight = 0;
        param.weight_label = new int[0];
        param.weight = new double[0];

        //Train model
        svm_model model = svm.svm_train(prob, param);
        //return model;
        
        //Test
        double[][] testMatrix = MATRIX_VAL.getFeatures(RecordType.TEST);
        svm_node[][] testNodeMatrix = convertToNodeMatrix(testMatrix);
        
        double[] testOutputs = MATRIX_VAL.getOutputValues(RecordType.TEST);
        
        int goodPreds = 0;
        for (int i = 0; i < testMatrix.length; i++) {
            double d = svm.svm_predict(model, testNodeMatrix[i]);
            System.out.println(d);
            
            if (d == testOutputs[i])
                goodPreds++;
        }
        
        System.out.println("Accuracy = " + ((double)goodPreds / testMatrix.length));

        return null;
    }
}
