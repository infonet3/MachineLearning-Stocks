/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Modeling;

import ML_Formulas.LinearRegFormulas;
import ML_Formulas.LogisticRegFormulas;
import MatrixOps.Matrix;
import MatrixOps.MatrixValues;
import MatrixOps.RecordType;
import static Modeling.ModelTypes.LINEAR_REG;
import static Modeling.ModelTypes.LOGIST_REG;
import StockData.StockDataHandler;

/**
 *
 * @author Matt Jones
 */
public class Optimization implements Runnable {

    private String ticker;
    private ModelTypes modelType;
    private int daysInFuture;
    private double[] thetaValues;
    
    public Optimization(String ticker, ModelTypes modelType, int daysInFuture) {
        this.ticker = ticker;
        this.modelType = modelType;
        this.daysInFuture = daysInFuture;
    }
            
    public void run() {
        
        try {
            //Model Settings
            final RecordType TRAINING = RecordType.TRAINING;
            final RecordType CROSS_VAL = RecordType.CROSS_VALIDATION;
            final RecordType TEST = RecordType.TEST;
            
            //Pull data for this stock from the DB and save to class field
            MatrixValues matrixValues = Matrix.loadMatrixFromDB(ticker, daysInFuture, modelType, null, null);
            double[] averages = matrixValues.getOriginalFeatureAverages();
            double[] ranges = matrixValues.getOriginalFeatureRanges();

            //Calculate costs for different sizes of lambda
            double trainingCost = 0.0;
            double crossValCost = 0.0;
            double testCost = 0.0;
            double finalLambda = 0.0;

            RunModels model = new RunModels();
            thetaValues = model.getThetaForModel(matrixValues, modelType, ticker, daysInFuture, finalLambda);
            
            switch (modelType) {
                case LINEAR_REG:
                    //Save cost values for all 3 datasets (Training, Cross Val, Test)
                    trainingCost = LinearRegFormulas.costFunction(matrixValues.getFeatures(TRAINING), thetaValues, matrixValues.getOutputValues(TRAINING), finalLambda);
                    crossValCost = LinearRegFormulas.costFunction(matrixValues.getFeatures(CROSS_VAL), thetaValues, matrixValues.getOutputValues(CROSS_VAL), finalLambda);
                    testCost = LinearRegFormulas.costFunction(matrixValues.getFeatures(TEST), thetaValues, matrixValues.getOutputValues(TEST), finalLambda);

                    break;
                case LOGIST_REG:
                    //Save cost values for all 3 datasets (Training, Cross Val, Test)
                    trainingCost = LogisticRegFormulas.costFunction(matrixValues.getFeatures(TRAINING), thetaValues, matrixValues.getOutputValues(TRAINING), finalLambda);
                    crossValCost = LogisticRegFormulas.costFunction(matrixValues.getFeatures(CROSS_VAL), thetaValues, matrixValues.getOutputValues(CROSS_VAL), finalLambda);
                    testCost = LogisticRegFormulas.costFunction(matrixValues.getFeatures(TEST), thetaValues, matrixValues.getOutputValues(TEST), finalLambda);
                    
                    break;
            }

            //Save values to DB
            StockDataHandler sdh = new StockDataHandler();
            sdh.setModelValues(ticker, modelType.toString(), daysInFuture, thetaValues, averages, ranges, finalLambda, trainingCost, crossValCost, testCost);

            
        } catch (Exception exc) {
            System.out.println(exc);
        }
                   
    }

    public double[] getThetaValues() {
        return thetaValues;
    }
}
