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
import StockData.StockDataHandler;
import StockData.StockTicker;
import java.util.List;

/**
 *
 * @author Matt Jones
 */
public class RunModels {

    //Methods
    public void runModels(final ModelTypes MODEL, final int DAYS_IN_FUTURE) throws Exception {

        testAllStocks(MODEL, DAYS_IN_FUTURE);
    }

    private void getModelError(RecordType type) {
    }
    
    private double[] initializeTheta(int size) {

        double[] theta = new double[size];
        for (int i = 0; i < theta.length; i++) {
            theta[i] = 1.0;
        }

        return theta;
    }
    
    //Run through all stock and determine optimal values of theta for prediction
    private void testAllStocks(final ModelTypes MODEL, final int DAYS_IN_FUTURE) throws Exception {
        
        //Model Settings
        final RecordType TRAINING = RecordType.TRAINING;
        final RecordType CROSS_VAL = RecordType.CROSS_VALIDATION;
        final RecordType TEST = RecordType.TEST;
        final double[] lambdas = {0, 0.001, 0.003, 0.01, 0.03, 0.1, 0.3, 1, 3, 10};

        //Run through all stock tickers
        StockDataHandler sdh = new StockDataHandler();
        List<StockTicker> stockList = sdh.getAllStockTickers(false);
        MatrixValues matrixValues;
        for (StockTicker ticker : stockList) {
            
            //LINEAR REGRESSION==========================================================================================================================
            //Pull data for this stock from the DB and save to class field
            matrixValues = Matrix.loadMatrixFromDB(ticker.getTicker(), DAYS_IN_FUTURE, MODEL);
            double[] averages = matrixValues.getOriginalFeatureAverages();
            double[] ranges = matrixValues.getOriginalFeatureRanges();
            
            //Calculate costs for different sizes of lambda
            double[] costFnTrain = new double[lambdas.length];
            double[] costFnCrossVal = new double[lambdas.length];
            double[] totalCost = new double[lambdas.length];
            double finalLambda = 0.0;
            
            switch (MODEL) {
                case LINEAR_REG:

                    try {
                        double[] linearRegThetas = getThetaForModel(matrixValues, MODEL, ticker.getTicker(), DAYS_IN_FUTURE, finalLambda);

                        //Save cost values for all 3 datasets (Training, Cross Val, Test)
                        double trainingCost = LinearRegFormulas.costFunction(matrixValues.getFeatures(TRAINING), linearRegThetas, matrixValues.getOutputValues(TRAINING), finalLambda);
                        double crossValCost = LinearRegFormulas.costFunction(matrixValues.getFeatures(CROSS_VAL), linearRegThetas, matrixValues.getOutputValues(CROSS_VAL), finalLambda);
                        double testCost = LinearRegFormulas.costFunction(matrixValues.getFeatures(TEST), linearRegThetas, matrixValues.getOutputValues(TEST), finalLambda);

                        //Save values to DB
                        sdh.setModelValues(ticker.getTicker(), "LINEAR-REG", linearRegThetas, averages, ranges, finalLambda, trainingCost, crossValCost, testCost);
                    } catch (Exception exc) {
                        System.out.println(exc);
                    }

                    break;
            }
        }
    }

    //Find which value of lambda produced the overall lowest cost amongst the traning and cross validation test set
    private double getLowestCostOption(double[] totalCost, double[] lambdas) {
        
        int smallestCostIndex = 0;

        for (int i = 1; i < lambdas.length; i++) {
            if (totalCost[i] < totalCost[smallestCostIndex]) {
                smallestCostIndex = i;
            }
        }

        return smallestCostIndex;
    }
    
    private double[] getThetaForModel(final MatrixValues MATRIX_VAL, final ModelTypes MOD_APPR, final String TICKER, final int DAYS_IN_FUTURE, double lambda) throws Exception {
        //Get values from the MatrixValues object
        final RecordType REC_TYPE = RecordType.TRAINING;
        double[][] trainingMatrix = MATRIX_VAL.getFeatures(REC_TYPE);
        double[] results = MATRIX_VAL.getOutputValues(REC_TYPE);

        //Calculate Theta
        double[] thetas = initializeTheta(trainingMatrix[0].length);
        thetas = runGradientDescent(MOD_APPR, trainingMatrix, results, thetas, lambda);
        return thetas;
    }

    private double[] runGradientDescent(ModelTypes approach, double[][] trainingMatrix, double[] results, double[] theta, double lambda) throws Exception {
        //Run Gradient Descent until there is less than a 0.001 variance
        final double MAX_VARIANCE = 0.001;
        double oldCostFunction = Double.MAX_VALUE;
        double costFunction = 0.0;

        int i;
        for (i = 0; ; i++) {
            switch(approach) {
                case LINEAR_REG:
                    LinearRegFormulas.gradientDescent(trainingMatrix, theta, results, lambda);
                    costFunction = LinearRegFormulas.costFunction(trainingMatrix, theta, results, lambda);
                    break;
                case LOGISTIC_REG:
                    LogisticRegFormulas.gradientDescent(trainingMatrix, theta, results, lambda);
                    costFunction = LogisticRegFormulas.costFunction(trainingMatrix, theta, results, lambda);
                    break;
            } //End switch
            
            //Test Check
            if (i > 5000) {
                throw new Exception("Problem with Gradient Descent================================================");
            }
            
            //Learning Rate ALPHA Check
            if (oldCostFunction < costFunction)
                throw new Exception("Learning Rate ALPHA is too high!");
            
            //See if the variance has been met
            if (oldCostFunction - costFunction < MAX_VARIANCE) 
                break;
            
            oldCostFunction = costFunction;
        }
        System.out.println("Cost Function = " + costFunction + ", Iterations = " + i);

        return theta;
    }
}
        
