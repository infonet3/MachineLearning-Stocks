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
public class RunModels implements Runnable {

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
    
    public void run() {
        
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
        List<StockTicker> stockList = sdh.getAllStockTickers(true); //FIX THIS LATER, SET TO FALSE!!!!
        MatrixValues matrixValues;
        boolean isSkip = true;
        for (int i = 0; i < stockList.size(); i++) {
            StockTicker ticker = stockList.get(i);

            if (isSkip) {
                if (ticker.getTicker().equals("APAM")) //Not running for some reason???
                    isSkip = false;

                continue;
            }
            
            //Pull data for this stock from the DB and save to class field
            matrixValues = Matrix.loadMatrixFromDB(ticker.getTicker(), DAYS_IN_FUTURE, MODEL, null, null);
            double[] averages = matrixValues.getOriginalFeatureAverages();
            double[] ranges = matrixValues.getOriginalFeatureRanges();
            
            //Calculate costs for different sizes of lambda
            double trainingCost = 0.0;
            double crossValCost = 0.0;
            double testCost = 0.0;
            double finalLambda = 0.0;

            double[] thetaValues = getThetaForModel(matrixValues, MODEL, ticker.getTicker(), DAYS_IN_FUTURE, finalLambda);

            switch (MODEL) {
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
            sdh.setModelValues(ticker.getTicker(), MODEL.toString(), DAYS_IN_FUTURE, thetaValues, averages, ranges, finalLambda, trainingCost, crossValCost, testCost);
            
            System.gc();
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
    
    double[] getThetaForModel(final MatrixValues MATRIX_VAL, final ModelTypes MOD_APPR, final String TICKER, final int DAYS_IN_FUTURE, double lambda) throws Exception {
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
        final double maxVariance;
        switch (approach) {
            case LOGIST_REG:
                maxVariance = 0.00001;
                break;
            
            case LINEAR_REG:
                maxVariance = 0.001;
                break;
                
            default:
                throw new Exception("Method: runGradientDescent, Desc: Invalid Model Type!");
        }

        double oldCostFunction = Double.MAX_VALUE;
        double costFunction = 0.0;

        int i;
        for (i = 0; ; i++) {
            switch(approach) {
                case LINEAR_REG:
                    LinearRegFormulas.gradientDescent(trainingMatrix, theta, results, lambda);
                    costFunction = LinearRegFormulas.costFunction(trainingMatrix, theta, results, lambda);
                    break;
                case LOGIST_REG:
                    LogisticRegFormulas.gradientDescent(trainingMatrix, theta, results, lambda);
                    costFunction = LogisticRegFormulas.costFunction(trainingMatrix, theta, results, lambda);
                    break;
            } //End switch
       
            
            //Test Check
            final int MAX_ITERATIONS = 4000;
            if (i > MAX_ITERATIONS) {
                throw new Exception("Problem with Gradient Descent================================================");
            }

            //Learning Rate ALPHA Check
            if (oldCostFunction < costFunction)
                throw new Exception("Learning Rate ALPHA is too high!");

            //See if the variance has been met
            if (oldCostFunction - costFunction < maxVariance) 
                break;

            oldCostFunction = costFunction;
        }

        System.out.println("Cost Function = " + costFunction + ", Iterations = " + i);

        return theta;
    }
}
        
