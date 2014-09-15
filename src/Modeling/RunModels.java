/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Modeling;

import ML_Formulas.CostResults;
import ML_Formulas.LogisticRegFormulas;
import ML_Formulas.RandomForrest;
import ML_Formulas.SVM;
import MatrixOps.Matrix;
import MatrixOps.MatrixValues;
import MatrixOps.RecordType;
import static Modeling.ModelTypes.LINEAR_REG;
import static Modeling.ModelTypes.LOGIST_REG;
import StockData.StockDataHandler;
import StockData.StockTicker;
import java.util.List;
import libsvm.svm_model;
import org.apache.mahout.classifier.df.DecisionForest;
import org.apache.mahout.classifier.df.builder.DefaultTreeBuilder;
import org.apache.mahout.classifier.df.data.*;
import org.apache.mahout.classifier.df.ref.SequentialBuilder;
import org.apache.mahout.common.RandomUtils;

/**
 *
 * @author Matt Jones
 */
public class RunModels {

    //Methods
    public void runModels(final ModelTypes MODEL, final int DAYS_IN_FUTURE) throws Exception {

        testAllStocks(MODEL, DAYS_IN_FUTURE);
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
        List<StockTicker> stockList = sdh.getAllStockTickers(true); //FIX THIS LATER, SET TO FALSE!!!!
        MatrixValues matrixValues;
        for (int i = 0; i < stockList.size(); i++) {
            StockTicker ticker = stockList.get(i);
            long startTime = System.currentTimeMillis();
            
            try {
                //Pull data for this stock from the DB and save to class field
                matrixValues = Matrix.loadMatrixFromDB(ticker.getTicker(), DAYS_IN_FUTURE, MODEL, null, null);

                //Calculate costs for different sizes of lambda
                CostResults trainingCost = null;
                CostResults crossValCost = null;
                CostResults testCost = null;
                double finalLambda = 0.0;
                double[] thetaValues = null;

                switch (MODEL) {
                    case RAND_FORST:
                        RandomForrest rf = new RandomForrest();
                        int numTrees = 200;
                        DecisionForest df = rf.createRandomForrest(matrixValues, numTrees);
                        rf.saveForestToFile(df, ticker.getTicker());

                        trainingCost = rf.testRandomForrest(df, matrixValues.getFeatures(TRAINING), matrixValues.getOutputValues(TRAINING), numTrees);
                        crossValCost = rf.testRandomForrest(df, matrixValues.getFeatures(CROSS_VAL), matrixValues.getOutputValues(CROSS_VAL), numTrees);
                        testCost = rf.testRandomForrest(df, matrixValues.getFeatures(TEST), matrixValues.getOutputValues(TEST), numTrees);
                        System.gc();
                        break;
                    
                    case SVM:
                        svm_model model = SVM.createModel(matrixValues);
                        break;

                    case LINEAR_REG:
                        //Save cost values for all 3 datasets (Training, Cross Val, Test)
                        thetaValues = getThetaForModel(matrixValues, MODEL, ticker.getTicker(), DAYS_IN_FUTURE, finalLambda);

                        //trainingCost = LinearRegFormulas.costFunction(matrixValues.getFeatures(TRAINING), thetaValues, matrixValues.getOutputValues(TRAINING), finalLambda);
                        //crossValCost = LinearRegFormulas.costFunction(matrixValues.getFeatures(CROSS_VAL), thetaValues, matrixValues.getOutputValues(CROSS_VAL), finalLambda);
                        //testCost = LinearRegFormulas.costFunction(matrixValues.getFeatures(TEST), thetaValues, matrixValues.getOutputValues(TEST), finalLambda);

                        break;
                    case LOGIST_REG:
                        //Save cost values for all 3 datasets (Training, Cross Val, Test)
                        thetaValues = getThetaForModel(matrixValues, MODEL, ticker.getTicker(), DAYS_IN_FUTURE, finalLambda);

                        trainingCost = LogisticRegFormulas.costFunction(matrixValues.getFeatures(TRAINING), thetaValues, matrixValues.getOutputValues(TRAINING), finalLambda);
                        crossValCost = LogisticRegFormulas.costFunction(matrixValues.getFeatures(CROSS_VAL), thetaValues, matrixValues.getOutputValues(CROSS_VAL), finalLambda);
                        testCost = LogisticRegFormulas.costFunction(matrixValues.getFeatures(TEST), thetaValues, matrixValues.getOutputValues(TEST), finalLambda);

                        break;
                }

                //Save values to DB
                double[] averages = matrixValues.getOriginalFeatureAverages();
                double[] ranges = matrixValues.getOriginalFeatureRanges();
                sdh.setModelValues(ticker.getTicker(), MODEL.toString(), DAYS_IN_FUTURE, thetaValues, averages, ranges, finalLambda, trainingCost, crossValCost, testCost);

                long endTime = System.currentTimeMillis();
                long procTime = endTime - startTime;
                System.out.println("Processing Time = " + (procTime / 1000.0) + " sec");
                
                System.gc();

            } catch(Exception exc) {
                System.out.println("Method: testAllStocks, Ticker: " + ticker.getTicker() + ", Desc: " + exc);
            }

        } //End of for loop
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
                maxVariance = 0.000007;
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
        final int MAX_ITERATIONS = 12000;
        CostResults costResults = null;
        for (i = 0; ; i++) {
            switch(approach) {
                case LINEAR_REG:
                    //LinearRegFormulas.gradientDescent(trainingMatrix, theta, results, lambda);
                    //costFunction = LinearRegFormulas.costFunction(trainingMatrix, theta, results, lambda);
                    break;
                case LOGIST_REG:
                    LogisticRegFormulas.gradientDescent(trainingMatrix, theta, results, lambda);
                    costResults = LogisticRegFormulas.costFunction(trainingMatrix, theta, results, lambda);
                    costFunction = costResults.getCost();
                    break;
            } //End switch
            
            //Test Check
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

        System.out.printf("Cost Function = %.4f Iterations = %d %n", costFunction, i);
        //System.out.println("Cost Function = " + costFunction + ", Iterations = " + i);

        return theta;
    }
}
        
