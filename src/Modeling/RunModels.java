/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Modeling;

import static Modeling.ModelTypes.LINEAR_REG;
import static Modeling.ModelTypes.LOGIST_REG;
import StockData.StockDataHandler;
import StockData.StockTicker;
import Utilities.Logger;
import java.io.FileInputStream;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import weka.classifiers.Evaluation;
import weka.classifiers.functions.LinearRegression;
import weka.classifiers.trees.M5P;
import weka.classifiers.trees.RandomForest;
import weka.core.Instances;
import weka.core.SerializationHelper;

/**
 *
 * @author Matt Jones
 */
public class RunModels implements Runnable {

    static Logger logger = new Logger();
    final String CONF_FILE = "Resources/settings.conf";
    final String MODEL_PATH;
    final ModelTypes MODEL;
    final int DAYS_IN_FUTURE; 
    final int YEARS_BACK;
    final Date MAX_DATE;
    
    public RunModels(final ModelTypes MODEL, final int DAYS_IN_FUTURE, final int YEARS_BACK, final Date MAX_DATE) throws Exception {

        //Load the file settings
        Properties p = new Properties();
        try (FileInputStream fis = new FileInputStream(CONF_FILE)) {
            p.load(fis);
            MODEL_PATH = p.getProperty("model_directory");
        }
        
        this.MODEL = MODEL;
        this.DAYS_IN_FUTURE = DAYS_IN_FUTURE;
        this.YEARS_BACK = YEARS_BACK;
        this.MAX_DATE = MAX_DATE;
    }
    
    //Methods
    public void run() {
        try {
            runModels();
        } catch(Exception exc) {
            
            try {
                logger.Log("RunModels", "run", "Exception", exc.toString(), true);
            } catch (Exception exc2) {
                System.out.println(exc2.toString());
            }

            //Stop the program
            System.exit(20);
        }
    }
        
    private void runModels() throws Exception {

        String summary = String.format("Model Type: %s, Days In Future: %d, Years Back: %d", MODEL, DAYS_IN_FUTURE, YEARS_BACK);
        logger.Log("RunModels", "runModels", summary, "", false);
        
        testAllStocks();
    }
    
    //Run through all stock and determine optimal values of theta for prediction
    private void testAllStocks() throws Exception {
        
        StockDataHandler sdh = new StockDataHandler();
        List<StockTicker> stockList = sdh.getAllStockTickers();

        //Calculate start and end dates
        Date toDt = Calendar.getInstance().getTime();
        if (MAX_DATE != null)
            toDt = MAX_DATE;

        Calendar fromCal = Calendar.getInstance();
        fromCal.setTime(toDt);
        fromCal.add(Calendar.YEAR, -YEARS_BACK);
        Date fromDt = fromCal.getTime();
        
        //Run through all stock tickers
        final int NUM_TREES = 100;

        for (int i = 0; i < stockList.size(); i++) {
            StockTicker ticker = stockList.get(i);
            
            System.gc();
            
            //Pull records from DB
            String dataExamples = sdh.getAllStockFeaturesFromDB(ticker.getTicker(), DAYS_IN_FUTURE, MODEL, fromDt, toDt, true, PredictionType.BACKTEST);

            //Pull the training data
            Instances train;
            try (StringReader sr = new StringReader(dataExamples)) {
                train = new Instances(sr);
            }
            train.setClassIndex(train.numAttributes() - 1); //Last item is the class label

            //Now Build the Models
            double accuracy = 0.0;
            Evaluation eval;
            String newFileName;
            Path p;
            switch (MODEL) {
                case RAND_FORST:

                    RandomForest rf = new RandomForest();
                    rf.setNumTrees(NUM_TREES);
                    rf.buildClassifier(train);

                    eval = new Evaluation(train);
                    eval.crossValidateModel(rf, train, 10, new Random(1));

                    accuracy = eval.correct() / (eval.correct() + eval.incorrect());
                    logger.Log("RunModels", "testAllStocks", "Model Stats", eval.toSummaryString("\nResults\n========\n", true), false);

                    newFileName = MODEL_PATH + "/" + ticker.getTicker() + "-RandomForest.model";
                    p = Paths.get(newFileName);
                    if (Files.exists(p))
                        Files.delete(p);

                    SerializationHelper.write(newFileName, rf);

                    break;

                case M5P:

                    M5P mp = new M5P();
                    mp.buildClassifier(train);

                    eval = new Evaluation(train);
                    eval.crossValidateModel(mp, train, 10, new Random(1));

                    accuracy = eval.correlationCoefficient();
                    logger.Log("RunModels", "testAllStocks", "Model Stats", eval.toSummaryString("\nResults\n========\n", true), false);

                    newFileName = MODEL_PATH + "/" + ticker.getTicker() + "-M5P.model";
                    p = Paths.get(newFileName);
                    if (Files.exists(p))
                        Files.delete(p);

                    SerializationHelper.write(newFileName, mp);

                    break;
                    
                case LINEAR_REG:

                    LinearRegression linReg = new LinearRegression();
                    linReg.buildClassifier(train);

                    eval = new Evaluation(train);
                    eval.crossValidateModel(linReg, train, 10, new Random(1));

                    accuracy = eval.correlationCoefficient();
                    logger.Log("RunModels", "testAllStocks", "Model Stats", eval.toSummaryString("\nResults\n========\n", true), false);

                    newFileName = MODEL_PATH + "/" + ticker.getTicker() + "-LinearRegression.model";
                    p = Paths.get(newFileName);
                    if (Files.exists(p))
                        Files.delete(p);

                    SerializationHelper.write(newFileName, linReg);

                    break;
            }

            //Save values to DB
            sdh.setModelValues(ticker.getTicker(), MODEL.toString(), DAYS_IN_FUTURE, accuracy);

            System.gc();


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
    
}
        
