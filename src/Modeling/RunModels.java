/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Modeling;

import static Modeling.ModelTypes.LINEAR_REG;
import static Modeling.ModelTypes.LOGIST_REG;
import StockData.StockDataHandler;
import StockData.StockTicker;
import java.io.FileInputStream;
import java.io.StringReader;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import weka.classifiers.Evaluation;
import weka.classifiers.trees.RandomForest;
import weka.core.Instances;
import weka.core.SerializationHelper;

/**
 *
 * @author Matt Jones
 */
public class RunModels {

    final String CONF_FILE = "settings.conf";
    final String MODEL_PATH;
    
    public RunModels() throws Exception {

        //Load the file settings
        Properties p = new Properties();
        try (FileInputStream fis = new FileInputStream(CONF_FILE)) {
            p.load(fis);
            MODEL_PATH = p.getProperty("model_directory");
        }

    }
    
    //Methods
    public void runModels(final ModelTypes MODEL, final int DAYS_IN_FUTURE) throws Exception {

        testAllStocks(MODEL, DAYS_IN_FUTURE);
    }
    
    //Run through all stock and determine optimal values of theta for prediction
    private void testAllStocks(final ModelTypes MODEL, final int DAYS_IN_FUTURE) throws Exception {
        
        //Run through all stock tickers
        StockDataHandler sdh = new StockDataHandler();
        List<StockTicker> stockList = sdh.getAllStockTickers();

        for (int i = 0; i < stockList.size(); i++) {
            StockTicker ticker = stockList.get(i);
            long startTime = System.currentTimeMillis();
            
            try {
                //Pull data for this stock from the DB
                String dataExamples = sdh.getAllStockFeaturesFromDB(ticker.getTicker(), DAYS_IN_FUTURE, MODEL, null, null);

                //Now Build the Models
                double accuracy = 0.0;
                switch (MODEL) {
                    case RAND_FORST:

                        System.gc();
                        
                        StringReader sr = new StringReader(dataExamples);
                        Instances train = new Instances(sr);
                        sr.close();

                        train.setClassIndex(train.numAttributes() - 1); //Last item is the class label
        
                        RandomForest rf = new RandomForest();
                        rf.buildClassifier(train);

                        Evaluation eval = new Evaluation(train);
                        eval.crossValidateModel(rf, train, 10, new Random(1));
                        
                        accuracy = eval.correct() / (eval.correct() + eval.incorrect());
                        System.out.println(eval.toSummaryString("\nResults\n========\n", true));

                        String newFileName = MODEL_PATH + "\\" + ticker.getTicker() + ".model";
                        SerializationHelper.write(newFileName, rf);
                        
                        break;
                    
                    case SVM:
                        break;

                    case LINEAR_REG:
                        break;
                    case LOGIST_REG:
                        break;
                }

                //Save values to DB
                sdh.setModelValues(ticker.getTicker(), MODEL.toString(), DAYS_IN_FUTURE, accuracy);

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
    
}
        
