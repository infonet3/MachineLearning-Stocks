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
import java.util.List;
import java.util.Properties;
import java.util.Random;
import weka.classifiers.Evaluation;
import weka.classifiers.trees.M5P;
import weka.classifiers.trees.RandomForest;
import weka.core.Instances;
import weka.core.SerializationHelper;

/**
 *
 * @author Matt Jones
 */
public class RunModels {

    static Logger logger = new Logger();
    final String CONF_FILE = "Resources\\settings.conf";
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

        String summary = String.format("Model Type: %s, Days In Future: %d", MODEL, DAYS_IN_FUTURE);
        logger.Log("RunModels", "runModels", summary, "");

        testAllStocks(MODEL, DAYS_IN_FUTURE);
    }
    
    //Run through all stock and determine optimal values of theta for prediction
    private void testAllStocks(final ModelTypes MODEL, final int DAYS_IN_FUTURE) throws Exception {
        
        //Run through all stock tickers
        StockDataHandler sdh = new StockDataHandler();
        List<StockTicker> stockList = sdh.getAllStockTickers();

        for (int i = 0; i < stockList.size(); i++) {
            StockTicker ticker = stockList.get(i);
            
            System.gc();
            
            try {
                //Pull data for this stock from the DB
                String dataExamples = sdh.getAllStockFeaturesFromDB(ticker.getTicker(), DAYS_IN_FUTURE, MODEL, null, null);

                //Now Build the Models
                double accuracy = 0.0;
                StringReader sr;
                Instances train;
                Evaluation eval;
                String newFileName;
                Path p;
                switch (MODEL) {
                    case RAND_FORST:
                        
                        sr = new StringReader(dataExamples);
                        train = new Instances(sr);
                        sr.close();

                        train.setClassIndex(train.numAttributes() - 1); //Last item is the class label
        
                        RandomForest rf = new RandomForest();
                        rf.buildClassifier(train);

                        eval = new Evaluation(train);
                        eval.crossValidateModel(rf, train, 10, new Random(1));
                        
                        accuracy = eval.correct() / (eval.correct() + eval.incorrect());
                        logger.Log("RunModels", "testAllStocks", "Model Stats", eval.toSummaryString("\nResults\n========\n", true));

                        newFileName = MODEL_PATH + "\\" + ticker.getTicker() + "-RandomForest.model";
                        p = Paths.get(newFileName);
                        if (Files.exists(p))
                            Files.delete(p);
                        
                        SerializationHelper.write(newFileName, rf);
                        
                        break;
                    
                    case M5P:

                        sr = new StringReader(dataExamples);
                        train = new Instances(sr);
                        sr.close();

                        train.setClassIndex(train.numAttributes() - 1); //Last item is the class label
        
                        M5P mp = new M5P();
                        mp.buildClassifier(train);

                        eval = new Evaluation(train);
                        eval.crossValidateModel(mp, train, 10, new Random(1));
                        
                        accuracy = eval.correlationCoefficient();
                        logger.Log("RunModels", "testAllStocks", "Model Stats", eval.toSummaryString("\nResults\n========\n", true));

                        newFileName = MODEL_PATH + "\\" + ticker.getTicker() + "-M5P.model";
                        p = Paths.get(newFileName);
                        if (Files.exists(p))
                            Files.delete(p);

                        SerializationHelper.write(newFileName, mp);
                        
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
                
                System.gc();

            } catch(Exception exc) {
                logger.Log("RunModels", "testAllStocks", "Exception", ticker.getTicker() + ": " + exc.toString());
                throw exc;
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
        
