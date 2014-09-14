/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package BootStrap;

import Modeling.ModelTypes;
import Modeling.Predictor;
import Modeling.RunModels;
import StockData.BEA_Data;
import StockData.PredictionValues;
import StockData.StockDataHandler;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 *
 * @author Matt Jones
 */
public class Main {
    public static void main(String... args) throws Exception {

        final int DAYS_IN_FUTURE = 5;
        final ModelTypes LIN_REG = ModelTypes.LINEAR_REG;
        final ModelTypes LOG_REG = ModelTypes.LOGIST_REG;
        final ModelTypes SVM = ModelTypes.SVM;
        final ModelTypes RND_FOR = ModelTypes.RANDOM_FORREST;

        StockDataHandler sdh = new StockDataHandler();
        //sdh.downloadAllStockData();

        //COMPLETE THIS
        //String output = sdh.downloadCensusData();
        

        final int DAYS_BACK = 0;
        //sdh.computeMovingAverages(DAYS_BACK);
        //sdh.computeStockQuoteSlopes(DAYS_BACK);

        //Run Linear Regression
        RunModels models = new RunModels();
        //models.runModels(LIN_REG, DAYS_IN_FUTURE);

        //Run Random Forrest
        models.runModels(RND_FOR, DAYS_IN_FUTURE);
        
        //Run Logistic Regression
        models.runModels(LOG_REG, DAYS_IN_FUTURE);
        //models.runModels(SVM, DAYS_IN_FUTURE);

        //Generate Predictions
        Calendar toDate = Calendar.getInstance();
        Calendar fromDate = Calendar.getInstance();
        fromDate.set(2004, 7, 1);

        Predictor pred = new Predictor();
        //pred.predictAllStocksForDates(LIN_REG, DAYS_IN_FUTURE, fromDate.getTime(), toDate.getTime());
        pred.predictAllStocksForDates(LOG_REG, DAYS_IN_FUTURE, fromDate.getTime(), toDate.getTime());
        
        //Backtesting
        //pred.backtest(LIN_REG, fromDate.getTime(), toDate.getTime());
        pred.backtest(LOG_REG, fromDate.getTime(), toDate.getTime());
    }
}
