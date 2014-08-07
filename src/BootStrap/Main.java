/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package BootStrap;

import Modeling.ModelTypes;
import Modeling.Predictor;
import Modeling.RunModels;
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

        final int DAYS_IN_FUTURE = 1;
        final ModelTypes LIN_REG = ModelTypes.LINEAR_REG;
        final ModelTypes LOG_REG = ModelTypes.LOGIST_REG;

        StockDataHandler sdh = new StockDataHandler();
        sdh.downloadAllStockData();

        final int DAYS_BACK = 0;
        sdh.computeMovingAverages(DAYS_BACK);
        sdh.computeStockQuoteSlopes(DAYS_BACK);

        //Run Linear Regression
        RunModels models = new RunModels();
        models.runModels(LIN_REG, DAYS_IN_FUTURE);

        //Run Logistic Regression
        models.runModels(LOG_REG, DAYS_IN_FUTURE);

        //Generate Predictions
        Calendar toDate = Calendar.getInstance();
        Calendar fromDate = Calendar.getInstance();
        fromDate.set(2000, 0, 1);

        Predictor pred = new Predictor();
        pred.predictAllStocksForDates(LIN_REG, DAYS_IN_FUTURE, fromDate.getTime(), toDate.getTime());
        pred.predictAllStocksForDates(LOG_REG, DAYS_IN_FUTURE, fromDate.getTime(), toDate.getTime());
        
        //Backtesting
        pred.backtest(LIN_REG, fromDate.getTime(), toDate.getTime());
        pred.backtest(LOG_REG, fromDate.getTime(), toDate.getTime());
    }
}
