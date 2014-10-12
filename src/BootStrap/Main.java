/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package BootStrap;

import Modeling.ModelTypes;
import Modeling.Predictor;
import Modeling.RunModels;
import StockData.StockDataHandler;
import Trading.TradeEngine;
import java.util.Calendar;

/**
 *
 * @author Matt Jones
 */
public class Main {
    public static void main(String... args) throws Exception {

        final int DAYS_IN_FUTURE = 10; //Business Days
        final ModelTypes RND_FOR = ModelTypes.RAND_FORST;
        final ModelTypes M5P = ModelTypes.M5P;

        TradeEngine trade = new TradeEngine();
        //trade.connect();
        
        StockDataHandler sdh = new StockDataHandler();
        //sdh.downloadAllStockData();

        //COMPLETE THIS
        //String output = sdh.downloadCensusData();

        final int DAYS_BACK = 0;
        //sdh.computeMovingAverages(DAYS_BACK);
        //sdh.computeStockQuoteSlopes(DAYS_BACK);

        RunModels models = new RunModels();
        //models.runModels(RND_FOR, DAYS_IN_FUTURE);
        //models.runModels(M5P, DAYS_IN_FUTURE);

        //Generate Predictions
        Calendar toDate = Calendar.getInstance();
        Calendar fromDate = Calendar.getInstance();
        //fromDate.set(2006, 0, 4);
        fromDate.set(2010, 0, 4);

        //Historical Prediction
        final String PRED_TYPE_BACKTEST = "BACKTEST";
        Predictor pred = new Predictor();
        //pred.predictAllStocksForDates(RND_FOR, DAYS_IN_FUTURE, fromDate.getTime(), toDate.getTime(), PRED_TYPE_BACKTEST);
        //pred.predictAllStocksForDates(M5P, DAYS_IN_FUTURE, fromDate.getTime(), toDate.getTime(), PRED_TYPE_BACKTEST);

        //Get Current Prediction for Today
        final String PRED_TYPE_CURRENT = "CURRENT";
        Calendar today = Calendar.getInstance();
        //pred.predictAllStocksForDates(RND_FOR, 0, today.getTime(), today.getTime(), PRED_TYPE_CURRENT);
        //pred.predictAllStocksForDates(M5P, 0, today.getTime(), today.getTime(), PRED_TYPE_CURRENT);
        
        //Backtesting
        //pred.backtest(RND_FOR, fromDate.getTime(), toDate.getTime());
        //pred.backtest(M5P, fromDate.getTime(), toDate.getTime());
        pred.topNBacktest(5, fromDate.getTime(), toDate.getTime());
    }
}
