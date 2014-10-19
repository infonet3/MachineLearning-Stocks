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
import Utilities.Logger;
import java.util.Calendar;

/**
 *
 * @author Matt Jones
 */
public class Main {
    
    static Logger logger = new Logger();
    
    public static void main(String[] args) throws Exception {

        if (args.length != 1) {
            System.out.println("Missing argument!");

            System.exit(1);
        }
        
        final int DAYS_IN_FUTURE = 10; //Business Days
        Predictor pred = new Predictor();
        Calendar fromDate = Calendar.getInstance();
        fromDate.set(2006, 0, 4);
        Calendar toDate = Calendar.getInstance();

        //Actions
        String argument = args[0];
        switch(argument) {
            //Download Data
            case "D":
                logger.Log("Main", "main", "Option D", "Downloading Data");

                StockDataHandler sdh = new StockDataHandler();
                sdh.downloadAllStockData();

                final int DAYS_BACK = 0;
                sdh.computeMovingAverages(DAYS_BACK);
                sdh.computeStockQuoteSlopes(DAYS_BACK);

                break;
                
            //Generate Models    
            case "M":
                logger.Log("Main", "main", "Option M", "Generating Models");

                RunModels models = new RunModels();
                models.runModels(ModelTypes.RAND_FORST, DAYS_IN_FUTURE);
                models.runModels(ModelTypes.M5P, DAYS_IN_FUTURE);
                
                break;

            //Current Predictions
            case "C":
                logger.Log("Main", "main", "Option C", "Get Current Predictions");

                final String PRED_TYPE_CURRENT = "CURRENT";
                Calendar today = Calendar.getInstance();
                pred.predictAllStocksForDates(ModelTypes.RAND_FORST, 0, today.getTime(), today.getTime(), PRED_TYPE_CURRENT);
                pred.predictAllStocksForDates(ModelTypes.M5P, 0, today.getTime(), today.getTime(), PRED_TYPE_CURRENT);
                
                break;
                
            //Backtest
            case "B":
                logger.Log("Main", "main", "Option B", "Perform Backtesting");

                final String PRED_TYPE_BACKTEST = "BACKTEST";
                pred.predictAllStocksForDates(ModelTypes.RAND_FORST, DAYS_IN_FUTURE, fromDate.getTime(), toDate.getTime(), PRED_TYPE_BACKTEST);
                pred.predictAllStocksForDates(ModelTypes.M5P, DAYS_IN_FUTURE, fromDate.getTime(), toDate.getTime(), PRED_TYPE_BACKTEST);
                
                pred.backtest(ModelTypes.RAND_FORST, fromDate.getTime(), toDate.getTime());
                pred.backtest(ModelTypes.M5P, fromDate.getTime(), toDate.getTime());
                pred.topNBacktest(5, fromDate.getTime(), toDate.getTime());

                break;

            //Trading
            case "T":
                logger.Log("Main", "main", "Option T", "Perform Automated Trading");

                TradeEngine trade = new TradeEngine();
                trade.connect();
                
                break;
        }
        
    }
}
