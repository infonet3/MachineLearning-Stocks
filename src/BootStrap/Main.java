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
import java.util.Date;

/**
 *
 * @author Matt Jones
 */
public class Main {
    
    static Logger logger = new Logger();
    
    public static void main(String[] args) throws Exception {

        try {
            if (args.length != 1) {
                logger.Log("Main", "main", "Program Arguments", "Num arguments != 1", false);
                System.out.println("Missing argument!");
                System.exit(1);
            }

            final int DAYS_IN_FUTURE = 10; //Business Days
            Date yesterday = Utilities.Dates.getYesterday();
            Predictor pred = new Predictor();

            //Actions
            String argument = args[0];
            switch(argument) {
                //Download Data
                case "D":
                    logger.Log("Main", "main", "Option D", "Downloading Data", false);

                    StockDataHandler sdh = new StockDataHandler();
                    sdh.downloadAllStockData();

                    final int DAYS_BACK = 0;
                    sdh.computeMovingAverages(DAYS_BACK);
                    sdh.computeStockQuoteSlopes(DAYS_BACK);

                    break;

                //Generate Models    
                case "M":
                    logger.Log("Main", "main", "Option M", "Generating Models", false);

                    RunModels models = new RunModels();
                    models.runModels(ModelTypes.RAND_FORST, DAYS_IN_FUTURE);
                    models.runModels(ModelTypes.M5P, DAYS_IN_FUTURE);

                    break;

                //Current Predictions
                case "C":
                    logger.Log("Main", "main", "Option C", "Get Current Predictions", false);

                    final String PRED_TYPE_CURRENT = "CURRENT";
                    pred.predictAllStocksForDates(ModelTypes.RAND_FORST, 0, DAYS_IN_FUTURE, yesterday, yesterday, PRED_TYPE_CURRENT);
                    pred.predictAllStocksForDates(ModelTypes.M5P, 0, DAYS_IN_FUTURE, yesterday, yesterday, PRED_TYPE_CURRENT);

                    break;

                //Backtest
                case "B":
                    logger.Log("Main", "main", "Option B", "Perform Backtesting", false);

                    final String PRED_TYPE_BACKTEST = "BACKTEST";
                    
                    Calendar fromDate = Calendar.getInstance();
                    fromDate.set(2006, 0, 4);

                    Calendar toDate = Calendar.getInstance();

                    pred.predictAllStocksForDates(ModelTypes.RAND_FORST, DAYS_IN_FUTURE, DAYS_IN_FUTURE, fromDate.getTime(), toDate.getTime(), PRED_TYPE_BACKTEST);
                    pred.predictAllStocksForDates(ModelTypes.M5P, DAYS_IN_FUTURE, DAYS_IN_FUTURE, fromDate.getTime(), toDate.getTime(), PRED_TYPE_BACKTEST);

                    pred.backtest(ModelTypes.RAND_FORST, fromDate.getTime(), toDate.getTime());
                    pred.backtest(ModelTypes.M5P, fromDate.getTime(), toDate.getTime());
                    pred.topNBacktest(5, fromDate.getTime(), toDate.getTime());

                    break;

                //Trading
                case "T":
                    logger.Log("Main", "main", "Option T", "Perform Automated Trading", false);

                    TradeEngine trade = new TradeEngine();
                    final int MAX_STOCK_COUNT = 5;
                    trade.emailTodaysStockPicks(MAX_STOCK_COUNT, yesterday);
                    trade.runTrading(MAX_STOCK_COUNT);
                    
                    
                    break;
            } //End Switch
            
        } catch (Exception exc) {
            logger.Log("Main", "main", "Exception", exc.toString(), true);
            Notifications.EmailActions.SendEmail("Exception in main method", exc.toString());
            throw exc;
        }
        
    }
}
