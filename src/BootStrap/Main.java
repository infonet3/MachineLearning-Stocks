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
import Utilities.Dates;
import Utilities.Logger;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

/**
 *
 * @author Matt Jones
 */
public class Main {
    
    static Logger logger = new Logger();
    
    public static void main(String[] args) throws Exception {

        try {
            if (args.length == 0) {
                logger.Log("Main", "main", "Program Arguments", "Min Arguments not entered, must be at least one.", true);
                System.out.println("Missing argument!");
                System.exit(1);
            }

            long startTime = System.currentTimeMillis();
            
            final int DAYS_IN_FUTURE = 10; //Business Days
            Dates dates = new Dates();
            Date yesterday = dates.getYesterday();
            Predictor pred = new Predictor();

            //Loop through the required actions
            String firstArg = args[0];
            StockDataHandler sdh = new StockDataHandler();
            Map<Date, String> mapHolidays = sdh.getAllHolidays();
            for (int i = 0; i < firstArg.length(); i++) {
                
                char curAction = firstArg.charAt(i);
                switch(curAction) {
                    //Download Data
                    case 'D':
                        logger.Log("Main", "main", "Option D", "Downloading Data", false);

                        sdh.downloadAllStockData();

                        final int DAYS_BACK = 0;
                        sdh.computeMovingAverages(DAYS_BACK);
                        sdh.computeStockQuoteSlopes(DAYS_BACK);

                        break;

                    //Generate Models    
                    case 'M':
                        logger.Log("Main", "main", "Option M", "Generating Models", false);

                        RunModels models = new RunModels();
                        models.runModels(ModelTypes.RAND_FORST, DAYS_IN_FUTURE);
                        models.runModels(ModelTypes.M5P, DAYS_IN_FUTURE);

                        break;

                    //Current Predictions
                    case 'C':
                        logger.Log("Main", "main", "Option C", "Get Current Predictions", false);

                        final String PRED_TYPE_CURRENT = "CURRENT";
                        pred.predictAllStocksForDates(ModelTypes.RAND_FORST, 0, DAYS_IN_FUTURE, yesterday, yesterday, PRED_TYPE_CURRENT);
                        pred.predictAllStocksForDates(ModelTypes.M5P, 0, DAYS_IN_FUTURE, yesterday, yesterday, PRED_TYPE_CURRENT);

                        break;

                    //Backtest
                    case 'B':
                        logger.Log("Main", "main", "Option B", "Perform Backtesting", false);

                        Calendar fromDate = Calendar.getInstance();
                        fromDate.set(2014, 5, 1);

                        Calendar toDate = Calendar.getInstance();

                        sdh.removeBacktestingData();
                        final String PRED_TYPE_BACKTEST = "BACKTEST";
                        pred.predictAllStocksForDates(ModelTypes.RAND_FORST, DAYS_IN_FUTURE, DAYS_IN_FUTURE, fromDate.getTime(), toDate.getTime(), PRED_TYPE_BACKTEST);
                        pred.predictAllStocksForDates(ModelTypes.M5P, DAYS_IN_FUTURE, DAYS_IN_FUTURE, fromDate.getTime(), toDate.getTime(), PRED_TYPE_BACKTEST);

                        //pred.backtest(ModelTypes.RAND_FORST, fromDate.getTime(), toDate.getTime());
                        //pred.backtest(ModelTypes.M5P, fromDate.getTime(), toDate.getTime());
                        pred.topNBacktest(5, fromDate.getTime(), toDate.getTime());

                        break;

                    //Trading
                    case 'T':
                        logger.Log("Main", "main", "Option T", "Perform Automated Trading", false);

                        //Cmd Line Arguments must equal 2 - T IB_GateWay_Port
                        if (args.length != 2) {
                            logger.Log("Main", "main", "Program Arguments", "For T Option - Must have two Arguments.", true);
                            System.out.println("Missing argument for T Option!");
                            System.exit(1);
                        }

                        //2nd Argument - IB Gateway Port
                        int port = Integer.parseInt(args[1]);

                        //Holiday Check
                        String holidayCode = mapHolidays.get(yesterday);
                        if (holidayCode == null)
                            holidayCode = "";

                        switch (holidayCode) {
                            case "Closed":
                                break;

                            case "Early": //Can only buy on such a day and not sell
                            default:
                                TradeEngine trade = new TradeEngine();
                                final int MAX_STOCK_COUNT = 5;
                                trade.emailTodaysStockPicks(MAX_STOCK_COUNT, yesterday);

                                trade.runTrading(MAX_STOCK_COUNT, port);
                                break;
                        }

                        break;
                        
                    default:
                        logger.Log("Main", "main", "Invalid Action Code", firstArg, true);
                        System.exit(12);
                
                        break;

                } //End Switch
                
            } //End action for loop

            //Output Job Timing
            long endTime = System.currentTimeMillis();
            int elapsedTimeMin = (int)((endTime - startTime) / (1000 * 60));
            String outputMsg = String.format("Minutes Elapsed = %d", elapsedTimeMin);
            logger.Log("Main", "main", "Run Options = " + firstArg, outputMsg, true);
            
        } catch (Exception exc) {
            logger.Log("Main", "main", "Exception", exc.toString(), true);
            throw exc;
        }
        
    }
}
