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
            
            final int DAYS_IN_FUTURE = 15; //Business Days
            final int YEARS_BACK = 5; //How much data to train on
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

                        break;

                    //Generate Models    
                    case 'M':
                        logger.Log("Main", "main", "Option M", "Generating Models", false);

                        Thread tRandForest = new Thread(new RunModels(ModelTypes.RAND_FORST, DAYS_IN_FUTURE, YEARS_BACK, null));
                        Thread tM5P = new Thread(new RunModels(ModelTypes.M5P, DAYS_IN_FUTURE, YEARS_BACK, null));

                        tRandForest.start();
                        tM5P.start();
                        
                        tRandForest.join();
                        tM5P.join();
                        
                        break;

                    //Current Predictions
                    case 'C':
                        logger.Log("Main", "main", "Option C", "Get Current Predictions", false);

                        final String PRED_TYPE_CURRENT = "CURRENT";
                        Thread tRandForestCurPred = new Thread(new Predictor(ModelTypes.RAND_FORST, 0, DAYS_IN_FUTURE, yesterday, yesterday, PRED_TYPE_CURRENT));
                        Thread tM5PCurPred = new Thread(new Predictor(ModelTypes.M5P, 0, DAYS_IN_FUTURE, yesterday, yesterday, PRED_TYPE_CURRENT));

                        tRandForestCurPred.start();
                        tM5PCurPred.start();
                        
                        tRandForestCurPred.join();
                        tM5PCurPred.join();
                        
                        break;

                    //Backtest
                    case 'B':
                        logger.Log("Main", "main", "Option B", "Perform Backtesting", false);

                        //Cmd Line Arguments must equal 3 - B NUM_STOCKS DAYS_IN_FUTURE
                        if (args.length != 3) {
                            logger.Log("Main", "main", "Program Arguments", "For B Option - Must have three Arguments [D NUM_STOCKS DAYS_IN_FUTURE].", true);
                            System.out.println("Missing argument for B Option!");
                            System.exit(1);
                        }
                        
                        final int NUM_STOCKS = Integer.parseInt(args[1]);
                        final int BACKTEST_DAYS_IN_FUTURE = Integer.parseInt(args[2]);
                        
                        String strOutput = String.format("Num Stocks: %d, Days In Future: %d", NUM_STOCKS, BACKTEST_DAYS_IN_FUTURE);
                        logger.Log("Main", "main", "Option B", strOutput, true);
                        
                        Calendar fromCal = Calendar.getInstance();
                        
                        fromCal.set(2010, 0, 22);
                        Date fromDt = fromCal.getTime();

                        Calendar toCal = Calendar.getInstance();
                        Date toDt = toCal.getTime();

                        pred.topNBacktest(NUM_STOCKS, fromDt, toDt, YEARS_BACK, BACKTEST_DAYS_IN_FUTURE);

                        break;

                    //Trading
                    case 'T':
                        logger.Log("Main", "main", "Option T", "Perform Automated Trading", false);

                        //Cmd Line Arguments must equal 2 - T IB_GateWay_Port
                        if (args.length != 2) {
                            logger.Log("Main", "main", "Program Arguments", "For T Option - Must have two Arguments [T PORT].", true);
                            System.out.println("Missing argument for T Option!");
                            System.exit(1);
                        }

                        //2nd Argument - IB Gateway Port
                        int port = Integer.parseInt(args[1]);

                        //How many stocks to hold in our portfolio
                        final int MAX_STOCK_COUNT = 7;
                        
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
