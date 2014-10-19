/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Modeling;

import StockData.PredictionValues;
import static Modeling.ModelTypes.LINEAR_REG;
import static Modeling.ModelTypes.LOGIST_REG;
import static Modeling.ModelTypes.RAND_FORST;
import StockData.*;
import Utilities.Logger;
import java.io.FileInputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import weka.classifiers.trees.M5P;
import weka.classifiers.trees.RandomForest;
import weka.core.Instances;
import weka.core.SerializationHelper;

/**
 *
 * @author Matt Jones
 */
public class Predictor {

    static Logger logger = new Logger();
    final String CONF_FILE = "Resources\\settings.conf";
    final String MODEL_PATH;

    public Predictor() throws Exception {

        //Load the file settings
        Properties p = new Properties();
        try (FileInputStream fis = new FileInputStream(CONF_FILE)) {
            p.load(fis);
            MODEL_PATH = p.getProperty("model_directory");
        }
    }
    
    
    public void predictAllStocksForDates(final ModelTypes MODEL_TYPE, final int DAYS_IN_FUTURE, final Date fromDate, final Date toDate, final String PRED_TYPE) throws Exception {

        String summary = "ModelType: " + MODEL_TYPE + ", From: " + fromDate + ", To: " + toDate + ", Days In Future: " + DAYS_IN_FUTURE + ", Prediction Type: " + PRED_TYPE;
        logger.Log("Predictor", "predictAllStocksForDates", summary, "");
        
        //Weekend Test - From Date
        Calendar calFrom = Calendar.getInstance();
        calFrom.setTime(fromDate);

        int dayOfWeek = calFrom.get(Calendar.DAY_OF_WEEK);
        if (dayOfWeek == Calendar.SATURDAY)
            calFrom.add(Calendar.DATE, -1);
        else if (dayOfWeek == Calendar.SUNDAY)
            calFrom.add(Calendar.DATE, -2);
        
        //Weekend Test - To Date
        Calendar calTo = Calendar.getInstance();
        calTo.setTime(toDate);
        
        dayOfWeek = calTo.get(Calendar.DAY_OF_WEEK);
        if (dayOfWeek == Calendar.SATURDAY)
            calTo.add(Calendar.DATE, -1);
        else if (dayOfWeek == Calendar.SUNDAY)
            calTo.add(Calendar.DATE, -2);
        
        //Loop through all stocks for the given day
        StockDataHandler sdh = new StockDataHandler();
        List<StockTicker> stockList = sdh.getAllStockTickers(); 
        for (StockTicker ticker : stockList) {

            System.gc();
            
            //Get Features for the selected dates
            String dataExamples = sdh.getAllStockFeaturesFromDB(ticker.getTicker(), DAYS_IN_FUTURE, MODEL_TYPE, calFrom.getTime(), calTo.getTime());

            //Ensure were not missing data
            if (dataExamples.isEmpty()) {
                logger.Log("Predictor", "predictAllStocksForDates", ticker.getTicker(), "No data returned from DB");
                throw new Exception("Method: predictAllStocksForDates, no data returned!");
            }
            
            //Load the model
            String modelPath;
            StringReader sr;
            Instances test;
            List<PredictionValues> listPredictions;
            switch (MODEL_TYPE) {
                case LINEAR_REG:
                    break;
                case LOGIST_REG:
                    break;
                case M5P:
                    
                    modelPath = MODEL_PATH + "\\" + ticker.getTicker() + "-M5P.model";
                    M5P mp = (M5P)SerializationHelper.read(modelPath);

                    sr = new StringReader(dataExamples);
                    test = new Instances(sr);
                    sr.close();
                    
                    test.setClassIndex(test.numAttributes() - 1);
                    
                    // label instances
                    listPredictions = new ArrayList<>();
                    for (int i = 0; i < test.numInstances(); i++) {
                        double clsLabel = mp.classifyInstance(test.instance(i));

                        double[] array = test.instance(i).toDoubleArray();
                        int year = (int)array[0];
                        int month = (int)array[1];
                        int date = (int)(int)array[2];
                        
                        Calendar curDate = Calendar.getInstance();
                        curDate.set(year, month - 1, date);

                        //Move the target day N business days out
                        Calendar targetDate = getTargetDate(year, month, date, DAYS_IN_FUTURE);

                        //Save Prediction
                        BigDecimal bd = new BigDecimal(String.valueOf(clsLabel));
                        PredictionValues val = new PredictionValues(ticker.getTicker(), curDate.getTime(), targetDate.getTime(), MODEL_TYPE.toString(), PRED_TYPE, bd);
                        listPredictions.add(val);
                    }
                    
                    //Save Predictions to DB - Save all predictions for one stock at a time
                    sdh.insertStockPredictions(listPredictions);
                    break;

                case RAND_FORST:
                    
                    modelPath = MODEL_PATH + "\\" + ticker.getTicker() + "-RandomForest.model";
                    RandomForest rf = (RandomForest)SerializationHelper.read(modelPath);

                    sr = new StringReader(dataExamples);
                    test = new Instances(sr);
                    sr.close();
                    
                    test.setClassIndex(test.numAttributes() - 1);
                    
                    // label instances
                    listPredictions = new ArrayList<>();
                    for (int i = 0; i < test.numInstances(); i++) {
                        double clsLabel = rf.classifyInstance(test.instance(i));

                        double[] array = test.instance(i).toDoubleArray();
                        int year = (int)array[0];
                        int month = (int)array[1];
                        int date = (int)(int)array[2];
                        
                        Calendar curDate = Calendar.getInstance();
                        curDate.set(year, month - 1, date);

                        //Move the target day N business days out
                        Calendar targetDate = getTargetDate(year, month, date, DAYS_IN_FUTURE);

                        //Save Prediction
                        BigDecimal bd = new BigDecimal(String.valueOf(clsLabel));
                        PredictionValues val = new PredictionValues(ticker.getTicker(), curDate.getTime(), targetDate.getTime(), MODEL_TYPE.toString(), PRED_TYPE, bd);
                        listPredictions.add(val);
                    }
                    
                    //Save Predictions to DB - Save all predictions for one stock at a time
                    sdh.insertStockPredictions(listPredictions);
                    break;
             }

        }

    }

    private Calendar getTargetDate(final int YEAR, final int MONTH, final int DATE, final int DAYS_OUT) {

        Calendar targetDate = Calendar.getInstance();
        targetDate.set(YEAR, MONTH - 1, DATE);
        int daysInAdvance = 0;

        for (;;) {
            
            if (daysInAdvance == DAYS_OUT)
                break;

            //Weekend
            if (targetDate.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY || targetDate.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY)
                targetDate.add(Calendar.DATE, 1);
            //Business Days
            else {
                targetDate.add(Calendar.DATE, 1);
                daysInAdvance++;
            }
        }
        
        return targetDate;
    }
    
    public void topNBacktest(int NUM_STOCKS, final Date FROM_DATE, final Date TO_DATE) throws Exception {
        
        String summary = "Number Stocks: " + NUM_STOCKS + ", From: " + FROM_DATE + ", To: " + TO_DATE;
        logger.Log("Predictor", "topNBacktest", summary, "");
        
        Calendar curDate = Calendar.getInstance();
        curDate.setTime(FROM_DATE);
        
        Calendar finalDate = Calendar.getInstance();
        finalDate.setTime(TO_DATE);

        //Financial Amounts
        final BigDecimal TRADING_COST = new BigDecimal("10.00");
        final BigDecimal STARTING_CAPITAL = new BigDecimal("40000.00");
        final BigDecimal MIN_CASH = new BigDecimal("200.00");
        BigDecimal currentCapital = new BigDecimal(STARTING_CAPITAL.toString());
        
        List<StockHolding> stockHoldings = new ArrayList<>();
        List<PendingOrder> stockOrders = new ArrayList<>();
        
        int dayOfWeek;
        StockDataHandler sdh = new StockDataHandler();
        for (;;) {

            if (curDate.compareTo(finalDate) >= 0)
                break;
            
            //Weekend Test
            dayOfWeek = curDate.get(Calendar.DAY_OF_WEEK);
            if (dayOfWeek == Calendar.SATURDAY || dayOfWeek == Calendar.SUNDAY) {
                curDate.add(Calendar.DATE, 1);
                continue;
            }

            //Holiday Test
            StockQuote testQuote = sdh.getStockQuote("AXP", curDate.getTime());
            if (testQuote == null) {
                curDate.add(Calendar.DATE, 1);
                continue;
            }

            /*
            //Portfolio Value
            BigDecimal acctValue = new BigDecimal(currentCapital.toString());
            for (int i = 0; i < stockHoldings.size(); i++) {
                StockHolding holding = stockHoldings.get(i);
                StockQuote quote = sdh.getStockQuote(holding.getTicker(), curDate.getTime());
                BigDecimal numShares = new BigDecimal(String.valueOf(holding.getSharesHeld()));
                BigDecimal stockValue = quote.getClose().multiply(numShares);

                acctValue = acctValue.add(stockValue);
            }
            
            System.out.printf("Portfolio Value: %s %n", acctValue.toString());

            //Broke Test
            if (acctValue.doubleValue() < 1000) {
                System.out.println("YOUR BROKE!");
                break;
            }
            */
            
            //Execute pending stock orders
            while (stockOrders.size() > 0) {
                PendingOrder order = stockOrders.get(0);
                StockQuote quote = sdh.getStockQuote(order.getTicker(), curDate.getTime());
                BigDecimal curPrice = quote.getOpen();
                BigDecimal numShares = new BigDecimal(String.valueOf(order.getNumShares()));
                
                switch (order.getType()) {
                    case BUY:
                        BigDecimal orderCost = curPrice.multiply(numShares);
                        currentCapital = currentCapital.subtract(orderCost).subtract(TRADING_COST); //Commission
                
                        StockHolding stock = new StockHolding(order.getTicker(), order.getNumShares(), order.getProjectedDt());
                        stockHoldings.add(stock);

                        String buyActivity = String.format("BUY: Ticker: %s, Shares: %d, Cost: %s, Projected Date: %s %n", order.getTicker(), order.getNumShares(), curPrice.toPlainString(), order.getProjectedDt().toString());
                        logger.Log("Predictor", "topNBacktest", buyActivity, "");
                        
                        break;
                        
                    case SELL:
                        BigDecimal proceeds = curPrice.multiply(numShares);
                        currentCapital = currentCapital.add(proceeds).subtract(TRADING_COST); //Commission

                        String sellActivity = String.format("SELL: Ticker: %s, Shares: %d, Cost: %s %n", order.getTicker(), order.getNumShares(), curPrice.toPlainString());
                        logger.Log("Predictor", "topNBacktest", sellActivity, "");

                        //Remove the holding record
                        for (int i = 0; i < stockHoldings.size(); i++) {
                            String holding = stockHoldings.get(i).getTicker();
                            if (holding.equals(order.getTicker())) {
                                stockHoldings.remove(i);
                                break;
                            }
                        }

                        //Last order?
                        if (stockOrders.size() == 1) {
                            String sumValue = String.format("Value = %.2f, Date = %s %n", currentCapital.doubleValue(), curDate.getTime().toString());
                            logger.Log("Predictor", "topNBacktest", sumValue, "");
                        }
                        
                        break;
                }
                
                //Delete Order
                stockOrders.remove(0);
            }

            //Have any holdings expired - enter sale order night of targetDate
            for (int i = 0; i < stockHoldings.size(); i++) {
                Date tDate = stockHoldings.get(i).getTargetDate();
                Date curDt = curDate.getTime();
                
                if (curDt.compareTo(tDate) >= 0) {
                    StockHolding stock = stockHoldings.get(i);
                    PendingOrder order = new PendingOrder(OrderType.SELL, stock.getTicker(), stock.getSharesHeld(), null);
                    stockOrders.add(order);
                }
            }
            
            //See how many stock we can buy
            int openings = NUM_STOCKS - stockHoldings.size();
            if (openings > 0) {

                //Get List of Stocks forecasted to go up
                List<String> stockList = sdh.getPostiveClassificationPredictions(curDate.getTime());
                if (stockList.size() > 0) {

                    //Get the Forecasted Percent Change for each stock
                    FuturePrice fp;
                    List<FuturePrice> fpList = new ArrayList<>();
                    for (String stock : stockList) {
                        fp = sdh.getTargetValueRegressionPredictions(stock, curDate.getTime());
                        fpList.add(fp);
                    }
                    
                    //Filter to the top N stocks by the forecasted % change
                    Collections.sort(fpList);
                    
                    //Generate buy orders
                    int size = fpList.size();
                    if (size >= NUM_STOCKS) {
                        
                        for (int i = size - 1; i > (size - 1 - openings); i--) {

                            String ticker = fpList.get(i).getTicker();
                            Date projDt = fpList.get(i).getProjectedDt();

                            StockQuote quote = sdh.getStockQuote(ticker, curDate.getTime());
                            BigDecimal curPrice = quote.getClose();

                            BigDecimal numOpenings = new BigDecimal(String.valueOf(openings));
                            int numShares = currentCapital.subtract(MIN_CASH)
                                            .divide(numOpenings, RoundingMode.HALF_DOWN)
                                            .divide(curPrice, RoundingMode.HALF_DOWN)
                                            .intValue();

                            PendingOrder order = new PendingOrder(OrderType.BUY, ticker, numShares, projDt);
                            stockOrders.add(order);
                        } //End Buy orders Loop
                    } //Minimum Viable Stocks IF
                } //Stock List IF
            } //Openings IF
            
            //Move forward
            curDate.add(Calendar.DATE, 1);
        }
    }
    
    //Method: backtest
    //Description: Looks through all the stocks and backtests single stock and single model at a time
    public void backtest(final ModelTypes MODEL_TYPE, final Date FROM_DATE, final Date TO_DATE) throws Exception {

        //Commissions
        final BigDecimal TRADING_COST = new BigDecimal("10.00");
        
        //Loop through all stocks
        List<BacktestingResults> listResults = new ArrayList<>();
        StockDataHandler sdh = new StockDataHandler();
        List<StockTicker> stockList = sdh.getAllStockTickers(); 
        for (StockTicker ticker : stockList) {
        
            try {
                logger.Log("Predictor", "backtest", ticker.getTicker(), "");

                //Run through all predictions for a given stock
                List<PredictionValues> listPredictions = sdh.getStockBackTesting(ticker.getTicker(), 
                                                                                 MODEL_TYPE.toString(), 
                                                                                 FROM_DATE, TO_DATE);
                BigDecimal capital = new BigDecimal("10000.0");
                int sharesOwned = 0;
                int numTrades = 0;
                BigDecimal curClosePrice = null;
                BigDecimal curOpenPrice = null;
                boolean buyFlag = false;
                boolean sellFlag = false;
                int waitDays = 0;
                Date projectedDate = null;
                
                //Used for Buy and Hold values
                boolean isFirstDate = true;
                BigDecimal firstPrice = null;

                Prices:
                for (PredictionValues pred : listPredictions) {
                    curOpenPrice = pred.getCurOpenValue();
                    curClosePrice = pred.getCurCloseValue();
                    
                    //Used for Buy and Hold
                    if (isFirstDate) {
                        firstPrice = curOpenPrice;
                        isFirstDate = false;
                    }

                    if (waitDays > 0)
                        waitDays--;

                    //Buy Stock
                    if (buyFlag && sharesOwned == 0 && waitDays == 0) {

                        buyFlag = false;
                        projectedDate = pred.getProjectedDate(); //Must hold the stock until the projected date

                        //Broke Test
                        if (capital.doubleValue() < 1000) {
                            System.out.println("YOUR BROKE!");
                            break Prices;
                        }

                        capital = capital.add(TRADING_COST.negate()); //Commission
                        sharesOwned = capital.divide(curOpenPrice, 0, RoundingMode.DOWN).intValue();
                        BigDecimal cost = curOpenPrice.multiply(new BigDecimal(sharesOwned));
                        capital = capital.add(cost.negate());
                        numTrades++;
                    }
                    //Sell Stock
                    else if (sellFlag && sharesOwned > 0 && pred.getDate().getTime() >= projectedDate.getTime()) {

                        sellFlag = false;
                        waitDays = 3; //Due to Free Ride restrictions in Roth IRA, THIS HURTS PERFORMANCE BAD!!!

                        BigDecimal proceeds = curOpenPrice.multiply(new BigDecimal(sharesOwned));
                        capital = capital.add(proceeds);
                        capital = capital.add(TRADING_COST.negate()); //Commission
                        sharesOwned = 0;
                        numTrades++;
                    }
                    //Hold for another week
                    else if (buyFlag && sharesOwned > 0 && pred.getDate().getTime() >= projectedDate.getTime()) {
                        projectedDate = pred.getProjectedDate(); //Must hold the stock until the projected date
                    }
                    
                    //Evaluate Models for Buy and Sell decisions
                    switch (MODEL_TYPE) {
                        case M5P:
                        case LINEAR_REG:
                            //Buy
                            if (pred.getEstimatedValue().doubleValue() > curOpenPrice.doubleValue()) 
                                buyFlag = true;
                            //Sell
                            else if (pred.getEstimatedValue().doubleValue() < curOpenPrice.doubleValue())
                                sellFlag = true;

                            break;

                        case LOGIST_REG:
                            //Buy
                            if (pred.getEstimatedValue().doubleValue() > 0.50) 
                                buyFlag = true;
                            //Sell
                            else if (pred.getEstimatedValue().doubleValue() < 0.50) 
                                sellFlag = true;

                            break;
                            
                        case RAND_FORST: //Will only return 1.0 or 0.0
                            //Buy
                            if (pred.getEstimatedValue().doubleValue() > 0.50) 
                                buyFlag = true;
                            //Sell
                            else if (pred.getEstimatedValue().doubleValue() < 0.50) 
                                sellFlag = true;

                            break;
                    }

                } //End for loop

                //Sum up all assets
                final BigDecimal ORIG_CAPITAL = new BigDecimal("10000.00");
                final BigDecimal MULTIPLIER = new BigDecimal("100.00");

                BigDecimal totalAssets = curClosePrice.multiply(new BigDecimal(sharesOwned)).add(capital);
                BigDecimal pctChg = totalAssets.add(ORIG_CAPITAL.negate())
                                               .divide(ORIG_CAPITAL, 2, RoundingMode.UP)
                                               .multiply(MULTIPLIER);

                BigDecimal buyAndHoldChg = curClosePrice.add(firstPrice.negate())
                                                        .divide(firstPrice, 2, RoundingMode.HALF_UP)
                                                        .multiply(MULTIPLIER);

                BacktestingResults results = new BacktestingResults(ticker.getTicker(), MODEL_TYPE.toString(), 
                        FROM_DATE, TO_DATE, numTrades, pctChg, buyAndHoldChg);
                
                listResults.add(results);

            } catch(Exception exc) {
                logger.Log("Predictor", "backtest", "Exception", ticker.getTicker() + ": " + exc.toString());
                throw exc;
            }

        } //End ticker loop
        
        //Save to DB
        sdh.setStockBacktestingIntoDB(listResults);
    }
}
