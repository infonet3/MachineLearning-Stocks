/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Modeling;

import StockData.StockHolding;
import StockData.PredictionValues;
import static Modeling.ModelTypes.LINEAR_REG;
import static Modeling.ModelTypes.LOGIST_REG;
import static Modeling.ModelTypes.RAND_FORST;
import StockData.*;
import Utilities.Dates;
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
import java.util.Map;
import java.util.Properties;
import weka.classifiers.Classifier;
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
    final String CONF_FILE = "Resources/settings.conf";
    final String MODEL_PATH;

    public Predictor() throws Exception {

        //Load the file settings
        Properties p = new Properties();
        try (FileInputStream fis = new FileInputStream(CONF_FILE)) {
            p.load(fis);
            MODEL_PATH = p.getProperty("model_directory");
        }
    }
    
    
    public void predictAllStocksForDates(final ModelTypes MODEL_TYPE, final int DAYS_IN_FUTURE_MODEL, final int TARGET_DAYS_OUT, final Date fromDate, final Date toDate, final String PRED_TYPE) throws Exception {

        String summary = "ModelType: " + MODEL_TYPE + ", From: " + fromDate + ", To: " + toDate + ", Days In Future: " + DAYS_IN_FUTURE_MODEL + ", Target Days Out: " + TARGET_DAYS_OUT + ", Prediction Type: " + PRED_TYPE;
        logger.Log("Predictor", "predictAllStocksForDates", summary, "", false);
        
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
        Dates dates = new Dates();
        StockDataHandler sdh = new StockDataHandler();
        List<StockTicker> stockList = sdh.getAllStockTickers(); 
        for (StockTicker ticker : stockList) {
            
            System.gc();
            
            //Get Features for the selected dates
            String dataExamples = sdh.getAllStockFeaturesFromDB(ticker.getTicker(), DAYS_IN_FUTURE_MODEL, MODEL_TYPE, calFrom.getTime(), calTo.getTime());
            
            //Load the model
            String modelPath;
            Classifier classifier = null;
            switch (MODEL_TYPE) {

                case M5P:
                    modelPath = MODEL_PATH + "/" + ticker.getTicker() + "-M5P.model";
                    M5P mp = (M5P)SerializationHelper.read(modelPath);
                    classifier = mp;
                    break;

                case RAND_FORST:
                    modelPath = MODEL_PATH + "/" + ticker.getTicker() + "-RandomForest.model";
                    RandomForest rf = (RandomForest)SerializationHelper.read(modelPath);
                    classifier = rf;
                    break;
                    
            } //End Switch

            //Sanity Check
            if (classifier == null) {
                logger.Log("Predictor", "predictAllStocksForDates", "Loading Classifier", "Failed to Load Classifier!", true);
                throw new Exception("Failed to Load Classifier!");
            }

            //Load in the records to classify
            Instances test;
            try (StringReader sr = new StringReader(dataExamples)) {
                test = new Instances(sr);
            }
            test.setClassIndex(test.numAttributes() - 1);

            //Label instances
            List<PredictionValues> listPredictions = new ArrayList<>();
            for (int i = 0; i < test.numInstances(); i++) {
                double clsLabel = classifier.classifyInstance(test.instance(i));

                double[] array = test.instance(i).toDoubleArray();
                int year = (int)array[0];
                int month = (int)array[1];
                int date = (int)(int)array[2];

                Calendar curDate = Calendar.getInstance();
                curDate.set(year, month - 1, date);

                //Move the target day N business days out
                Calendar targetDate = dates.getTargetDate(curDate, TARGET_DAYS_OUT);

                //Save Prediction
                BigDecimal bd = new BigDecimal(String.valueOf(clsLabel));
                PredictionValues val = new PredictionValues(ticker.getTicker(), curDate.getTime(), targetDate.getTime(), MODEL_TYPE.toString(), PRED_TYPE, bd);
                listPredictions.add(val);
                
            } //End of Instance Loop

            //Save Predictions to DB - Save all predictions for one stock at a time
            sdh.insertStockPredictions(listPredictions);
            
        } //End ticker list loop
        
    }
    
    public List<StockHolding> topStockPicks(final int NUM_STOCKS, final Date runDate, final String PRED_TYPE) throws Exception {

        logger.Log("Predictor", "topStockPicks", "Stock Count = " + NUM_STOCKS, "", false);
        
        List<StockHolding> topStocks = new ArrayList<>();
        
        try {
            //Get List of Stocks forecasted to go up
            StockDataHandler sdh = new StockDataHandler();
            List<String> stockList = sdh.getPostiveClassificationPredictions(runDate, PRED_TYPE);
            if (stockList.size() >= NUM_STOCKS) {

                //Get the Forecasted Percent Change for each stock
                FuturePrice fp;
                List<FuturePrice> fpList = new ArrayList<>();
                for (String stock : stockList) {
                    fp = sdh.getTargetValueRegressionPredictions(stock, runDate, PRED_TYPE);
                    fpList.add(fp);
                }

                //Filter to the top N stocks by the forecasted % change
                Collections.sort(fpList);

                //Pick Top stocks
                int size = fpList.size();
                for (int i = size - 1; i > (size - 1 - NUM_STOCKS); i--) {
                    String ticker = fpList.get(i).getTicker();
                    Date projDt = fpList.get(i).getProjectedDt();
                    StockHolding stk = new StockHolding(ticker, 0, projDt);
                    topStocks.add(stk);
                } 
            }
            
        } catch(Exception exc) {
            logger.Log("Predictor", "topStockPicks", "Exception", exc.toString(), true);
            throw exc;
        }
        
        return topStocks;
    }
    
    public void topNBacktest(int NUM_STOCKS, final Date FROM_DATE, final Date TO_DATE) throws Exception {
        
        String summary = "Number Stocks: " + NUM_STOCKS + ", From: " + FROM_DATE + ", To: " + TO_DATE;
        logger.Log("Predictor", "topNBacktest", summary, "", false);
        
        Calendar curDate = Calendar.getInstance();
        curDate.setTime(FROM_DATE);
        curDate.set(Calendar.AM_PM, Calendar.AM);
        curDate.set(Calendar.HOUR, 0);
        curDate.set(Calendar.MINUTE, 0);
        curDate.set(Calendar.SECOND, 0);
        curDate.set(Calendar.MILLISECOND, 0);
        
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
        Map<Date, String> mapHolidays = sdh.getAllHolidays();

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
            Date dt = curDate.getTime();
            String holidayCode = mapHolidays.get(dt);
            if (holidayCode != null && holidayCode.equals("Closed")) {
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
                        logger.Log("Predictor", "topNBacktest", buyActivity, "", false);
                        
                        break;
                        
                    case SELL:
                        BigDecimal proceeds = curPrice.multiply(numShares);
                        currentCapital = currentCapital.add(proceeds).subtract(TRADING_COST); //Commission

                        String sellActivity = String.format("SELL: Ticker: %s, Shares: %d, Cost: %s %n", order.getTicker(), order.getNumShares(), curPrice.toPlainString());
                        logger.Log("Predictor", "topNBacktest", sellActivity, "", false);

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
                            logger.Log("Predictor", "topNBacktest", sumValue, "", false);
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

                final String PRED_TYPE = "BACKTEST";
                List<StockHolding> topStocks = topStockPicks(NUM_STOCKS, curDate.getTime(), PRED_TYPE);
                    
                //Generate buy orders
                int size = topStocks.size();
                if (size == NUM_STOCKS) {

                    for (StockHolding holding : topStocks) {

                        String ticker = holding.getTicker();
                        Date projDt = holding.getTargetDate();

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
                logger.Log("Predictor", "backtest", ticker.getTicker(), "", false);

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
                            logger.Log("Predictor", "backtest", "Broke", "Backtesting funds are lower than $1,000", false);
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
                logger.Log("Predictor", "backtest", "Exception", ticker.getTicker() + ": " + exc.toString(), true);
                throw exc;
            }

        } //End ticker loop
        
        //Save to DB
        sdh.setStockBacktestingIntoDB(listResults);
    }
}
