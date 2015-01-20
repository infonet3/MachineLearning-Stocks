/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Modeling;

import StockData.StockHolding;
import StockData.PredictionValues;
import static Modeling.ModelTypes.LINEAR_REG;
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
import weka.classifiers.functions.LinearRegression;
import weka.classifiers.trees.M5P;
import weka.classifiers.trees.RandomForest;
import weka.core.Instances;
import weka.core.SerializationHelper;

/**
 *
 * @author Matt Jones
 */
public class Predictor implements Runnable {

    static Logger logger = new Logger();
    final String CONF_FILE = "Resources/settings.conf";
    String modelPathRoot;
    
    ModelTypes modelType;
    int daysInFutureModel;
    int targetDaysOut;
    Date fromDate;
    Date toDate;
    String predType;

    public Predictor() throws Exception {

        //Load the file settings
        Properties p = new Properties();
        try (FileInputStream fis = new FileInputStream(CONF_FILE)) {
            p.load(fis);
            modelPathRoot = p.getProperty("model_directory");
        }
    }

    //Used when multithreading is needed to generate predictions
    public Predictor(final ModelTypes MODEL_TYPE, final int DAYS_IN_FUTURE_MODEL, final int TARGET_DAYS_OUT, final Date FROM_DATE, final Date TO_DATE, final String PRED_TYPE) throws Exception {

        this();
        
        this.modelType = MODEL_TYPE;
        this.daysInFutureModel = DAYS_IN_FUTURE_MODEL;
        this.targetDaysOut = TARGET_DAYS_OUT;
        this.fromDate = FROM_DATE;
        this.toDate = TO_DATE;
        this.predType = PRED_TYPE;
    }

    @Override
    public void run() {

        try {
            predictAllStocksForDates(modelType, daysInFutureModel, targetDaysOut, fromDate, toDate, predType);
        } catch(Exception exc) {
            try {
                logger.Log("Predictor", "run", "Exception", exc.toString(), true);
            } catch(Exception exc2) {
                System.out.println(exc2.toString());
            }
            
            System.exit(25);
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
            String dataExamples = sdh.getAllStockFeaturesFromDB(ticker.getTicker(), DAYS_IN_FUTURE_MODEL, MODEL_TYPE, calFrom.getTime(), calTo.getTime(), false);
            
            //Load the model
            String curModelPath;
            Classifier classifier = null;
            switch (MODEL_TYPE) {

                case M5P:
                    curModelPath = modelPathRoot + "/" + ticker.getTicker() + "-M5P.model";
                    M5P mp = (M5P)SerializationHelper.read(curModelPath);
                    classifier = mp;
                    break;

                case RAND_FORST:
                    curModelPath = modelPathRoot + "/" + ticker.getTicker() + "-RandomForest.model";
                    RandomForest rf = (RandomForest)SerializationHelper.read(curModelPath);
                    classifier = rf;
                    break;
                    
                case LINEAR_REG:
                    curModelPath = modelPathRoot + "/" + ticker.getTicker() + "-LinearRegression.model";
                    LinearRegression linReg = (LinearRegression)SerializationHelper.read(curModelPath);
                    classifier = linReg;
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
    
    public List<StockHolding> topStockPicks(final int NUM_STOCKS, final Date RUN_DATE, final String PRED_TYPE) throws Exception {

        logger.Log("Predictor", "topStockPicks", "Stock Count = " + NUM_STOCKS, "", false);
        
        List<StockHolding> topStocks = new ArrayList<>();
        final double MIN_RATIO = 0.99;
        
        try {
            //Ensure the predictions are present before continuing
            StockDataHandler sdh = new StockDataHandler();
            double predToTickerPct = sdh.getPredToTickersPct(RUN_DATE);
            String summary = String.format("Predictions To Ticker Percent = %.2f", predToTickerPct);
            logger.Log("Predictor", "topStockPicks", summary, "", false);
            
            if (predToTickerPct < MIN_RATIO) {
                logger.Log("Predictor", "topStockPicks", "Prediction to Ticker Ratio TOO LOW!", "", true);
                System.exit(30);
            }
            
            //Get List of Stocks forecasted to go up
            List<String> stockList = sdh.getPostiveClassificationPredictions(RUN_DATE, PRED_TYPE);
            if (stockList.size() >= NUM_STOCKS) {

                //Get the Forecasted Percent Change for each stock
                FuturePrice fp;
                List<FuturePrice> fpList = new ArrayList<>();
                final String MODEL_TYPE = "M5P";
                for (String stock : stockList) {
                    fp = sdh.getTargetValueRegressionPredictions(stock, RUN_DATE, PRED_TYPE, MODEL_TYPE);
                    fpList.add(fp);
                }

                //Filter to the top N stocks by the forecasted % change
                Collections.sort(fpList);

                //Pick Top stocks
                int size = fpList.size();
                for (int i = size - 1; i > (size - 1 - NUM_STOCKS); i--) {
                    String ticker = fpList.get(i).getTicker();
                    Date projDt = fpList.get(i).getProjectedDt();
                    double pctChg = fpList.get(i).getForecastPctChg();

                    StockHolding stk = new StockHolding(ticker, 0, projDt);
                    topStocks.add(stk);
                    
                    String output = String.format("Ticker: %s, Forecast Pct Chg: %f, Target Date: %s", ticker, pctChg, projDt.toString());
                    logger.Log("Predictor", "topStockPicks", output, "", false);
                } 
            }
            
        } catch(Exception exc) {
            logger.Log("Predictor", "topStockPicks", "Exception", exc.toString(), true);
            throw exc;
        }
        
        return topStocks;
    }

    /*Method: topNBacktest
     *Description: Always trades at the open price 
     */
    public void topNBacktest(int NUM_STOCKS, final Date FROM_DATE, final Date TO_DATE, final int YEARS_BACK, final int DAYS_IN_FUTURE) throws Exception {

        String summary = String.format("Num Stocks: %d, Days In Future: %d, From Date: %s, To Date: %s", NUM_STOCKS, DAYS_IN_FUTURE, FROM_DATE.toString(), TO_DATE.toString());
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
        final BigDecimal TRADING_COST = new BigDecimal("5.00");
        final BigDecimal STARTING_CAPITAL = new BigDecimal("40000.00");
        final BigDecimal MIN_CASH = new BigDecimal("200.00");
        BigDecimal currentCapital = new BigDecimal(STARTING_CAPITAL.toString());
        
        List<StockHolding> stockHoldings = new ArrayList<>();
        List<PendingOrder> stockOrders = new ArrayList<>();
        List<BacktestingResults> testResults = new ArrayList<>();
        
        int dayOfWeek;
        StockDataHandler sdh = new StockDataHandler();

        Map<Date, String> mapHolidays = sdh.getAllHolidays();

        Date buyDate = null;
        Date sellDate = null;
        BigDecimal tmpCapital = null;
        for (int day = 0; ; day++) {

            if (curDate.compareTo(finalDate) >= 0)
                break;
            
            //Generate models - 3 Months
            final int DAYS_FORECAST = 90;
            if (day == 0 || (day % DAYS_FORECAST == 0)) {

                //Save results to DB
                if (testResults.size() > 0) {
                    sdh.setStockBacktestingIntoDB(testResults);
                    testResults.clear();
                }
                
                sdh.removeBacktestingData();
                Date dt = curDate.getTime();
                
                Calendar toCal = Calendar.getInstance();
                toCal.setTime(dt);
                toCal.add(Calendar.DATE, DAYS_FORECAST);
                Date toDt = toCal.getTime();

                //Generate models
                Thread tRandForst = new Thread(new RunModels(ModelTypes.RAND_FORST, DAYS_IN_FUTURE, YEARS_BACK, dt));
                Thread tM5P = new Thread(new RunModels(ModelTypes.M5P, DAYS_IN_FUTURE, YEARS_BACK, dt));
                
                tRandForst.start();
                tM5P.start();
                
                tRandForst.join();
                tM5P.join();

                //Use model to create predictions
                final String PRED_TYPE_BACKTEST = "BACKTEST";
                Thread tRandForstPreds = new Thread(new Predictor(ModelTypes.RAND_FORST, DAYS_IN_FUTURE, DAYS_IN_FUTURE, dt, toDt, PRED_TYPE_BACKTEST));
                Thread tM5PPreds = new Thread(new Predictor(ModelTypes.M5P, DAYS_IN_FUTURE, DAYS_IN_FUTURE, dt, toDt, PRED_TYPE_BACKTEST));
                
                tRandForstPreds.start();
                tM5PPreds.start();
                
                tRandForstPreds.join();
                tM5PPreds.join();
            }
            
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

            //In case we don't have some holidays listed
            StockQuote quote = sdh.getStockQuote("AAPL", curDate.getTime());
            if (quote == null) {
                curDate.add(Calendar.DATE, 1);
                continue;
            }
            
            //Execute pending stock orders
            while (stockOrders.size() > 0) {
                PendingOrder order = stockOrders.get(0);
                quote = sdh.getStockQuote(order.getTicker(), curDate.getTime());
                
                BigDecimal curPrice = quote.getOpen();
                BigDecimal numShares = new BigDecimal(String.valueOf(order.getNumShares()));
                
                switch (order.getType()) {
                    case BUY:
                        
                        if (stockHoldings.isEmpty())
                            tmpCapital = new BigDecimal(currentCapital.toString());
                        
                        BigDecimal orderCost = curPrice.multiply(numShares);
                        currentCapital = currentCapital.subtract(orderCost).subtract(TRADING_COST); //Commission
                
                        StockHolding stock = new StockHolding(order.getTicker(), order.getNumShares(), order.getProjectedDt());
                        stockHoldings.add(stock);

                        buyDate = curDate.getTime();
                        
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
                        
                        sellDate = curDate.getTime();

                        //Last order?
                        if (stockOrders.size() == 1) {
                            String sumValue = String.format("Value = %.2f, Date = %s %n", currentCapital.doubleValue(), curDate.getTime().toString());
                            logger.Log("Predictor", "topNBacktest", sumValue, "", true);

                            BigDecimal sp500PctChg = sdh.getSP500PercentChange(buyDate, sellDate);
                            BigDecimal pctChg = currentCapital.divide(tmpCapital, RoundingMode.HALF_UP);
                            BacktestingResults results = new BacktestingResults(buyDate, sellDate, currentCapital, pctChg, sp500PctChg);
                            testResults.add(results);
                            
                            if (currentCapital.doubleValue() < 1000.00)
                                throw new Exception("YOUR BROKE!");
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

                //Honor 3 wait waiting period - given the trade is execute the day after the buy order, only wait two days
                if (sellDate != null) {
                    Calendar calSale = Calendar.getInstance();
                    calSale.setTime(sellDate);

                    final int WAITING_PERIOD = 2;
                    Dates dates = new Dates();
                    calSale = dates.getTargetDate(calSale, WAITING_PERIOD);

                    //Exit if 3 day waiting period isn't yet met
                    if (curDate.compareTo(calSale) < 0) {
                        curDate.add(Calendar.DATE, 1);
                        continue;
                    }
                }
                
                final String PRED_TYPE = "BACKTEST";
                List<StockHolding> topStocks = topStockPicks(NUM_STOCKS, curDate.getTime(), PRED_TYPE);

                //Generate buy orders
                int size = topStocks.size();
                if (size == NUM_STOCKS) {

                    for (StockHolding holding : topStocks) {

                        String ticker = holding.getTicker();
                        Date projDt = holding.getTargetDate();

                        quote = sdh.getStockQuote(ticker, curDate.getTime());
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
   
}
