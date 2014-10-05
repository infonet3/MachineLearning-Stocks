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
import java.io.FileInputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import weka.classifiers.trees.RandomForest;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.SerializationHelper;

/**
 *
 * @author Matt Jones
 */
public class Predictor {

    final String CONF_FILE = "settings.conf";
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
 
        //Loop through all stocks for the given day
        StockDataHandler sdh = new StockDataHandler();
        List<StockTicker> stockList = sdh.getAllStockTickers(); 
        for (StockTicker ticker : stockList) {
            
            //Get Features for the selected dates
            String dataExamples = sdh.getAllStockFeaturesFromDB(ticker.getTicker(), DAYS_IN_FUTURE, MODEL_TYPE, fromDate, toDate);

            //Load the model
            switch (MODEL_TYPE) {
                case LINEAR_REG:
                    break;
                case LOGIST_REG:
                    break;
                case RAND_FORST:
                    System.gc();
                    
                    String modelPath = MODEL_PATH + "\\" + ticker.getTicker() + ".model";
                    RandomForest rf = (RandomForest)SerializationHelper.read(modelPath);

                    StringReader sr = new StringReader(dataExamples);
                    Instances test = new Instances(sr);
                    sr.close();
                    
                    test.setClassIndex(test.numAttributes() - 1);
                    
                    // label instances
                    List<PredictionValues> listPredictions = new ArrayList<>();
                    for (int i = 0; i < test.numInstances(); i++) {
                        double clsLabel = rf.classifyInstance(test.instance(i));

                        double[] array = test.instance(i).toDoubleArray();
                        int year = (int)array[0];
                        int month = (int)array[1];
                        int date = (int)(int)array[2];
                        
                        Calendar curDate = Calendar.getInstance();
                        curDate.set(year, month - 1, date);

                        //Move the target day N business days out
                        Calendar targetDate = Calendar.getInstance();
                        targetDate.set(year, month - 1, date);
                        int daysInAdvance = 0;
                        for (;;) {
                            //Weekend
                            if (targetDate.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY || targetDate.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY)
                                targetDate.add(Calendar.DATE, 1);
                            //Business Days
                            else {
                                targetDate.add(Calendar.DATE, 1);
                                daysInAdvance++;
                            }
                            
                            if (daysInAdvance == DAYS_IN_FUTURE)
                                break;
                        }
                        
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
    
    public void backtest(final ModelTypes MODEL_TYPE, final Date FROM_DATE, final Date TO_DATE) throws Exception {

        //Commissions
        final BigDecimal TRADING_COST = new BigDecimal("10.00");
        
        //Loop through all stocks
        List<BacktestingResults> listResults = new ArrayList<>();
        StockDataHandler sdh = new StockDataHandler();
        List<StockTicker> stockList = sdh.getAllStockTickers(); 
        for (StockTicker ticker : stockList) {
        
            try {
                System.out.println("Backtest Stock: " + ticker.getTicker());

                //Run through all predictions for a given stock
                List<PredictionValues> listPredictions = sdh.getStockBackTesting(ticker.getTicker(), MODEL_TYPE.toString(), FROM_DATE, TO_DATE);
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
                BigDecimal pctChg = totalAssets.add(ORIG_CAPITAL.negate()).divide(ORIG_CAPITAL, 2, RoundingMode.UP).multiply(MULTIPLIER);

                BigDecimal buyAndHoldChg = curClosePrice.add(firstPrice.negate()).divide(firstPrice, 2, RoundingMode.HALF_UP).multiply(MULTIPLIER);

                BacktestingResults results = new BacktestingResults(ticker.getTicker(), MODEL_TYPE.toString(), FROM_DATE, TO_DATE, numTrades, pctChg, buyAndHoldChg);
                listResults.add(results);

            } catch(Exception exc) {
                System.out.println("Method: backtest, Ticker: " + ticker.getTicker() + ", Desc: " + exc);
            }

        } //End ticker loop
        
        //Save to DB
        sdh.setStockBacktestingIntoDB(listResults);
    }
}
