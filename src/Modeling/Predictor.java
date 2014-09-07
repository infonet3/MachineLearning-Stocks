/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Modeling;

import StockData.PredictionValues;
import ML_Formulas.LinearRegFormulas;
import ML_Formulas.LogisticRegFormulas;
import MatrixOps.MatrixValues;
import static Modeling.ModelTypes.LINEAR_REG;
import static Modeling.ModelTypes.LOGIST_REG;
import StockData.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 *
 * @author Matt Jones
 */
public class Predictor {
    
    public void predictAllStocksForDates(final ModelTypes MODEL_TYPE, final int DAYS_IN_FUTURE, final Date fromDate, final Date toDate) throws Exception {
 
        //Loop through all stocks for the given day
        StockDataHandler sdh = new StockDataHandler();
        List<StockTicker> stockList = sdh.getAllStockTickers(true); //FIX THIS LATER, SET TO FALSE!!!!
        for (StockTicker ticker : stockList) {

            //Get Features for the selected dates
            List<Features> featureList = sdh.getAllStockFeaturesFromDB(ticker.getTicker(), DAYS_IN_FUTURE, true, MODEL_TYPE, fromDate, toDate);

            //If features are unavailable for this date then skip it
            if (featureList.isEmpty()) {
                System.out.println("Method: predictAllStocksForDate, Ticker: " + ticker.getTicker() + ", No Features Available!");
                continue;
            }
                
            //Extract the weights
            List<Weight> listWeights = sdh.getWeights(ticker.getTicker(), MODEL_TYPE);
            int numWeights = listWeights.size();
            
            //Sanity Check
            int featureSize = featureList.get(0).getFeatureValues().length;
            if (featureSize != numWeights) {
                System.out.println("Method: predictAllStocks, Desc: Number of features != Number of weights."); 
                continue;
            }
            
            double[] weights = new double[numWeights];
            double[] avg = new double[numWeights];
            double[] range = new double[numWeights];
            
            for (int i = 0; i < weights.length; i++) {
                weights[i] = listWeights.get(i).getTheta().doubleValue();
                avg[i] = listWeights.get(i).getAverage().doubleValue();
                range[i] = listWeights.get(i).getRange().doubleValue();
            }
            
            //Mean Normalization of the features & Get date from features
            int listSize = featureList.size();
            Calendar[] curDates = new Calendar[listSize];
            for (int i = 0; i < listSize; i++) {
                
                Features f = featureList.get(i);
                Date dt = f.getDate();
                double[] row = f.getFeatureValues();

                curDates[i] = Calendar.getInstance();
                curDates[i].setTime(dt);
                
                double[] normalizedRow = MatrixValues.meanNormalization(row, avg, range);
                f = new Features(dt, normalizedRow);
                featureList.set(i, f);
            }
            
            //Calculate the Hypothesis
            double[] hypothesisValues = new double[listSize];
            for (int i = 0; i < listSize; i++) {
                
                switch (MODEL_TYPE) {
                    case LINEAR_REG:
                        hypothesisValues[i] = LinearRegFormulas.hypothesis(featureList.get(i).getFeatureValues(), weights);
                        break;
                    case LOGIST_REG:
                        hypothesisValues[i] = LogisticRegFormulas.hypothesis(featureList.get(i).getFeatureValues(), weights);
                        break;
                }
            }
 
            //Set the predicted target date
            Calendar[] targetDates = new Calendar[listSize];
            for (int i = 0; i < listSize; i++) {
                //Now increment the target date
                targetDates[i] = (Calendar)curDates[i].clone();
                if (DAYS_IN_FUTURE == 1 && targetDates[i].get(Calendar.DAY_OF_WEEK) == Calendar.FRIDAY) 
                    targetDates[i].add(Calendar.DATE, 3);
                else 
                    targetDates[i].add(Calendar.DATE, DAYS_IN_FUTURE);
            }

            //Add the predictions to the list
            List<PredictionValues> listPredictions = new ArrayList<>();
            for (int i = 0; i < listSize; i++) {
                BigDecimal bd = new BigDecimal(hypothesisValues[i]);
                PredictionValues val = new PredictionValues(ticker.getTicker(), curDates[i].getTime(), targetDates[i].getTime(), MODEL_TYPE.toString(), bd);
                listPredictions.add(val);
            }

            //Save Predictions to DB - Save all predictions for one stock at a time
            sdh.setStockPredictions(listPredictions);

        } //End For Loop
        
    }
    
    public void backtest(final ModelTypes MODEL_TYPE, final Date FROM_DATE, final Date TO_DATE) throws Exception {

        //Commissions
        final BigDecimal TRADING_COST = new BigDecimal("10.00");
        
        //Loop through all stocks
        List<BacktestingResults> listResults = new ArrayList<>();
        StockDataHandler sdh = new StockDataHandler();
        List<StockTicker> stockList = sdh.getAllStockTickers(true); //FIX THIS LATER, SET TO FALSE!!!!
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
