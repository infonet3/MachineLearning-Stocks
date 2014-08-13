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

        if (!(DAYS_IN_FUTURE == 1 || (DAYS_IN_FUTURE % 7) == 0)) {
            throw new Exception("Method: predictAllStocks, Desc: Can only predict 1 day out or a multiple of 7!" );
        }
 
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
                throw new Exception("Method: predictAllStocks, Desc: Number of features != Number of weights."); 
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
        
            //Run through all predictions for a given stock
            List<PredictionValues> listPredictions = sdh.getStockBackTesting(ticker.getTicker(), MODEL_TYPE.toString(), FROM_DATE, TO_DATE);
            BigDecimal capital = new BigDecimal("10000.0");
            int sharesOwned = 0;
            int numTrades = 0;
            BigDecimal curPrice = null;
            boolean buyFlag = false;
            boolean sellFlag = false;
            
            //Used for Buy and Hold values
            boolean isFirstDate = true;
            BigDecimal firstPrice = null;
            
            Prices:
            for (PredictionValues pred : listPredictions) {
                curPrice = pred.getActualValue();
                
                if (isFirstDate) {
                    firstPrice = pred.getActualValue();
                    isFirstDate = false;
                }
                
                switch (MODEL_TYPE) {
                    case LINEAR_REG:
                        if (pred.getEstimatedValue().doubleValue() > curPrice.doubleValue() && sharesOwned == 0) //Buy
                            buyFlag = true;
                        else if (pred.getEstimatedValue().doubleValue() <= curPrice.doubleValue() && sharesOwned > 0) //Sell
                            sellFlag = true;
                            
                        break;

                    case LOGIST_REG:
                        if (pred.getEstimatedValue().doubleValue() > 0.5 && sharesOwned == 0) //Buy
                            buyFlag = true;
                        else if (pred.getEstimatedValue().doubleValue() <= 0.5 && sharesOwned > 0) //Sell
                            sellFlag = true;
                            
                        break;
                }

                //Trading Actions
                if (buyFlag) {

                    buyFlag = false;
                
                    //Broke Test
                    if (capital.doubleValue() < 1000) {
                        System.out.println("YOUR BROKE!");
                        break Prices;
                    }
                            
                    capital = capital.add(TRADING_COST.negate());
                    sharesOwned = capital.divide(curPrice, 0, RoundingMode.DOWN).intValue();
                    BigDecimal cost = pred.getActualValue().multiply(new BigDecimal(sharesOwned));
                    capital = capital.add(cost.negate());
                    numTrades++;
                }
                else if (sellFlag) {
                    
                    sellFlag = false;
                
                    BigDecimal proceeds = curPrice.multiply(new BigDecimal(sharesOwned));
                    capital = capital.add(proceeds);
                    capital = capital.add(TRADING_COST.negate());
                    sharesOwned = 0;
                    numTrades++;
                }
                
            } //End for loop
         
            //Sum up all assets
            final BigDecimal ORIG_CAPITAL = new BigDecimal("10000.00");
            final BigDecimal MULTIPLIER = new BigDecimal("100.00");
            
            BigDecimal totalAssets = curPrice.multiply(new BigDecimal(sharesOwned)).add(capital);
            BigDecimal pctChg = totalAssets.add(ORIG_CAPITAL.negate()).divide(ORIG_CAPITAL, 2, RoundingMode.UP).multiply(MULTIPLIER);

            BigDecimal buyAndHoldChg = curPrice.add(firstPrice.negate()).divide(firstPrice, 2, RoundingMode.HALF_UP).multiply(MULTIPLIER);
            
            BacktestingResults results = new BacktestingResults(ticker.getTicker(), MODEL_TYPE.toString(), FROM_DATE, TO_DATE, numTrades, pctChg, buyAndHoldChg);
            listResults.add(results);

        } //End ticker loop
        
        //Save to DB
        sdh.setStockBacktestingIntoDB(listResults);
    }
}
