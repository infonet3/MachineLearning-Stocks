/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Modeling;

import ML_Formulas.LinearRegFormulas;
import MatrixOps.MatrixValues;
import StockData.*;
import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 *
 * @author Matt Jones
 */
public class Predictor {
    public void predictAllStocks(final ModelTypes MODEL_TYPE, final int DAYS_IN_FUTURE, final Date testDate) throws Exception {

        StockDataHandler sdh = new StockDataHandler();
        List<StockTicker> stockList = sdh.getAllStockTickers();
        for (StockTicker ticker : stockList) {

            //Get Features and pre calculated weights
            double[] features = sdh.getFeatures(ticker.getTicker(), testDate);
            
            //Extract the weights
            List<Weight> listWeights = sdh.getWeights(ticker.getTicker(), MODEL_TYPE);
            int numWeights = listWeights.size();
            
            double[] weights = new double[numWeights];
            double[] avg = new double[numWeights];
            double[] range = new double[numWeights];
            
            for (int i = 0; i < weights.length; i++) {
                weights[i] = listWeights.get(i).getTheta().doubleValue();
                avg[i] = listWeights.get(i).getAverage().doubleValue();
                range[i] = listWeights.get(i).getRange().doubleValue();
            }
            
            //Mean Normalization of the features
            features = MatrixValues.meanNormalization(features, avg, range);
            
            //Hypothesis
            double value = LinearRegFormulas.hypothesis(features, weights);
            BigDecimal bd = new BigDecimal(value);

            Calendar c = Calendar.getInstance();
            c.add(Calendar.DAY_OF_MONTH, DAYS_IN_FUTURE);
            
            sdh.setStockPredictions(ticker.getTicker(), c.getTime(), MODEL_TYPE, bd);
        }
    }
}
