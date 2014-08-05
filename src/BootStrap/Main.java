/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package BootStrap;

import Modeling.ModelTypes;
import Modeling.Predictor;
import Modeling.RunModels;
import StockData.StockDataHandler;
import java.util.Calendar;
import java.util.Date;

/**
 *
 * @author Matt Jones
 */
public class Main {
    public static void main(String... args) throws Exception {
        
        StockDataHandler downloader = new StockDataHandler();
        //downloader.downloadAllStockData();
        
        final int DAYS_BACK = 0;
        //downloader.computeMovingAverages(DAYS_BACK);
        //downloader.computeStockQuoteSlopes(DAYS_BACK);

        final int DAYS_IN_FUTURE = 1;
    
        //Run Linear Regression
        RunModels models = new RunModels();
        final ModelTypes MODEL_TYPE = ModelTypes.LINEAR_REG;
        models.runModels(MODEL_TYPE, DAYS_IN_FUTURE);
        predictStockValues(MODEL_TYPE, DAYS_IN_FUTURE);

        //Run Logistic Regression
        final ModelTypes LOG_REG = ModelTypes.LOGIST_REG;
        models.runModels(LOG_REG, DAYS_IN_FUTURE);
        predictStockValues(LOG_REG, DAYS_IN_FUTURE);
        
    }
    
    private static void predictStockValues(final ModelTypes MODEL_TYPE, final int DAYS_IN_FUTURE) throws Exception {
        
        //Set starting date for predictions
        Calendar today = Calendar.getInstance();
        Calendar cal = Calendar.getInstance();
        cal.set(2011, 0, 3);
        
        //Now loop through all days until today
        Predictor pred = new Predictor();
        for (;;) {
            if (cal.compareTo(today) > 0) {
                break;
            }
            
            switch (cal.get(Calendar.DAY_OF_WEEK)) {
                case Calendar.MONDAY: case Calendar.TUESDAY: case Calendar.WEDNESDAY: case Calendar.THURSDAY: case Calendar.FRIDAY:

                    //Predict Future Prices Based on the Models
                    pred.predictAllStocks(MODEL_TYPE, DAYS_IN_FUTURE, cal.getTime());
                    break;
                    
                case Calendar.SATURDAY: case Calendar.SUNDAY:
                    break;
            }
            
            //Move to next day
            cal.add(Calendar.DATE, 1); 
        }
    }
}
