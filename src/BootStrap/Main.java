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
        
        //StockDataHandler downloader = new StockDataHandler();
        //downloader.downloadAllStockData();
        
        //downloader.computeMovingAverages();
        //downloader.computeStockQuoteSlopes();

        final ModelTypes MODEL_TYPE = ModelTypes.LINEAR_REG;
        final int DAYS_IN_FUTURE = 28;
        
        //RunModels models = new RunModels();
        //models.runModels(modelType, DAYS_IN_FUTURE);
        
        predictStockValues(MODEL_TYPE, DAYS_IN_FUTURE);
    }
    
    private static void predictStockValues(final ModelTypes MODEL_TYPE, final int DAYS_IN_FUTURE) throws Exception {
        
        //Set starting date for predictions
        Calendar today = Calendar.getInstance();
        Calendar cal = Calendar.getInstance();
        cal.set(2014, 5, 2);
        
        //Now loop through all days until today
        Predictor pred = new Predictor();
        for (;;) {
            
            System.out.println("Date = " + cal.getTime().toString());
            
            if (cal.compareTo(today) > 0) {
                break;
            }
            
            switch (cal.get(Calendar.DAY_OF_WEEK)) {
                case Calendar.MONDAY:
                case Calendar.TUESDAY:
                case Calendar.WEDNESDAY:
                case Calendar.THURSDAY:
                case Calendar.FRIDAY:
                    //Predict Future Prices Based on the Models
                    pred.predictAllStocks(MODEL_TYPE, DAYS_IN_FUTURE, cal.getTime());
                    break;
                    
                case Calendar.SATURDAY:
                case Calendar.SUNDAY:
                    break;
            }
            
            //Move to next day
            cal.add(Calendar.DATE, 1); 
        }
    }
}
