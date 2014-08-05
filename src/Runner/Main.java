/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Runner;

import Modeling.ModelTypes;
import Modeling.Predictor;
import Modeling.RunModels;
import StockData.StockDataHandler;
import java.util.Date;

/**
 *
 * @author Matt Jones
 */
public class Main {
    public static void main(String... args) throws Exception {
        
        StockDataHandler downloader = new StockDataHandler();
        //downloader.downloadAllStockData();
        
        //downloader.computeMovingAverages();
        //downloader.computeStockQuoteSlopes();
        
        ModelTypes modelType = ModelTypes.LINEAR_REG;
        int daysInFuture = 28;
        
        RunModels models = new RunModels();
        models.runModels(modelType, daysInFuture);
        
        final String MODEL_TYPE = "LINEAR-REG";
        Date date = new Date();
        Predictor pred = new Predictor();
        pred.predictAllStocks(modelType, daysInFuture, date);
    }
}
