/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Runner;

import Modeling.RunModels;
import StockData.StockDataHandler;

/**
 *
 * @author Matt Jones
 */
public class Main {
    public static void main(String... args) throws Exception {
        
        StockDataHandler downloader = new StockDataHandler();
        downloader.downloadAllStockData();

        downloader.computeMovingAverages();
        downloader.computeStockQuoteSlopes();
        
        //RunModels models = new RunModels();
        //models.runModels();
    }
}
