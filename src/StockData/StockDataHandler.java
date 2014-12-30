/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package StockData;

import Modeling.FuturePrice;
import Modeling.ModelTypes;
import static Modeling.ModelTypes.LOGIST_REG;
import static Modeling.ModelTypes.RAND_FORST;
import static Modeling.ModelTypes.SVM;
import Utilities.Logger;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import com.mysql.jdbc.jdbc2.optional.*;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import javax.json.Json;
import javax.json.stream.JsonParser;

/**
 *
 * @author Matt Jones
 */
public class StockDataHandler {

    static Logger logger = new Logger();
    
    final String CONF_FILE = "Resources/settings.conf";
    final String STOCK_TICKERS_PATH;
    final String MODEL_DATA_OUTPUT_PATH;
    
    final String MYSQL_SERVER_HOST;
    final String MYSQL_SERVER_PORT;
    final String MYSQL_SERVER_DB;
    final String MYSQL_SERVER_LOGIN;
    final String MYSQL_SERVER_PASSWORD;
    
    final String QUANDL_AUTH_TOKEN;
    final String QUANDL_BASE_URL;
    
    final String BEA_USER_ID;
    
    public StockDataHandler() throws Exception {
        //Load the file settings
        Properties p = new Properties();
        try (FileInputStream fis = new FileInputStream(CONF_FILE)) {
            p.load(fis);
            MYSQL_SERVER_HOST = p.getProperty("mysql_server_host");
            MYSQL_SERVER_PORT = p.getProperty("mysql_server_port");
            MYSQL_SERVER_DB = p.getProperty("mysql_server_db");
            MYSQL_SERVER_LOGIN = p.getProperty("mysql_server_login");
            MYSQL_SERVER_PASSWORD = p.getProperty("mysql_server_password");
    
            STOCK_TICKERS_PATH = p.getProperty("stock_tickers_path");
            MODEL_DATA_OUTPUT_PATH = p.getProperty("model_data_output_path");
            
            QUANDL_AUTH_TOKEN = p.getProperty("quandl_auth_token");
            QUANDL_BASE_URL = p.getProperty("quandl_base_url");
            
            BEA_USER_ID = p.getProperty("bea_user_id");
        }
    }
    
    private Connection getDBConnection() throws Exception {
        MysqlDataSource dataSource = new MysqlDataSource();
        Connection conxn = null;
        
        try {
            dataSource.setServerName(MYSQL_SERVER_HOST);
            dataSource.setPort(Integer.parseInt(MYSQL_SERVER_PORT));
            dataSource.setDatabaseName(MYSQL_SERVER_DB);
            conxn  = dataSource.getConnection(MYSQL_SERVER_LOGIN, MYSQL_SERVER_PASSWORD);

        } catch (Exception exc) {
            Notifications.EmailActions.SendEmail("ML Notification - Database Error", "Cannot connect to DB.  Details: " + exc.toString());
            System.exit(3);
        }
        
        return conxn;
    }

    public void removeBacktestingData() throws Exception {
        
        logger.Log("StockDataHandler", "removeBacktestingData", "", "", false);

        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Remove_BackTest_Data()}")) {

            stmt.executeUpdate();
            
        } catch(Exception exc) {
            logger.Log("StockDataHandler", "removeBacktestingData", "Exception", exc.toString(), true);
            throw exc;
        }
    }
    
    public void computeMovingAverages() throws Exception {
        
        logger.Log("StockDataHandler", "computeMovingAverages", "", "", false);
        
        List<StockTicker> tickers = getAllStockTickers(); 
        
        final int FIVE = 5;
        final int TWENTY = 20;
        final int SIXTY = 60;

        //Iterate through all stock tickers
        for (StockTicker stockTicker : tickers) {
            
            //Iterate through the stock prices
            List<MovingAverage> listMAs = new ArrayList<>();
            List<StockPrice> priceList = getAllStockQuotes(stockTicker.getTicker()); //Desc order

            int numPrices = priceList.size();
            for (int i = 0; i < numPrices; i++) {
                
                StockPrice stkPrice = priceList.get(i);
                boolean isUpdated = false;
                
                //Five Day MA
                BigDecimal fiveDayMA = new BigDecimal("0.0");
                if (stkPrice.getFiveDayMA() == null && (i + FIVE - 1) <= (numPrices - 1)) {
                    for (int j = 0; j < FIVE; j++) {
                        StockPrice tmpPrice = priceList.get(i + j);
                        fiveDayMA = fiveDayMA.add(tmpPrice.getPrice());
                    }
                    fiveDayMA = fiveDayMA.divide(new BigDecimal(FIVE), 2, RoundingMode.HALF_UP);
                    isUpdated = true;
                } 
                else {
                    fiveDayMA = stkPrice.getFiveDayMA();
                }

                //Twenty Day MA
                BigDecimal twentyDayMA = new BigDecimal("0.0");
                if (stkPrice.getTwentyDayMA() == null && (i + TWENTY - 1) <= (numPrices - 1)) {
                    for (int j = 0; j < TWENTY; j++) {
                        StockPrice tmpPrice = priceList.get(i + j);
                        twentyDayMA = twentyDayMA.add(tmpPrice.getPrice());
                    }
                    twentyDayMA = twentyDayMA.divide(new BigDecimal(TWENTY), 2, RoundingMode.HALF_UP);
                    isUpdated = true;
                } 
                else {
                    twentyDayMA = stkPrice.getTwentyDayMA();
                }

                //Sixty Day MA
                BigDecimal sixtyDayMA = new BigDecimal("0.0");
                if (stkPrice.getSixtyDayMA() == null && (i + SIXTY - 1) <= (numPrices - 1)) {
                    for (int j = 0; j < SIXTY; j++) {
                        StockPrice tmpPrice = priceList.get(i + j);
                        sixtyDayMA = sixtyDayMA.add(tmpPrice.getPrice());
                    }
                    sixtyDayMA = sixtyDayMA.divide(new BigDecimal(SIXTY), 2, RoundingMode.HALF_UP);
                    isUpdated = true;
                } 
                else {
                    sixtyDayMA = stkPrice.getSixtyDayMA();
                }

                //Five Day Volume MA
                BigDecimal fiveDayVolMA = new BigDecimal("0.0");
                if (stkPrice.getFiveDayVolMA() == null && (i + FIVE - 1) <= (numPrices - 1)) {
                    for (int j = 0; j < FIVE; j++) {
                        StockPrice tmpPrice = priceList.get(i + j);
                        fiveDayVolMA = fiveDayVolMA.add(tmpPrice.getVolume());
                    }
                    fiveDayVolMA = fiveDayVolMA.divide(new BigDecimal(FIVE), 2, RoundingMode.HALF_UP);
                    isUpdated = true;
                } 
                else {
                    fiveDayVolMA = stkPrice.getFiveDayVolMA();
                }

                //Twenty Day Volume MA
                BigDecimal twentyDayVolMA = new BigDecimal("0.0");
                if (stkPrice.getTwentyDayVolMA() == null && (i + TWENTY - 1) <= (numPrices - 1)) {
                    for (int j = 0; j < TWENTY; j++) {
                        StockPrice tmpPrice = priceList.get(i + j);
                        twentyDayVolMA = twentyDayVolMA.add(tmpPrice.getVolume());
                    }
                    twentyDayVolMA = twentyDayVolMA.divide(new BigDecimal(TWENTY), 2, RoundingMode.HALF_UP);
                    isUpdated = true;
                } 
                else {
                    twentyDayVolMA = stkPrice.getTwentyDayVolMA();
                }

                //Sixty Day MA
                BigDecimal sixtyDayVolMA = new BigDecimal("0.0");
                if (stkPrice.getSixtyDayVolMA() == null && (i + SIXTY - 1) <= (numPrices - 1)) {
                    for (int j = 0; j < SIXTY; j++) {
                        StockPrice tmpPrice = priceList.get(i + j);
                        sixtyDayVolMA = sixtyDayVolMA.add(tmpPrice.getVolume());
                    }
                    sixtyDayVolMA = sixtyDayVolMA.divide(new BigDecimal(SIXTY), 2, RoundingMode.HALF_UP);
                    isUpdated = true;
                } 
                else {
                    sixtyDayVolMA = stkPrice.getSixtyDayVolMA();
                }
                
                //Save MAs to list
                if (isUpdated) {
                    MovingAverage avg = new MovingAverage(stockTicker.getTicker(), stkPrice.getDate(), fiveDayMA, twentyDayMA, sixtyDayMA, fiveDayVolMA, twentyDayVolMA, sixtyDayVolMA);
                    listMAs.add(avg);
                }
                
            } //End of PriceList loop

            //Save MAs to DB
            setMovingAverages(listMAs);
            System.gc();
        } //End of ticker loop
    }

    public void computeStockQuoteSlopes() throws Exception {
        
        logger.Log("StockDataHandler", "computeStockQuoteSlopes", "", "", false);
        
        List<StockTicker> tickers = getAllStockTickers(); 
        
        //Iterate through all stock tickers
        for (StockTicker stockTicker : tickers) {
            
            //Iterate through the 5 Day Moving Averages
            List<StockPrice> priceList = getAllMAs(stockTicker.getTicker());
            List<StockQuoteSlope> slopeList = new ArrayList<>();
            
            final int FIVE = 5;
            final int TWENTY = 20;
            final int SIXTY = 60;

            logger.Log("StockDataHandler", "computeStockQuoteSlopes", "Ticker: " + stockTicker.getTicker(), "", false);

            //Loop through the prices
            int numPrices = priceList.size();
            for (int i = 0; i < numPrices; i++) {

                StockPrice stkPrice = priceList.get(i);
                BigDecimal curMA = stkPrice.getFiveDayMA();

                //5 Day Slope
                if (stkPrice.getFiveDaySlope() == null && (i + FIVE) <= (numPrices - 1)) {

                    BigDecimal fiveDayDelta = priceList.get(i + FIVE).getFiveDayMA();
                    
                    BigDecimal slope = curMA.add(fiveDayDelta.negate()).divide(new BigDecimal(FIVE), 5, RoundingMode.HALF_UP);
                    StockQuoteSlope sqSlope = new StockQuoteSlope(stockTicker.getTicker(), priceList.get(i).getDate(), FIVE, slope);
                    slopeList.add(sqSlope);
                }

                //20 Day Slope
                if (stkPrice.getTwentyDaySlope() == null && (i + TWENTY) <= (numPrices - 1)) {

                    BigDecimal twentyDayDelta = priceList.get(i + TWENTY).getFiveDayMA();
                    
                    BigDecimal slope = curMA.add(twentyDayDelta.negate()).divide(new BigDecimal(TWENTY), 5, RoundingMode.HALF_UP);
                    StockQuoteSlope sqSlope = new StockQuoteSlope(stockTicker.getTicker(), priceList.get(i).getDate(), TWENTY, slope);
                    slopeList.add(sqSlope);
                }

                //60 Day Slope
                if (stkPrice.getSixtyDaySlope() == null && (i + SIXTY) <= (numPrices - 1)) {

                    BigDecimal sixtyDayDelta = priceList.get(i + SIXTY).getFiveDayMA();
                    
                    BigDecimal slope = curMA.add(sixtyDayDelta.negate()).divide(new BigDecimal(SIXTY), 5, RoundingMode.HALF_UP);
                    StockQuoteSlope sqSlope = new StockQuoteSlope(stockTicker.getTicker(), priceList.get(i).getDate(), SIXTY, slope);
                    slopeList.add(sqSlope);
                }

            } //End of price loop
            
            //Send data to DB
            setStockQuoteSlope(slopeList);
            System.gc();
            
        } //End of ticker loop
    }

    private void setStockQuoteSlope(List<StockQuoteSlope> slopeList) throws Exception {

        logger.Log("StockDataHandler", "setStockQuoteSlope", "", "", false);

        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Update_StockQuote_Slope(?, ?, ?, ?)}")) {

            conxn.setAutoCommit(false);
            
            for (StockQuoteSlope s : slopeList) {
                java.sql.Date sqlDate = new java.sql.Date(s.getDate().getTime());
            
                stmt.setString(1, s.getTicker());
                stmt.setDate(2, sqlDate);
                stmt.setInt(3, s.getDays());
                stmt.setBigDecimal(4, s.getSlope());

                stmt.executeUpdate();
            }

            //Send data to DB
            stmt.executeBatch();
            conxn.commit();

        } catch(Exception exc) {
            logger.Log("StockDataHandler", "setStockQuoteSlope", "Exception", exc.toString(), true);
            throw exc;
        }
    }
    
    public int getStockOrderID() throws Exception {
        
        logger.Log("StockDataHandler", "getStockOrderID", "", "", false);

        int orderID = -1;
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Retrieve_StockOrderID () }")) {
            
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                orderID = rs.getInt(1);
            }
            else {
                throw new Exception("Order ID wasn't returned from SP");
            }
            
        } catch (Exception exc) {
            logger.Log("StockDataHandler", "getStockOrderID", "Exception", exc.toString(), true);
            throw exc;
        }
        
        return orderID;
    }
    
    public Date getLastStockSaleDate() throws Exception {
        
        logger.Log("StockDataHandler", "getLastStockSaleDate", "", "", false);

        Date lastSale = null;
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Retrieve_Stock_Holding_Last_SaleDate () }")) {
            
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                lastSale = rs.getDate(1);
            }
            
        } catch (Exception exc) {
            logger.Log("StockDataHandler", "getLastStockSaleDate", "Exception", exc.toString(), true);
            throw exc;
        }
        
        return lastSale;
    }
    
    public List<StockHolding> getCurrentStockHoldings() throws Exception {

        logger.Log("StockDataHandler", "getCurrentStockHoldings", "", "", false);

        List<StockHolding> listHoldings = new ArrayList<>();
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Retrieve_Current_Stock_Holdings () }")) {
            
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                String ticker = rs.getString(1);
                int numShares = rs.getInt(2);
                Date expDt = rs.getDate(3);
                StockHolding holding = new StockHolding(ticker, numShares, expDt);
                listHoldings.add(holding);
            }
            
        } catch (Exception exc) {
            logger.Log("StockDataHandler", "getCurrentStockHoldings", "Exception", exc.toString(), true);
            throw exc;
        }
        
        return listHoldings;
    }
    
    public void insertStockPredictions(List<PredictionValues> predictionList) throws Exception {

        logger.Log("StockDataHandler", "insertStockPrediction", "", "", false);

        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Insert_StockPrediction(?, ?, ?, ?, ?, ?)}")) {

            conxn.setAutoCommit(false);
            
            Map<Date, Date> map = new HashMap<>();
            for (PredictionValues p : predictionList) {

                java.sql.Date dt = new java.sql.Date(p.getDate().getTime());
                java.sql.Date projDt = new java.sql.Date(p.getProjectedDate().getTime());
                
                //Dedup Check
                if (map.containsKey(dt)) 
                    throw new Exception("Method: insertStockPredictions, Duplicate dates found!");
                else
                    map.put(dt, dt);
                
                //Write values to DB
                stmt.setString(1, p.getTicker());
                stmt.setDate(2, dt);
                stmt.setDate(3, projDt);
                stmt.setString(4, p.getModelType());
                stmt.setString(5, p.getPredType());
                stmt.setBigDecimal(6, p.getEstimatedValue());

                stmt.addBatch();
            }
            
            stmt.executeBatch();
            conxn.commit();

        } catch (Exception exc) {
            logger.Log("StockDataHandler", "insertStockPrediction", "Exception", exc.toString(), true);
            throw exc;
        }

    }
    
    private void setMovingAverages(List<MovingAverage> listMAs) throws Exception {

        logger.Log("StockDataHandler", "setMovingAverages", "", "", false);

        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Update_StockQuote(?, ?, ?, ?, ?, ?, ?, ?)}")) {

            conxn.setAutoCommit(false);
            
            for (MovingAverage ma : listMAs) {

                stmt.setString(1, ma.getStockTicker());

                java.sql.Date sqlDate = new java.sql.Date(ma.getDate().getTime());
                stmt.setDate(2, sqlDate);
                stmt.setBigDecimal(3, ma.getFiveDayMA());
                stmt.setBigDecimal(4, ma.getTwentyDayMA());
                stmt.setBigDecimal(5, ma.getSixtyDayMA());
                stmt.setBigDecimal(6, ma.getFiveDayVolMA());
                stmt.setBigDecimal(7, ma.getTwentyDayVolMA());
                stmt.setBigDecimal(8, ma.getSixtyDayVolMA());
                stmt.addBatch();
            }
            
            stmt.executeBatch();
            conxn.commit();

        } catch(Exception exc) {
            logger.Log("StockDataHandler", "setMovingAverages", "Exception", exc.toString(), true);
            throw exc;
        }
    }

    public FuturePrice getTargetValueRegressionPredictions(String stock, Date date, final String PRED_TYPE, final String MODEL_TYPE) throws Exception {

        String summary = String.format("Stock: %s, Date: %s", stock, date.toString());
        logger.Log("StockDataHandler", "getTargetValueRegressionPredictions", summary, "", false);

        FuturePrice fp;
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Retrieve_Predictions_Regression_PctForecast(?, ?, ?, ?)}")) {

            stmt.setString(1, stock);
            
            java.sql.Date dt = new java.sql.Date(date.getTime());
            stmt.setDate(2, dt);

            stmt.setString(3, PRED_TYPE);
            stmt.setString(4, MODEL_TYPE);
            
            ResultSet rs = stmt.executeQuery();
            
            BigDecimal curPrice;
            double forecastPctChg = 0.0;
            Date projectedDt;
            if (rs.next()) {
                curPrice = rs.getBigDecimal(1);
                forecastPctChg = rs.getDouble(2);
                projectedDt = rs.getDate(3);
                
                fp = new FuturePrice(stock, forecastPctChg, curPrice, projectedDt);
            }
            else
                throw new Exception("Method: getTargetValueRegressionPredictions, Description: No data returned!");
                
        } catch(Exception exc) {
            logger.Log("StockDataHandler", "getTargetValueRegressionPredictions", "Exception", exc.toString(), true);
            throw exc;
        }
        
        return fp;
    }
    
    public List<String> getPostiveClassificationPredictions(Date date, final String PRED_TYPE) throws Exception {

        String summary = String.format("Date: %s", date.toString());
        logger.Log("StockDataHandler", "getPositiveClassificationPredictions", summary, "", false);

        List<String> stockList = new ArrayList<>();

        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Retrieve_Predictions_Classification_Upward (?, ?)}")) {

            java.sql.Date dt = new java.sql.Date(date.getTime());
            stmt.setDate(1, dt);

            stmt.setString(2, PRED_TYPE);
            
            ResultSet rs = stmt.executeQuery();
            
            String stock;
            while(rs.next()) {
                stock = rs.getString(1);
                stockList.add(stock);
            }
                
        } catch(Exception exc) {
            logger.Log("StockDataHandler", "getPositiveClassificationPredictions", "Exception", exc.toString(), true);
            throw exc;
        }
        
        return stockList;
    }
    
    /*Method: getAllStockQuotes
     *Description: return prices in descending order 
     */
    private List<StockPrice> getAllStockQuotes(String ticker) throws Exception {

        String summary = String.format("Ticker: %s", ticker);
        logger.Log("StockDataHandler", "getAllStockQuotes", summary, "", false);
        
        List<StockPrice> stockPrices = new ArrayList<>();
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_RetrieveAll_StockQuotes(?)}")) {
            
            stmt.setString(1, ticker);

            ResultSet rs = stmt.executeQuery();
            
            StockPrice price;
            while(rs.next()) {
                price = new StockPrice(rs.getDate(1), rs.getBigDecimal(2), rs.getBigDecimal(3), rs.getBigDecimal(4), rs.getBigDecimal(5), 
                        rs.getBigDecimal(6), rs.getBigDecimal(7), rs.getBigDecimal(8), rs.getBigDecimal(9));
                stockPrices.add(price);
            }
                
        } catch(Exception exc) {
            logger.Log("StockDataHandler", "getAllStockQuotes", "Exception", exc.toString(), true);
            throw exc;
        }
        
        return stockPrices;
    }

    public List<PredictionValues> getStockBackTesting(String ticker, String modelType, Date fromDate, Date toDate) throws Exception {

        String summary = String.format("Ticker: %s, Model Type: %s, From: %s, To: %s", ticker, modelType, fromDate, toDate);
        logger.Log("StockDataHandler", "getStockBackTesting", summary, "", false);

        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Retrieve_Stock_BackTesting(?, ?, ?, ?)}")) {
            
            stmt.setString(1, ticker);
            stmt.setString(2, modelType);
            
            java.sql.Date fromDt = new java.sql.Date(fromDate.getTime());
            stmt.setDate(3, fromDt);

            java.sql.Date toDt = new java.sql.Date(toDate.getTime());
            stmt.setDate(4, toDt);
            
            ResultSet rs = stmt.executeQuery();
            
            List<PredictionValues> listVals = new ArrayList<>();
            PredictionValues val;
            //pred.pk_DateID, pred.pk_ProjectedDateID, pred.EstimatedValue, curQuotes.Close, futureQuotes.Close, curQuotes.Open
            while(rs.next()) {
                val = new PredictionValues(ticker, rs.getDate(1), rs.getDate(2), modelType, rs.getBigDecimal(3), rs.getBigDecimal(4), rs.getBigDecimal(5), rs.getBigDecimal(6)); 
                listVals.add(val);
            }
                
            return listVals;
            
        } catch(Exception exc) {
            logger.Log("StockDataHandler", "getStockBackTesting", "Exception", exc.toString(), true);
            throw exc;
        }
    }

    public List<Weight> getWeights(String ticker, ModelTypes modelType) throws Exception {

        String summary = String.format("Ticker: %s, Model Type: %s", ticker, modelType);
        logger.Log("StockDataHandler", "getWeights", summary, "", false);
        
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Retrieve_Weights(?, ?)}")) {
            
            stmt.setString(1, ticker);
            stmt.setString(2, modelType.toString());
            
            ResultSet rs = stmt.executeQuery();
            List<Weight> listWeights = new ArrayList<>();
            Weight w;
            while (rs.next()) {
                w = new Weight(rs.getInt(1), rs.getBigDecimal(2), rs.getBigDecimal(3), rs.getBigDecimal(4));
                listWeights.add(w);
            }
            
            return listWeights;
        } catch(Exception exc) {
            logger.Log("StockDataHandler", "getWeights", "Exception", exc.toString(), true);
            throw exc;
        }
    }
    
    /*Method: getAll5DayMAs
     *Description: Return 5 Day MAs in DESC order 
     */
    private List<StockPrice> getAllMAs(String ticker) throws Exception {

        String summary = String.format("Ticker: %s", ticker);
        logger.Log("StockDataHandler", "getAllMAs", summary, "", false);

        List<StockPrice> stockPrices = new ArrayList<>();
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_RetrieveAll_MovingAvgs(?)}")) {
            
            stmt.setString(1, ticker);

            ResultSet rs = stmt.executeQuery();
            
            StockPrice price;
            while(rs.next()) {
                price = new StockPrice(rs.getDate(1), rs.getBigDecimal(2), rs.getBigDecimal(3), rs.getBigDecimal(4), rs.getBigDecimal(5));
                stockPrices.add(price);
            }
                
        } catch(Exception exc) {
            logger.Log("StockDataHandler", "getAll5DayMAs", "Exception", exc.toString(), true);
            throw exc;
        }
        
        return stockPrices;
    }
    
    public String getAllStockFeaturesFromDB(String stockTicker, int daysInFuture, ModelTypes approach, Date fromDt, Date toDt, boolean saveToFile) throws Exception {

        String strFromDt;
        if (fromDt == null)
            strFromDt = "null";
        else
            strFromDt = fromDt.toString();
        
        String strToDt;
        if (toDt == null)
            strToDt = "null";
        else
            strToDt = toDt.toString();
        
        String summary = String.format("Ticker: %s, Days In Future: %d, Model: %s, From: %s, To: %s", stockTicker, daysInFuture, approach, strFromDt, strToDt);
        logger.Log("StockDataHandler", "getAllStockFeaturesFromDB", summary, "", false);

        StringBuilder dataExamples = new StringBuilder();
        try (Connection conxn = getDBConnection()) {

            CallableStatement stmt = null;
            switch(approach) {
                case LINEAR_REG:
                case M5P:
                    stmt = conxn.prepareCall("{call sp_Retrieve_CompleteFeatureSetForStockTicker_ProjectedValue(?, ?, ?, ?)}");
                    break;
                case LOGIST_REG:
                case SVM:
                case RAND_FORST:
                    stmt = conxn.prepareCall("{call sp_Retrieve_CompleteFeatureSetForStockTicker_Classification(?, ?, ?, ?)}");
                    break;
            }
            
            stmt.setString(1, stockTicker);
            stmt.setInt(2, daysInFuture);
            
            if (fromDt == null)
                stmt.setNull(3, java.sql.Types.DATE);
            else {
                java.sql.Date fromDate = new java.sql.Date(fromDt.getTime());
                stmt.setDate(3, fromDate);
            }

            if (toDt == null)
                stmt.setNull(4, java.sql.Types.DATE);
            else {
                java.sql.Date toDate = new java.sql.Date(toDt.getTime());
                stmt.setDate(4, toDate);
            }

            ResultSet rs = stmt.executeQuery();
            
            ResultSetMetaData metaData = rs.getMetaData();
            int colCount = metaData.getColumnCount();

            dataExamples.append("@RELATION stock-data \n");
            
            //Output column labels
            for (int i = 1; i <= colCount; i++) {
                String s = "";
                if (i < colCount) {
                    s = "@ATTRIBUTE " + rs.getMetaData().getColumnLabel(i) + " NUMERIC \n";
                }
                else if (i == colCount) { //Target Val

                    if (approach == ModelTypes.LOGIST_REG || approach == ModelTypes.RAND_FORST)
                        s = "@ATTRIBUTE upDay {0.0, 1.0} \n";
                    else if (approach == ModelTypes.LINEAR_REG || approach == ModelTypes.M5P)
                        s = "@ATTRIBUTE " + rs.getMetaData().getColumnLabel(i) + " NUMERIC \n";
                }

                dataExamples.append(s);
            }
            
            //Output column values
            dataExamples.append("@DATA \n");
            int recordCount = 0;
            String s;
            StringBuffer row;
            
            while(rs.next()) {
                
                row = new StringBuffer();
                for (int i = 1; i <= colCount; i++) {
                    double d = rs.getDouble(i);
                    
                    if (i < colCount)
                        s = String.valueOf(d) + ",";
                    else
                        s = String.valueOf(d);
                    
                    row.append(s);
                }
                dataExamples.append(row);
                dataExamples.append("\n");
                recordCount++;
            }
            
            //Ensure that records were returned
            if (recordCount == 0) {
                String excOutput = String.format("Ticker: %s, No records were returned from the DB!", stockTicker);
                throw new Exception(excOutput);
            }
            
        } catch(Exception exc) {
            logger.Log("StockDataHandler", "getAllStockFeaturesFromDB", "Exception", exc.toString(), true);
            throw exc;
        }
        
        //Save to file if needed
        if (saveToFile) {
            String str = MODEL_DATA_OUTPUT_PATH + "/" + stockTicker + "-" + approach + ".arff";
            Path p = Paths.get(str);
            
            try (BufferedWriter bw = Files.newBufferedWriter(p, Charset.defaultCharset(), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
               bw.write(dataExamples.toString());
            }
        }
        
        return dataExamples.toString();
    }
    
    private void insertStockIndexDataIntoDB(String stockIndex, String indexPrices) throws Exception {

        String summary = String.format("Index: %s", stockIndex);
        logger.Log("StockDataHandler", "insertStockIndexDataIntoDB", summary, "", false);
        
        String[] rows = indexPrices.split("\n");

        String row;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date dt;
        java.sql.Date sqlDt;
        BigDecimal openPrice, highPrice, lowPrice, settlePrice, adjClosePrice, volume;

        int i = 0;
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Insert_StockIndexPrices (?, ?, ?, ?, ?, ?, ?, ?)}")) {

            conxn.setAutoCommit(false);
            
            for (i = 0; i < rows.length; i++) {
                if (i == 0) //Skip the header row
                    continue;

                //Parse the record
                try {
                    row = rows[i];
                    String[] cells = row.split(",");
                    
                    dt = sdf.parse(cells[0]);
                    sqlDt = new java.sql.Date(dt.getTime());

                    if (cells[1].length() > 0)
                        openPrice = new BigDecimal(cells[1]);
                    else
                        openPrice = new BigDecimal(0.0);
                    
                    if (cells[2].length() > 0)
                        highPrice = new BigDecimal(cells[2]);
                    else
                        highPrice = new BigDecimal(0.0);
                    
                    lowPrice = new BigDecimal(cells[3]);
                    settlePrice = new BigDecimal(cells[4]);

                    //Parse NIKEII differently
                    if (stockIndex.equals("NIKEII")) {
                        volume = new BigDecimal(0.0);
                        adjClosePrice = new BigDecimal(0.0);
                        
                    } else {
                        volume = new BigDecimal(cells[5]);
                        adjClosePrice = new BigDecimal(cells[6]);
                    }

                    //Insert the record into the DB
                    stmt.setDate(1, sqlDt);
                    stmt.setString(2, stockIndex);
                    stmt.setBigDecimal(3, openPrice);
                    stmt.setBigDecimal(4, highPrice);
                    stmt.setBigDecimal(5, lowPrice);
                    stmt.setBigDecimal(6, settlePrice);
                    stmt.setBigDecimal(7, volume);
                    stmt.setBigDecimal(8, adjClosePrice);
                    stmt.addBatch();
                    
                } catch(Exception exc) {
                    logger.Log("StockDataHandler", "insertStockIndexDataIntoDB", "Exception", exc.toString(), true);
                }
            } //End For
            
            //Execute DB commands
            stmt.executeBatch();
            conxn.commit();
            
        } catch(Exception exc) {
            logger.Log("StockDataHandler", "insertStockIndexDataIntoDB", "Exception", exc.toString(), true);
            throw exc;
        }
    }

    private void insertEnergyPricesIntoDB(String energyCode, String energyPrices) throws Exception {
        
        String summary = String.format("Code: %s", energyCode);
        logger.Log("StockDataHandler", "insertEnergyPricesIntoDB", summary, "", false);
        
        String[] rows = energyPrices.split("\n");

        String row;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date dt;
        java.sql.Date sqlDt;
        BigDecimal openPrice, highPrice, lowPrice, settlePrice;
        int volume, openInterest;

        int i = 0;
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Insert_EnergyPrices (?, ?, ?, ?, ?, ?, ?, ?)}")) {
            
            conxn.setAutoCommit(false);
            
            for (i = 0; i < rows.length; i++) {
                if (i == 0) //Skip the header row
                    continue;

                //Parse the record
                try {
                    row = rows[i];
                    String[] cells = row.split(",");

                    dt = sdf.parse(cells[0]);
                    sqlDt = new java.sql.Date(dt.getTime());
                    
                    openPrice = new BigDecimal(cells[1]);
                    highPrice = new BigDecimal(cells[2]);
                    lowPrice = new BigDecimal(cells[3]);
                    settlePrice = new BigDecimal(cells[4]);
                    volume = new BigDecimal(cells[5]).intValue();
                    openInterest = new BigDecimal(cells[6]).intValue();

                    //Insert the record into the DB
                    stmt.setDate(1, sqlDt);
                    stmt.setString(2, energyCode);
                    stmt.setBigDecimal(3, openPrice);
                    stmt.setBigDecimal(4, highPrice);
                    stmt.setBigDecimal(5, lowPrice);
                    stmt.setBigDecimal(6, settlePrice);
                    stmt.setInt(7, volume);
                    stmt.setInt(8, openInterest);
                    stmt.addBatch();
                    
                } catch(Exception exc) {
                    logger.Log("StockDataHandler", "insertEnergyPricesIntoDB", "Exception", exc.toString(), true);
                }
            } //End For
            
            //Send commands to DB
            stmt.executeBatch();
            conxn.commit();
            
        } catch(Exception exc) {
            logger.Log("StockDataHandler", "insertEnergyPricesIntoDB", "Exception", exc.toString(), true);
            throw exc;
        }
    }

    public void updateStockTrade(String ticker) throws Exception {

        logger.Log("StockDataHandler", "updateStockTrade", "", "", false);

        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Update_Stock_Trade (?) }")) {

            stmt.setString(1, ticker);

            stmt.executeUpdate();
            
        } catch(Exception exc) {
            logger.Log("StockDataHandler", "updateStockTrade", "Exception", exc.toString(), true);
            throw exc;
        }
    }
    
    public void insertStockTrades(String ticker, int numShares, Date purchaseDate, Date expirationDate) throws Exception {

        logger.Log("StockDataHandler", "insertStockTrades", "", "", false);

        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Insert_Stock_Trade (?, ?, ?, ?)}")) {

            stmt.setString(1, ticker);
            stmt.setInt(2, numShares);
            
            java.sql.Date purchDt = new java.sql.Date(purchaseDate.getTime());
            stmt.setDate(3, purchDt);
            
            java.sql.Date expDt = new java.sql.Date(expirationDate.getTime());
            stmt.setDate(4, expDt);

            stmt.executeUpdate();
            
        } catch(Exception exc) {
            logger.Log("StockDataHandler", "insertStockTrades", "Exception", exc.toString(), true);
            throw exc;
        }
    }
    
    private void insertCurrencyRatiosIntoDB(String currency, String currencyRatios) throws Exception {

        String summary = String.format("Currency: %s", currency);
        logger.Log("StockDataHandler", "insertCurrencyRatiosIntoDB", summary, "", false);

        String[] rows = currencyRatios.split("\n");

        String row;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date dt;
        java.sql.Date sqlDt;
        BigDecimal ratio;

        int i = 0;
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Insert_CurrencyRatios (?, ?, ?)}")) {
            
            conxn.setAutoCommit(false);
            
            for (i = 0; i < rows.length; i++) {
                if (i == 0) //Skip the header row
                    continue;

                //Parse the record
                try {
                    row = rows[i];
                    String[] cells = row.split(",");

                    dt = sdf.parse(cells[0]);
                    sqlDt = new java.sql.Date(dt.getTime());
                    
                    ratio = new BigDecimal(cells[1]);

                    //Insert the record into the DB
                    stmt.setDate(1, sqlDt);
                    stmt.setString(2, currency);
                    stmt.setBigDecimal(3, ratio);
                    stmt.addBatch();
                    
                } catch(Exception exc) {
                    logger.Log("StockDataHandler", "insertCurrencyRatiosIntoDB", "Exception", exc.toString(), true);
                }
            } //End for
            
            //Send commands to DB
            stmt.executeBatch();
            conxn.commit();

        } catch(Exception exc) {
            logger.Log("StockDataHandler", "insertCurrencyRatiosIntoDB", "Exception", exc.toString(), true);
            throw exc;
        }
    }

    public void setStockBacktestingIntoDB(List<BacktestingResults> listResults) throws Exception {

        logger.Log("StockDataHandler", "setStockBacktestingIntoDB", "", "", false);

        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Insert_BackTesting (?, ?, ?, ?, ?)}")) {
            
            conxn.setAutoCommit(false);
            
            for (BacktestingResults r : listResults) {
                java.sql.Date startDt = new java.sql.Date(r.getStartDt().getTime());
                java.sql.Date endDt = new java.sql.Date(r.getEndDt().getTime());
                
                stmt.setDate(1, startDt);
                stmt.setDate(2, endDt);
                stmt.setBigDecimal(3, r.getAssetValue());
                stmt.setBigDecimal(4, r.getPctChg());
                stmt.setBigDecimal(5, r.getSp500PctChg());
                
                stmt.addBatch();
            }
            
            //Send commands to DB
            stmt.executeBatch();
            conxn.commit();

        } catch(Exception exc) {
            logger.Log("StockDataHandler", "setStockBacktestingIntoDB", "Exception", exc.toString(), true);
            throw exc;
        }
    }

    
    private void insertPreciousMetalsPricesIntoDB(String metal, String goldPrices) throws Exception {

        String summary = String.format("Metal: %s", metal);
        logger.Log("StockDataHandler", "insertPreciousMetalsPricesIntoDB", summary, "", false);
        
        String[] rows = goldPrices.split("\n");

        String row;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date dt;
        java.sql.Date sqlDt;
        BigDecimal price;

        int i = 0;
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Insert_PreciousMetalsPrices (?, ?, ?)}")) {

            conxn.setAutoCommit(false);
            
            for (i = 0; i < rows.length; i++) {
                if (i == 0) //Skip the header row
                    continue;

                //Parse the record
                try {
                    row = rows[i];
                    String[] cells = row.split(",");

                    dt = sdf.parse(cells[0]);
                    sqlDt = new java.sql.Date(dt.getTime());
                    
                    price = new BigDecimal(cells[1]);

                    //Insert the record into the DB
                    stmt.setDate(1, sqlDt);
                    stmt.setString(2, metal);
                    stmt.setBigDecimal(3, price);
                    stmt.addBatch();
                    
                } catch(Exception exc) {
                    logger.Log("StockDataHandler", "insertPreciousMetalsPricesIntoDB", "Exception", exc.toString(), true);
                }
            } //End For
            
            //Send Commands to DB
            stmt.executeBatch();
            conxn.commit();

        } catch(Exception exc) {
            logger.Log("StockDataHandler", "insertPreciousMetalsPricesIntoDB", "Exception", exc.toString(), true);
            throw exc;
        }
    }

    private void removeAllBadData() throws Exception {

        logger.Log("StockDataHandler", "removeAllBadData", "", "", false);

        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_RemoveAll_BadData()}")) {
            
            stmt.executeUpdate();
        }
    }
    
    private void insertInflationDataIntoDB(String cpiInflation) throws Exception {

        logger.Log("StockDataHandler", "insertInflationDataIntoDB", "", "", false);

        String[] rows = cpiInflation.split("\n");

        String row;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date dt;
        java.sql.Date sqlDt;
        BigDecimal rate;

        int i = 0;
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Insert_CPI (?, ?)}")) {

            conxn.setAutoCommit(false);
            
            for (i = 0; i < rows.length; i++) {
                if (i == 0) //Skip the header row
                    continue;

                //Parse the record
                try {
                    row = rows[i];
                    String[] cells = row.split(",");

                    dt = sdf.parse(cells[0]);
                    Calendar c = Calendar.getInstance();
                    c.setTime(dt);

                    //Increment one month
                    c.set(Calendar.DAY_OF_MONTH, 1);
                    c.add(Calendar.MONTH, 1);
                    sqlDt = new java.sql.Date(c.getTimeInMillis());
                    
                    rate = new BigDecimal(cells[1]);

                    //Insert the record into the DB
                    stmt.setDate(1, sqlDt);
                    stmt.setBigDecimal(2, rate);
                    stmt.addBatch();
                    
                } catch(Exception exc) {
                    logger.Log("StockDataHandler", "insertInflationDataIntoDB", "Exception", exc.toString(), true);
                }
            } //End For
            
            //Send Command to DB
            stmt.executeBatch();
            conxn.commit();
            
        } catch(Exception exc) {
            logger.Log("StockDataHandler", "insertInflationDataIntoDB", "Exception", exc.toString(), true);
            throw exc;
        }
    }

    private void insertGDPDataIntoDB(List<BEA_Data> listData) throws Exception {

        logger.Log("StockDataHandler", "insertGDPDataIntoDB", "", "", false);

        try (Connection conxn = getDBConnection();
            CallableStatement stmt = conxn.prepareCall("{call sp_Insert_BEA_Data (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)}")) {

            conxn.setAutoCommit(false);

            for (BEA_Data e : listData) {
                
                int year = e.getYear();
                int quarter = e.getQuarter();

                //Move ahead one quarter
                if (quarter == 4) {
                    year++;
                    quarter = 1;
                }
                else {
                    quarter++;
                }
                
                //Minimal Date
                if (year < 1990)
                    continue;

                //Insert values into DB
                stmt.setShort(1, (short)year);
                stmt.setByte(2, (byte)quarter);
                stmt.setBigDecimal(3, e.getGrossPrivDomInv());
                stmt.setBigDecimal(4, e.getFixInvestment());
                stmt.setBigDecimal(5, e.getNonResidential());
                stmt.setBigDecimal(6, e.getResidential());
                stmt.setBigDecimal(7, e.getGDP());
                stmt.setBigDecimal(8, e.getGoods1());
                stmt.setBigDecimal(9, e.getGoods2());
                stmt.setBigDecimal(10, e.getGoods3());
                stmt.setBigDecimal(11, e.getServices1());
                stmt.setBigDecimal(12, e.getServices2());
                stmt.setBigDecimal(13, e.getServices3());
                stmt.setBigDecimal(14, e.getGovConsExpAndGrossInv());
                stmt.setBigDecimal(15, e.getFederal());
                stmt.setBigDecimal(16, e.getNatDefense());
                stmt.setBigDecimal(17, e.getNonDefense());
                stmt.setBigDecimal(18, e.getStateAndLocal());
                stmt.setBigDecimal(19, e.getStructures());
                stmt.setBigDecimal(20, e.getExports());
                stmt.setBigDecimal(21, e.getImports());
                stmt.setBigDecimal(22, e.getDurableGoods());
                stmt.setBigDecimal(23, e.getNonDurGoods());
                stmt.setBigDecimal(24, e.getPersConsExp());
                stmt.setBigDecimal(25, e.getIntPropProducts());
                stmt.setBigDecimal(26, e.getEquipment());

                stmt.addBatch();
            }

            //Send Commands to DB
            stmt.executeBatch();
            conxn.commit();
            
        } catch(Exception exc) {
            logger.Log("StockDataHandler", "insertGDPDataIntoDB", "Exception", exc.toString(), true);
            throw exc;
        }
    }
    
    private void insertUnemploymentRatesIntoDB(String unemploymentRates) throws Exception {

        logger.Log("StockDataHandler", "insertUnemploymentRatesIntoDB", "", "", false);

        String[] rows = unemploymentRates.split("\n");

        String row;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date dt;
        java.sql.Date sqlDt;
        BigDecimal rate;

        int i = 0;
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Insert_UnemploymentRates (?, ?)}")) {
            
            for (i = 0; i < rows.length; i++) {
                if (i == 0) //Skip the header row
                    continue;

                //Parse the record
                try {
                    row = rows[i];
                    String[] cells = row.split(",");

                    dt = sdf.parse(cells[0]);
                    sqlDt = new java.sql.Date(dt.getTime());
                    
                    rate = new BigDecimal(cells[1]);

                    //Insert the record into the DB
                    stmt.setDate(1, sqlDt);
                    stmt.setBigDecimal(2, rate);
                    
                    stmt.executeUpdate();
                    
                } catch(Exception exc) {
                    logger.Log("StockDataHandler", "insertUnemploymentRatesIntoDB", "Exception", exc.toString(), true);
                }
            }
        } catch(Exception exc) {
            logger.Log("StockDataHandler", "insertUnemploymentRatesIntoDB", "Exception", exc.toString(), true);
            throw exc;
        }
    }
    
    private void insertInterestRatesIntoDB(final String RATE_TYPE, String primeRates) throws Exception {

        String summary = String.format("Rate Type: %s", RATE_TYPE);
        logger.Log("StockDataHandler", "insertInterestRatesIntoDB", summary, "", false);

        String[] rows = primeRates.split("\n");

        String row;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date dt;
        java.sql.Date sqlDt;
        BigDecimal rate;

        int i = 0;
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Insert_InterestRates (?, ?, ?)}")) {
            
            conxn.setAutoCommit(false);
            
            for (i = 0; i < rows.length; i++) {
                if (i == 0) //Skip the header row
                    continue;

                //Parse the record
                try {
                    row = rows[i];
                    String[] cells = row.split(",");

                    dt = sdf.parse(cells[0]);
                    sqlDt = new java.sql.Date(dt.getTime());
                    
                    rate = new BigDecimal(cells[1]);

                    //Insert the record into the DB
                    stmt.setDate(1, sqlDt);
                    stmt.setString(2, RATE_TYPE);
                    stmt.setBigDecimal(3, rate);
                    stmt.addBatch();
                    
                } catch(Exception exc) {
                    logger.Log("StockDataHandler", "insertInterestRatesIntoDB", "Exception", exc.toString(), true);
                }
            } //End For
            
            //Send Commands to DB
            stmt.executeBatch();
            conxn.commit();
            
        } catch(Exception exc) {
            logger.Log("StockDataHandler", "insertInterestRatesIntoDB", "Exception", exc.toString(), true);
            throw exc;
        }
    }
    
    private void insertEconomicDataIntoDB(String indicator, String econData) throws Exception {

        String summary = String.format("Indicator: %s", indicator);
        logger.Log("StockDataHandler", "insertEconomicDataIntoDB", "", "", false);

        String[] rows = econData.split("\n");

        String row;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date dt;
        java.sql.Date sqlDt;
        BigDecimal value;

        int i = 0;
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Insert_EconomicData (?, ?, ?)}")) {
            
            conxn.setAutoCommit(false);
            
            for (i = 0; i < rows.length; i++) {
                if (i == 0) //Skip the header row
                    continue;

                //Parse the record
                try {
                    row = rows[i];
                    String[] cells = row.split(",");

                    dt = sdf.parse(cells[0]);
                    sqlDt = new java.sql.Date(dt.getTime());
                    
                    value = new BigDecimal(cells[1]);

                    //Insert the record into the DB
                    stmt.setDate(1, sqlDt);
                    stmt.setString(2, indicator);
                    stmt.setBigDecimal(3, value);
                    stmt.addBatch();
                    
                } catch(Exception exc) {
                    logger.Log("StockDataHandler", "insertEconomicDataIntoDB", "Exception", exc.toString(), true);
                }
            } //End For
            
            //Send Commands to DB
            stmt.executeBatch();
            conxn.commit();
            
        } catch(Exception exc) {
            logger.Log("StockDataHandler", "insertEconomicDataIntoDB", "Exception", exc.toString(), true);
            throw exc;
        }
    }
    
    private void insertMortgageDataIntoDB(String thirtyYrMtgRates) throws Exception {

        logger.Log("StockDataHandler", "insertMortgageDataIntoDB", "", "", false);

        String[] rows = thirtyYrMtgRates.split("\n");

        String row;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date dt;
        java.sql.Date sqlDt;
        BigDecimal price;

        int i = 0;
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Insert_30yr_mortgagerates (?, ?)}")) {
            
            conxn.setAutoCommit(false);
            
            for (i = 0; i < rows.length; i++) {
                if (i == 0) //Skip the header row
                    continue;

                //Parse the record
                try {
                    row = rows[i];
                    String[] cells = row.split(",");

                    dt = sdf.parse(cells[0]);
                    sqlDt = new java.sql.Date(dt.getTime());
                    
                    price = new BigDecimal(cells[1]);

                    //Insert the record into the DB
                    stmt.setDate(1, sqlDt);
                    stmt.setBigDecimal(2, price);
                    stmt.addBatch();
                    
                } catch(Exception exc) {
                    logger.Log("StockDataHandler", "insertMortgageDataIntoDB", "Exception", exc.toString(), true);
                }
            } //End For
            
            //Send Commands to DB
            stmt.executeBatch();
            conxn.commit();
            
        } catch(Exception exc) {
            logger.Log("StockDataHandler", "insertMortgageDataIntoDB", "Exception", exc.toString(), true);
            throw exc;
        }
    }
    
    private void insertNewHomePriceDataIntoDB(String newHomePrices) throws Exception {

        logger.Log("StockDataHandler", "insertNewHomePriceDataIntoDB", "", "", false);

        String[] rows = newHomePrices.split("\n");

        String row;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date dt;
        java.sql.Date sqlDt;
        BigDecimal price;

        int i = 0;
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Insert_NewHomePrices (?, ?)}")) {

            conxn.setAutoCommit(false);
            
            for (i = 0; i < rows.length; i++) {
                if (i == 0) //Skip the header row
                    continue;

                //Parse the record
                try {
                    row = rows[i];
                    String[] cells = row.split(",");

                    dt = sdf.parse(cells[0]);
                    sqlDt = new java.sql.Date(dt.getTime());
                    
                    price = new BigDecimal(cells[1]);

                    //Insert the record into the DB
                    stmt.setDate(1, sqlDt);
                    stmt.setBigDecimal(2, price);
                    stmt.addBatch();
                    
                } catch(Exception exc) {
                    logger.Log("StockDataHandler", "insertNewHomePriceDataIntoDB", "Exception", exc.toString(), true);
                }
            } //End For
            
            //Send Commands to DB
            stmt.executeBatch();
            conxn.commit();
            
        } catch(Exception exc) {
            logger.Log("StockDataHandler", "insertNewHomePriceDataIntoDB", "Exception", exc.toString(), true);
            throw exc;
        }
    }
    
    private void insertStockPricesIntoDB(String stockTicker, String stockValues) throws Exception {

        String summary = String.format("Ticker: %s", stockTicker);
        logger.Log("StockDataHandler", "insertStockPricesIntoDB", summary, "", false);

        String[] rows = stockValues.split("\n");

        String row;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date dt;
        BigDecimal open, high, low, close, volume;
        java.sql.Date sqlDt;
        int i = 0;
        
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Insert_StockQuote (?, ?, ?, ?, ?, ?, ?)}")) {

            conxn.setAutoCommit(false);
            
            for (i = 0; i < rows.length; i++) {
                if (i == 0) //Skip the header row
                    continue;

                //Parse the record
                try {
                    row = rows[i];
                    String[] cells = row.split(",");

                    dt = sdf.parse(cells[0]);
                    sqlDt = new java.sql.Date(dt.getTime());

                    open = new BigDecimal(cells[8]);
                    high = new BigDecimal(cells[9]);
                    low = new BigDecimal(cells[10]);
                    close = new BigDecimal(cells[11]);
                    volume = new BigDecimal(cells[12]);

                    //Insert the record into the DB
                    stmt.setString(1, stockTicker);
                    stmt.setDate(2, sqlDt);
                    stmt.setBigDecimal(3, open);
                    stmt.setBigDecimal(4, high);
                    stmt.setBigDecimal(5, low);
                    stmt.setBigDecimal(6, close);
                    stmt.setBigDecimal(7, volume);
                    stmt.addBatch();
                    
                } catch(Exception exc) {
                    logger.Log("StockDataHandler", "insertStockPricesIntoDB", "Exception", exc.toString(), true);
                }

            } //End for
            
            stmt.executeBatch();
            conxn.commit();
            
        } catch(Exception exc) {
            logger.Log("StockDataHandler", "insertStockPricesIntoDB", "Exception", exc.toString(), true);
            throw exc;
        }
    }

    private void insertStockFundamentalsIntoDB(List<StockFundamentals_Annual> listStockFund) throws Exception {

        logger.Log("StockDataHandler", "insertStockFundamentalsIntoDB", "", "", false);

        String row;
        java.sql.Date sqlDt;
        int i = 0;
       
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Insert_StockFundamentals (?, ?, ?, ?)}")) {
            
            conxn.setAutoCommit(false);
            
            for (i = 0; i < listStockFund.size(); i++) {

                StockFundamentals_Annual fund = listStockFund.get(i);
                Date[] dates = fund.getFinancials_Dates();

                //Save Revenue
                BigDecimal[] rev = fund.getFinancials_Revenue();
                for (int j = 0; j < rev.length; j++) {

                    stmt.setString(1, fund.getTicker());
                    
                    sqlDt = new java.sql.Date(dates[j].getTime());
                    stmt.setDate(2, sqlDt);

                    stmt.setString(3, "ANNUAL-REVENUE");
                    stmt.setBigDecimal(4, rev[j]);

                    stmt.addBatch();
                }

                //Gross Margin
                BigDecimal[] grossMargin = fund.getFinancials_GrossMargin();
                for (int j = 0; j < grossMargin.length; j++) {

                    stmt.setString(1, fund.getTicker());
                    
                    sqlDt = new java.sql.Date(dates[j].getTime());
                    stmt.setDate(2, sqlDt);

                    stmt.setString(3, "ANNUAL-GROSS-MARGIN");
                    stmt.setBigDecimal(4, grossMargin[j]);

                    stmt.addBatch();
                }

                //Operating Income
                BigDecimal[] operIncome = fund.getFinancials_OperIncome();
                for (int j = 0; j < operIncome.length; j++) {

                    stmt.setString(1, fund.getTicker());
                    
                    sqlDt = new java.sql.Date(dates[j].getTime());
                    stmt.setDate(2, sqlDt);

                    stmt.setString(3, "ANNUAL-OPERATING-INCOME");
                    stmt.setBigDecimal(4, operIncome[j]);

                    stmt.addBatch();
                }
                
                //Operating Margin
                BigDecimal[] operMargin = fund.getFinancials_OperMargin();
                for (int j = 0; j < operMargin.length; j++) {

                    stmt.setString(1, fund.getTicker());
                    
                    sqlDt = new java.sql.Date(dates[j].getTime());
                    stmt.setDate(2, sqlDt);

                    stmt.setString(3, "ANNUAL-OPERATING-MARGIN");
                    stmt.setBigDecimal(4, operMargin[j]);

                    stmt.addBatch();
                }
                
                //Net Income
                BigDecimal[] netIncome = fund.getFinancials_NetIncome();
                for (int j = 0; j < netIncome.length; j++) {

                    stmt.setString(1, fund.getTicker());
                    
                    sqlDt = new java.sql.Date(dates[j].getTime());
                    stmt.setDate(2, sqlDt);

                    stmt.setString(3, "ANNUAL-NET-INCOME");
                    stmt.setBigDecimal(4, netIncome[j]);

                    stmt.addBatch();
                }
                
                //EPS
                BigDecimal[] eps = fund.getFinancials_EPS();
                for (int j = 0; j < eps.length; j++) {

                    stmt.setString(1, fund.getTicker());
                    
                    sqlDt = new java.sql.Date(dates[j].getTime());
                    stmt.setDate(2, sqlDt);

                    stmt.setString(3, "ANNUAL-EPS");
                    stmt.setBigDecimal(4, eps[j]);

                    stmt.addBatch();
                }
                
                //Dividends
                BigDecimal[] div = fund.getFinancials_Dividends();
                for (int j = 0; j < div.length; j++) {

                    stmt.setString(1, fund.getTicker());
                    
                    sqlDt = new java.sql.Date(dates[j].getTime());
                    stmt.setDate(2, sqlDt);

                    stmt.setString(3, "ANNUAL-DIVIDENDS");
                    stmt.setBigDecimal(4, div[j]);

                    stmt.addBatch();
                }
                
                //Payout Ratio
                BigDecimal[] payout = fund.getFinancials_PayoutRatio();
                for (int j = 0; j < payout.length; j++) {

                    stmt.setString(1, fund.getTicker());
                    
                    sqlDt = new java.sql.Date(dates[j].getTime());
                    stmt.setDate(2, sqlDt);

                    stmt.setString(3, "ANNUAL-PAYOUT-RATIO");
                    stmt.setBigDecimal(4, payout[j]);

                    stmt.addBatch();
                }
                
                //Num Shares
                BigDecimal[] numShares = fund.getFinancials_SharesMil();
                for (int j = 0; j < numShares.length; j++) {

                    stmt.setString(1, fund.getTicker());
                    
                    sqlDt = new java.sql.Date(dates[j].getTime());
                    stmt.setDate(2, sqlDt);

                    stmt.setString(3, "ANNUAL-NUM-SHARES");
                    stmt.setBigDecimal(4, numShares[j]);

                    stmt.addBatch();
                }
                
                //Book Value Per Share
                BigDecimal[] bookVal = fund.getFinancials_BookValPerShare();
                for (int j = 0; j < bookVal.length; j++) {

                    stmt.setString(1, fund.getTicker());
                    
                    sqlDt = new java.sql.Date(dates[j].getTime());
                    stmt.setDate(2, sqlDt);

                    stmt.setString(3, "ANNUAL-BOOK-VALUE-PER-SHARE");
                    stmt.setBigDecimal(4, bookVal[j]);

                    stmt.addBatch();
                }
                
                //Operating Cash Flow
                BigDecimal[] operCashFlow = fund.getFinancials_OperCashFlow();
                for (int j = 0; j < operCashFlow.length; j++) {

                    stmt.setString(1, fund.getTicker());
                    
                    sqlDt = new java.sql.Date(dates[j].getTime());
                    stmt.setDate(2, sqlDt);

                    stmt.setString(3, "ANNUAL-OPERATING-CASH-FLOW");
                    stmt.setBigDecimal(4, operCashFlow[j]);

                    stmt.addBatch();
                }
                
                //Capital Spending
                BigDecimal[] capSpending = fund.getFinancials_CapSpending();
                for (int j = 0; j < capSpending.length; j++) {

                    stmt.setString(1, fund.getTicker());
                    
                    sqlDt = new java.sql.Date(dates[j].getTime());
                    stmt.setDate(2, sqlDt);

                    stmt.setString(3, "ANNUAL-CAPITAL-SPENDING");
                    stmt.setBigDecimal(4, capSpending[j]);

                    stmt.addBatch();
                }
                
                //Free Cash Flow
                BigDecimal[] freeCashFlow = fund.getFinancials_FreeCashFlow();
                for (int j = 0; j < freeCashFlow.length; j++) {

                    stmt.setString(1, fund.getTicker());
                    
                    sqlDt = new java.sql.Date(dates[j].getTime());
                    stmt.setDate(2, sqlDt);

                    stmt.setString(3, "ANNUAL-FREE-CASH-FLOW");
                    stmt.setBigDecimal(4, freeCashFlow[j]);

                    stmt.addBatch();
                }
                
                //Free Cash Flow Per Share
                BigDecimal[] freeCashFlowPerShare = fund.getFinancials_FreeCashFlow();
                for (int j = 0; j < freeCashFlowPerShare.length; j++) {

                    stmt.setString(1, fund.getTicker());
                    
                    sqlDt = new java.sql.Date(dates[j].getTime());
                    stmt.setDate(2, sqlDt);

                    stmt.setString(3, "ANNUAL-FREE-CASH-FLOW-PER-SHARE");
                    stmt.setBigDecimal(4, freeCashFlowPerShare[j]);

                    stmt.addBatch();
                }

                //Working Capital
                BigDecimal[] workCap = fund.getFinancials_WorkingCap();
                for (int j = 0; j < workCap.length; j++) {

                    stmt.setString(1, fund.getTicker());
                    
                    sqlDt = new java.sql.Date(dates[j].getTime());
                    stmt.setDate(2, sqlDt);

                    stmt.setString(3, "ANNUAL-WORKING-CAPITAL");
                    stmt.setBigDecimal(4, workCap[j]);

                    stmt.addBatch();
                }
                
                //Return on Assets
                BigDecimal[] roa = fund.getFinancials_ReturnOnAssets();
                for (int j = 0; j < roa.length; j++) {

                    stmt.setString(1, fund.getTicker());
                    
                    sqlDt = new java.sql.Date(dates[j].getTime());
                    stmt.setDate(2, sqlDt);

                    stmt.setString(3, "ANNUAL-RETURN-ON-ASSETS");
                    stmt.setBigDecimal(4, roa[j]);

                    stmt.addBatch();
                }

                //Return on Equity
                BigDecimal[] roe = fund.getFinancials_ReturnOnEquity();
                for (int j = 0; j < roe.length; j++) {

                    stmt.setString(1, fund.getTicker());
                    
                    sqlDt = new java.sql.Date(dates[j].getTime());
                    stmt.setDate(2, sqlDt);

                    stmt.setString(3, "ANNUAL-RETURN-ON-EQUITY");
                    stmt.setBigDecimal(4, roe[j]);

                    stmt.addBatch();
                }
            }
            
            stmt.executeBatch();
            conxn.commit();
            
        } catch(Exception exc) {
            logger.Log("StockDataHandler", "insertStockFundamentalsIntoDB", "Exception", exc.toString(), true);
            throw exc;
        }
    }
    
    public List<StockTicker> getAllStockTickers() throws Exception {

        logger.Log("StockDataHandler", "getAllStockTickers", "", "", false);

        List<StockTicker> tickerList = new ArrayList<>();
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_RetrieveAll_StockTickers()}")) {
            
            ResultSet rs = stmt.executeQuery();
            
            String ticker;
            String quandlCode;
            String description;
            String exchange;
            
            while(rs.next()) {
                ticker = rs.getString(1);
                quandlCode = rs.getString(2);
                description = rs.getString(3);
                exchange = rs.getString(4);
                
                StockTicker st = new StockTicker(ticker, quandlCode, description, exchange);
                tickerList.add(st);
            }
            
        } catch (Exception exc) {
            logger.Log("StockDataHandler", "getAllStockTickers", "Exception", exc.toString(), true);
            throw exc;
        }
        
        return tickerList;
    }
    
    public void loadStockTickers() throws Exception {

        logger.Log("StockDataHandler", "loadStockTickers", "", "", false);

        Path p = Paths.get(STOCK_TICKERS_PATH);
        int i = 0;
        try(BufferedReader reader = Files.newBufferedReader(p, Charset.defaultCharset())) {

            String row = "";
            String[] cells;
            for(i = 0; ; i++) {
                row = reader.readLine();
                if (row == null)
                    break;
                
                if (i > 0) {
                    cells = row.split(",");
                    insertStockTickersIntoDB(cells);
                }
            }
            
        } catch (Exception exc) {
            logger.Log("StockDataHandler", "loadStockTickers", "Exception", "Row:" + i + ", " + exc.toString(), true);
        }

    }

    private void insertStockTickersIntoDB(String[] cells) throws Exception {

        logger.Log("StockDataHandler", "insertStockTickersIntoDB", "", "", false);

        if (cells.length != 3)
            throw new Exception("Method: insertStockTickersIntoDB, invalid number of paramaters");
        
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Insert_StockTicker (?, ?, ?)}")) {
            
            stmt.setString(1, cells[0]);
            stmt.setString(2, cells[1]);
            stmt.setString(3, cells[2]);

            stmt.executeUpdate();
            
        } catch (Exception exc) {
            logger.Log("StockDataHandler", "insertStockTickersIntoDB", "Exception", exc.toString(), true);
            throw exc;
        }
    }

    
    private void insertStockFundamentals_Annual_IntoDB(List<StockFundamentals_Annual> listStockFund) throws Exception {

        logger.Log("StockDataHandler", "insertStockFundamentals_Annual_IntoDB", "", "", false);

        String row;
        java.sql.Date sqlDt;
       
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Insert_Stock_Fundamentals_Annual (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)}")) {
            
            conxn.setAutoCommit(false);
            
            for (int i = 0; i < listStockFund.size(); i++) {

                StockFundamentals_Annual fund = listStockFund.get(i);

                //Loop through the dates
                Date[] dates = fund.getFinancials_Dates();
                BigDecimal[] bdArray;
                for (int j = 0; j < dates.length - 1; j++) { //Skip TTM

                    stmt.setString(1, fund.getTicker());
                    
                    sqlDt = new java.sql.Date(dates[j].getTime());
                    stmt.setDate(2, sqlDt);

                    bdArray = fund.getFinancials_BookValPerShare();
                    stmt.setBigDecimal(3, bdArray[j]);

                    bdArray = fund.getFinancials_CapSpending();
                    stmt.setBigDecimal(4, bdArray[j]);
                    
                    bdArray = fund.getFinancials_Dividends();
                    stmt.setBigDecimal(5, bdArray[j]);

                    bdArray = fund.getFinancials_EPS();
                    stmt.setBigDecimal(6, bdArray[j]);

                    bdArray = fund.getFinancials_FreeCashFlow();
                    stmt.setBigDecimal(7, bdArray[j]);

                    bdArray = fund.getFinancials_FreeCashFlowPerShare();
                    stmt.setBigDecimal(8, bdArray[j]);

                    bdArray = fund.getFinancials_GrossMargin();
                    stmt.setBigDecimal(9, bdArray[j]);
                    
                    bdArray = fund.getFinancials_NetIncome();
                    stmt.setBigDecimal(10, bdArray[j]);

                    bdArray = fund.getFinancials_SharesMil();
                    stmt.setBigDecimal(11, bdArray[j]);
                    
                    bdArray = fund.getFinancials_OperCashFlow();
                    stmt.setBigDecimal(12, bdArray[j]);

                    bdArray = fund.getFinancials_OperMargin();
                    stmt.setBigDecimal(13, bdArray[j]);

                    bdArray = fund.getFinancials_PayoutRatio();
                    stmt.setBigDecimal(14, bdArray[j]);

                    bdArray = fund.getFinancials_ReturnOnAssets();
                    stmt.setBigDecimal(15, bdArray[j]);

                    bdArray = fund.getFinancials_ReturnOnEquity();
                    stmt.setBigDecimal(16, bdArray[j]);
                                        
                    bdArray = fund.getFinancials_Revenue();
                    stmt.setBigDecimal(17, bdArray[j]);

                    bdArray = fund.getFinancials_WorkingCap();
                    stmt.setBigDecimal(18, bdArray[j]);
                    
                    bdArray = fund.getFinancials_OperIncome();
                    stmt.setBigDecimal(19, bdArray[j]);
                    
                    stmt.addBatch();
                }
            }
            
            //Save to DB
            stmt.executeBatch();
            conxn.commit();
            
        } catch(Exception exc) {
            logger.Log("StockDataHandler", "insertStockFundamentals_Annual_IntoDB", "Exception", exc.toString(), true);
            throw exc;
        }
    }

    private void insertQtrStockFundamentalsIntoDB(List<StockFundamentals> listFund) throws Exception {

        logger.Log("StockDataHandler", "insertQtrStockFundamentalsIntoDB", "", "", false);

        String row;
        java.sql.Date sqlDt;
       
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Insert_Stock_Fundamentals_Qtr (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)}")) {
            
            conxn.setAutoCommit(false);
            final BigDecimal BLANK_VALUE = new BigDecimal("0.0");

            for (int i = 0; i < listFund.size(); i++) {

                StockFundamentals fund = listFund.get(i);
                
                stmt.setString(1, fund.getTicker());
                
                sqlDt = new java.sql.Date(fund.getDate().getTime());
                stmt.setDate(2, sqlDt);
                
                sqlDt = new java.sql.Date(fund.getReportingDt().getTime());
                stmt.setDate(3, sqlDt);
                
                if (fund.getAssets() != null)
                    stmt.setBigDecimal(4, fund.getAssets());
                else
                    stmt.setBigDecimal(4, BLANK_VALUE);

                if (fund.getDebt() != null)
                    stmt.setBigDecimal(5, fund.getDebt());
                else
                    stmt.setBigDecimal(5, BLANK_VALUE);

                if (fund.getEquity() != null)
                    stmt.setBigDecimal(6, fund.getEquity());
                else
                    stmt.setBigDecimal(6, BLANK_VALUE);

                if (fund.getLiabilities() != null)
                    stmt.setBigDecimal(7, fund.getLiabilities());
                else
                    stmt.setBigDecimal(7, BLANK_VALUE);

                if (fund.getRevenue() != null)
                    stmt.setBigDecimal(8, fund.getRevenue());
                else
                    stmt.setBigDecimal(8, BLANK_VALUE);
                
                if (fund.getNetIncome() != null)
                    stmt.setBigDecimal(9, fund.getNetIncome());
                else
                    stmt.setBigDecimal(9, BLANK_VALUE);

                if (fund.getNetIncomeCommon() != null)
                    stmt.setBigDecimal(10, fund.getNetIncomeCommon());
                else
                    stmt.setBigDecimal(10, BLANK_VALUE);

                if (fund.getEbitda() != null)
                    stmt.setBigDecimal(11, fund.getEbitda());
                else
                    stmt.setBigDecimal(11, BLANK_VALUE);

                if (fund.getEbt() != null)
                    stmt.setBigDecimal(12, fund.getEbt());
                else
                    stmt.setBigDecimal(12, BLANK_VALUE);
                    
                if (fund.getNcfo() != null)
                    stmt.setBigDecimal(13, fund.getNcfo());
                else
                    stmt.setBigDecimal(13, BLANK_VALUE);

                if (fund.getDps() != null)
                    stmt.setBigDecimal(14, fund.getDps());
                else
                    stmt.setBigDecimal(14, BLANK_VALUE);
                    
                if (fund.getEps() != null)
                    stmt.setBigDecimal(15, fund.getEps());
                else
                    stmt.setBigDecimal(15, BLANK_VALUE);

                if (fund.getEpsDiluted() != null)
                    stmt.setBigDecimal(16, fund.getEpsDiluted());
                else
                    stmt.setBigDecimal(16, BLANK_VALUE);

                if (fund.getNumShares() != null)
                    stmt.setLong(17, fund.getNumShares().longValue());
                else
                    stmt.setLong(17, 0);

                if (fund.getIntExposure() != null)
                    stmt.setBigDecimal(18, fund.getIntExposure());
                else
                    stmt.setBigDecimal(18, BLANK_VALUE);
                    
                if (fund.getWorkingCapital() != null)
                    stmt.setBigDecimal(19, fund.getWorkingCapital());
                else
                    stmt.setBigDecimal(19, BLANK_VALUE);

                if (fund.getFcf() != null)
                    stmt.setBigDecimal(20, fund.getFcf());
                else
                    stmt.setBigDecimal(20, BLANK_VALUE);

                if (fund.getFcfps() != null)
                    stmt.setBigDecimal(21, fund.getFcfps());
                else
                    stmt.setBigDecimal(21, BLANK_VALUE);
                
                if (fund.getMarketCap() != null)
                    stmt.setBigDecimal(22, fund.getMarketCap());
                else
                    stmt.setBigDecimal(22, BLANK_VALUE);

                if (fund.getNetMargin() != null)
                    stmt.setBigDecimal(23, fund.getNetMargin());
                else
                    stmt.setBigDecimal(23, BLANK_VALUE);
                    
                if (fund.getPriceBook() != null)
                    stmt.setBigDecimal(24, fund.getPriceBook());
                else
                    stmt.setBigDecimal(24, BLANK_VALUE);

                if (fund.getPriceEarnings() != null)
                    stmt.setBigDecimal(25, fund.getPriceEarnings());
                else
                    stmt.setBigDecimal(25, BLANK_VALUE);
                
                if (fund.getPriceSales() != null)
                    stmt.setBigDecimal(26, fund.getPriceSales());
                else
                    stmt.setBigDecimal(26, BLANK_VALUE);
                
                if (fund.getRoa() != null)
                    stmt.setBigDecimal(27, fund.getRoa());
                else
                    stmt.setBigDecimal(27, BLANK_VALUE);
                    
                if (fund.getRoe() != null)
                    stmt.setBigDecimal(28, fund.getRoe());
                else
                    stmt.setBigDecimal(28, BLANK_VALUE);
                    
                if (fund.getRos() != null)
                    stmt.setBigDecimal(29, fund.getRos());
                else
                    stmt.setBigDecimal(29, BLANK_VALUE);
                    
                if (fund.getSps() != null)
                    stmt.setBigDecimal(30, fund.getSps());
                else
                    stmt.setBigDecimal(30, BLANK_VALUE);
                
                stmt.addBatch();
            }
            
            //Save to DB
            stmt.executeBatch();
            conxn.commit();
            
        } catch(Exception exc) {
            logger.Log("StockDataHandler", "insertQtrStockFundamentalsIntoDB", "Exception", exc.toString(), true);
            throw exc;
        }
    }
    
    private void insertStockFundamentals_Quarter_IntoDB(List<StockFundamentals_Quarter> listStockFund) throws Exception {

        logger.Log("StockDataHandler", "insertStockFundamentals_Quarter_IntoDB", "", "", false);

        String row;
        java.sql.Date sqlDt;
       
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Insert_Stock_Fundamentals_Quarter (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)}")) {
            
            conxn.setAutoCommit(false);
            
            for (int i = 0; i < listStockFund.size(); i++) {

                StockFundamentals_Quarter fund = listStockFund.get(i);

                //Loop through the dates
                Date[] dates = fund.getFinancials_Dates();
                BigDecimal[] bdArray;
                for (int j = 0; j < dates.length - 1; j++) { //Skip TTM

                    stmt.setString(1, fund.getTicker());
                    
                    sqlDt = new java.sql.Date(dates[j].getTime());
                    stmt.setDate(2, sqlDt);

                    bdArray = fund.getFinancials_Revenue();
                    if (bdArray != null)
                        stmt.setBigDecimal(3, bdArray[j]);
                    else
                        stmt.setBigDecimal(3, new BigDecimal("0.0"));
                    
                    bdArray = fund.getFinancials_CostOfRev();
                    if (bdArray != null)
                        stmt.setBigDecimal(4, bdArray[j]);
                    else
                        stmt.setBigDecimal(4, new BigDecimal("0.0"));
                    
                    bdArray = fund.getFinancials_GrossProfit();
                    if (bdArray != null)
                        stmt.setBigDecimal(5, bdArray[j]);
                    else
                        stmt.setBigDecimal(5, new BigDecimal("0.0"));
                    
                    bdArray = fund.getFinancials_RandD();
                    if (bdArray != null)
                        stmt.setBigDecimal(6, bdArray[j]);
                    else
                        stmt.setBigDecimal(6, new BigDecimal("0.0"));
                    
                    bdArray = fund.getFinancials_SalesGenAdmin();
                    if (bdArray != null)
                        stmt.setBigDecimal(7, bdArray[j]);
                    else
                        stmt.setBigDecimal(7, new BigDecimal("0.0"));
                    
                    bdArray = fund.getFinancials_TotalOpExp();
                    if (bdArray != null)
                        stmt.setBigDecimal(8, bdArray[j]);
                    else
                        stmt.setBigDecimal(8, new BigDecimal("0.0"));
                    
                    bdArray = fund.getFinancials_OperIncome();
                    if (bdArray != null)
                        stmt.setBigDecimal(9, bdArray[j]);
                    else
                        stmt.setBigDecimal(9, new BigDecimal("0.0"));
                    
                    bdArray = fund.getFinancials_IntExp();
                    if (bdArray != null)
                        stmt.setBigDecimal(10, bdArray[j]);
                    else
                        stmt.setBigDecimal(10, new BigDecimal("0.0"));
                    
                    bdArray = fund.getFinancials_OtherIncome();
                    if (bdArray != null)
                        stmt.setBigDecimal(11, bdArray[j]);
                    else
                        stmt.setBigDecimal(11, new BigDecimal("0.0"));
                    
                    bdArray = fund.getFinancials_IncomeBeforeTax();
                    if (bdArray != null)
                        stmt.setBigDecimal(12, bdArray[j]);
                    else
                        stmt.setBigDecimal(12, new BigDecimal("0.0"));
                    
                    bdArray = fund.getFinancials_ProvForIncTax();
                    if (bdArray != null)
                        stmt.setBigDecimal(13, bdArray[j]);
                    else
                        stmt.setBigDecimal(13, new BigDecimal("0.0"));
                    
                    bdArray = fund.getFinancials_NetIncomeContOp();
                    if (bdArray != null)
                        stmt.setBigDecimal(14, bdArray[j]);
                    else
                        stmt.setBigDecimal(14, new BigDecimal("0.0"));
                    
                    bdArray = fund.getFinancials_NetIncomeDiscontOp();
                    if (bdArray != null)
                        stmt.setBigDecimal(15, bdArray[j]);
                    else
                        stmt.setBigDecimal(15, new BigDecimal("0.0"));

                    bdArray = fund.getFinancials_NetIncome();
                    if (bdArray != null)
                        stmt.setBigDecimal(16, bdArray[j]);
                    else
                        stmt.setBigDecimal(16, new BigDecimal("0.0"));
                    
                    bdArray = fund.getFinancials_NetIncomeCommonShareholders();
                    if (bdArray != null)
                        stmt.setBigDecimal(17, bdArray[j]);
                    else
                        stmt.setBigDecimal(17, new BigDecimal("0.0"));
                    
                    bdArray = fund.getFinancials_EPS_Basic();
                    if (bdArray != null)
                        stmt.setBigDecimal(18, bdArray[j]);
                    else
                        stmt.setBigDecimal(18, new BigDecimal("0.0"));
                    
                    bdArray = fund.getFinancials_EPS_Diluted();
                    if (bdArray != null)
                        stmt.setBigDecimal(19, bdArray[j]);
                    else
                        stmt.setBigDecimal(19, new BigDecimal("0.0"));
                    
                    bdArray = fund.getFinancials_AvgSharesOutstanding_Basic();
                    if (bdArray != null)
                        stmt.setBigDecimal(20, bdArray[j]);
                    else
                        stmt.setBigDecimal(20, new BigDecimal("0.0"));
                    
                    bdArray = fund.getFinancials_AvgSharesOutstanding_Diluted();
                    if (bdArray != null)
                        stmt.setBigDecimal(21, bdArray[j]);
                    else
                        stmt.setBigDecimal(21, new BigDecimal("0.0"));
                    
                    bdArray = fund.getFinancials_EBITDA();
                    if (bdArray != null)
                        stmt.setBigDecimal(22, bdArray[j]);
                    else
                        stmt.setBigDecimal(22, new BigDecimal("0.0"));
                    
                    stmt.addBatch();
                }
            }
            
            //Save to DB
            stmt.executeBatch();
            conxn.commit();
            
        } catch(Exception exc) {
            logger.Log("StockDataHandler", "insertStockFundamentals_Quarter_IntoDB", "Exception", exc.toString(), true);
            throw exc;
        }
    }

    
    public Date get30YrMortgageRates_UpdateDate() throws Exception {

        logger.Log("StockDataHandler", "get30YrMortgageRates_UpdateDate", "", "", false);

        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Retrieve_30yr_MortgageRates_LastUpdate ()}")) {

            ResultSet rs = stmt.executeQuery();
            
            if (rs.next())
                return rs.getDate(1);
            else {
                Calendar c = GregorianCalendar.getInstance();
                c.set(1990, 1, 1);
                return c.getTime();
            }

        } catch (Exception exc) {
            logger.Log("StockDataHandler", "get30YrMortgageRates_UpdateDate", "Exception", exc.toString(), true);
            throw exc;
        }
    }

    public Date getAvgNewHomePrices_UpdateDate() throws Exception {

        logger.Log("StockDataHandler", "getAvgNewHomePrices_UpdateDate", "", "", false);

        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Retrieve_AvgNewHomePrices_LastUpdate ()}")) {

            ResultSet rs = stmt.executeQuery();
            
            if (rs.next())
                return rs.getDate(1);
            else {
                Calendar c = GregorianCalendar.getInstance();
                c.set(1990, 1, 1);
                return c.getTime();
            }
            
        } catch (Exception exc) {
            logger.Log("StockDataHandler", "getAvgNewHomePrices_UpdateDate", "Exception", exc.toString(), true);
            throw exc;
        }
    }

    public BigDecimal getSP500PercentChange(Date startDt, Date endDt) throws Exception {

        String summary = String.format("Start Date: %s, End Date: %s", startDt.toString(), endDt.toString());
        logger.Log("StockDataHandler", "getSP500PercentChange", summary, "", false);

        BigDecimal pctChg;
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Retrieve_SP500Chg (?, ?)}")) {

            java.sql.Date sDt = new java.sql.Date(startDt.getTime());
            stmt.setDate(1, sDt);

            java.sql.Date eDt = new java.sql.Date(endDt.getTime());
            stmt.setDate(2, eDt);
            
            ResultSet rs = stmt.executeQuery();
            
            if (rs.next()) {
                pctChg = rs.getBigDecimal(1);
            }
            else {
                return null;
            }
            
        } catch (Exception exc) {
            logger.Log("StockDataHandler", "getSP500PercentChange", "Exception", exc.toString(), true);
            throw exc;
        }
        
        return pctChg;
    }
    
    public StockQuote getStockQuote(String ticker, Date date) throws Exception {

        String summary = String.format("Ticker: %s, Date: %s", ticker, date.toString());
        logger.Log("StockDataHandler", "getStockQuote", summary, "", false);

        StockQuote quote = new StockQuote();
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Retrieve_StockQuote (?, ?)}")) {

            stmt.setString(1, ticker);
            
            java.sql.Date dt = new java.sql.Date(date.getTime());
            stmt.setDate(2, dt);
            
            ResultSet rs = stmt.executeQuery();
            
            if (rs.next()) {
                quote.setOpen(rs.getBigDecimal(1));
                quote.setHigh(rs.getBigDecimal(2));
                quote.setLow(rs.getBigDecimal(3));
                quote.setClose(rs.getBigDecimal(4));
                quote.setVolume(rs.getBigDecimal(5));
            }
            else {
                return null;
            }
            
        } catch (Exception exc) {
            logger.Log("StockDataHandler", "getStockQuote", "Exception", exc.toString(), true);
            throw exc;
        }
        
        return quote;
    }
    
    public Date getCPI_UpdateDate() throws Exception {

        logger.Log("StockDataHandler", "getCPI_UpdateDate", "", "", false);

        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Retrieve_ConsumerPriceIndex_LastUpdate ()}")) {
            
            ResultSet rs = stmt.executeQuery();
            
            if (rs.next())
                return rs.getDate(1);
            else {
                Calendar c = GregorianCalendar.getInstance();
                c.set(1990, 1, 1);
                return c.getTime();
            }
            
        } catch (Exception exc) {
            logger.Log("StockDataHandler", "getCPI_UpdateDate", "Exception", exc.toString(), true);
            throw exc;
        }
    }

    public Date getEconomicData_UpdateDate(String econInd) throws Exception {

        String summary = String.format("Indicator: %s", econInd);
        logger.Log("StockDataHandler", "getEconomicData_UpdateDate", summary, "", false);

        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Retrieve_EconomicData_LastUpdate (?)}")) {
            
            stmt.setString(1, econInd);
            ResultSet rs = stmt.executeQuery();
            
            if (rs.next())
                return rs.getDate(1);
            else {
                Calendar c = GregorianCalendar.getInstance();
                c.set(1990, 1, 1);
                return c.getTime();
            }
            
        } catch (Exception exc) {
            logger.Log("StockDataHandler", "getEconomicData_UpdateDate", "Exception", exc.toString(), true);
            throw exc;
        }
    }

    public Date getCurrencyRatios_UpdateDate(String currencyCode) throws Exception {

        String summary = String.format("Currency: %s", currencyCode);
        logger.Log("StockDataHandler", "getCurrencyRatios_UpdateDate", summary, "", false);

        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Retrieve_Currency_Ratios_LastUpdate (?)}")) {
            
            stmt.setString(1, currencyCode);
            ResultSet rs = stmt.executeQuery();
            
            if (rs.next())
                return rs.getDate(1);
            else {
                Calendar c = GregorianCalendar.getInstance();
                c.set(1990, 1, 1);
                return c.getTime();
            }
            
        } catch (Exception exc) {
            logger.Log("StockDataHandler", "getCurrencyRations_UpdateDate", "Exception", exc.toString(), true);
            throw exc;
        }
    }

    public Date getEnergyPrices_UpdateDate(String energyCode) throws Exception {

        String summary = String.format("Code: %s", energyCode);
        logger.Log("StockDataHandler", "getEnergyPrices_UpdateDate", summary, "", false);

        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Retrieve_Energy_Prices_LastUpdate (?)}")) {
            
            stmt.setString(1, energyCode);
            ResultSet rs = stmt.executeQuery();
            
            if (rs.next())
                return rs.getDate(1);
            else {
                Calendar c = GregorianCalendar.getInstance();
                c.set(1990, 1, 1);
                return c.getTime();
            }
            
        } catch (Exception exc) {
            logger.Log("StockDataHandler", "getEnergyPrices_UpdateDate", "Exception", exc.toString(), true);
            throw exc;
        }
    }

    public Map<Date, String> getAllHolidays() throws Exception {

        logger.Log("StockDataHandler", "getAllHolidays", "", "", false);

        Map<Date, String> holidayMap = new HashMap<>();
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Retrieve_AllHolidays ()}")) {
            
            ResultSet rs = stmt.executeQuery();
            
            while (rs.next()) {
                Date dt = rs.getDate(1);
                String val = rs.getString(2);
                holidayMap.put(dt, val);
            }

            return holidayMap;
            
        } catch (Exception exc) {
            logger.Log("StockDataHandler", "getHolidays", "Exception", exc.toString(), true);
            throw exc;
        }
        
    }
    
    public Quarter getBEA_UpdateDate() throws Exception {

        logger.Log("StockDataHandler", "getBEA_UpdateDate", "", "", false);

        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Retrieve_BEA_LastUpdate ()}")) {
            
            ResultSet rs = stmt.executeQuery();
            
            Quarter qtr;
            if (rs.next()) {
                qtr = new Quarter(rs.getInt(1), rs.getInt(2));
                return qtr;
            }
            else {
                return null;
            }
            
        } catch (Exception exc) {
            logger.Log("StockDataHandler", "getBEA_UpdateDate", "Exception", exc.toString(), true);
            throw exc;
        }
    }

    public Date getStockFundamentals_UpdateDate(String stockTicker, String indicator) throws Exception {

        String summary = String.format("Ticker: %s, Indicator: ", stockTicker, indicator);
        logger.Log("StockDataHandler", "getStockFundamentals_UpdateDate", summary, "", false);

        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Retrieve_StockFundamentals_LastUpdate (?, ?)}")) {
            
            stmt.setString(1, stockTicker);
            stmt.setString(2, indicator);
            
            ResultSet rs = stmt.executeQuery();
            
            if (rs.next())
                return rs.getDate(1);
            else {
                Calendar c = GregorianCalendar.getInstance();
                c.set(1990, 1, 1);
                return c.getTime();
            }
            
        } catch (Exception exc) {
            logger.Log("StockDataHandler", "getStockFundamentals_UpdateDate", "Exception", exc.toString(), true);
            throw exc;
        }
    }

    public Date getInterestRates_UpdateDate(final String INT_RATE_TYPE) throws Exception {

        String summary = String.format("Rate Type: %s", INT_RATE_TYPE);
        logger.Log("StockDataHandler", "getInterestRates_UpdateDate", summary, "", false);

        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Retrieve_InterestRates_LastUpdate (?)}")) {

            stmt.setString(1, INT_RATE_TYPE);
            
            ResultSet rs = stmt.executeQuery();
            
            if (rs.next())
                return rs.getDate(1);
            else {
                Calendar c = GregorianCalendar.getInstance();
                c.set(1990, 1, 1);
                return c.getTime();
            }
            
        } catch (Exception exc) {
            logger.Log("StockDataHandler", "getInterestRates_UpdateDate", "Exception", exc.toString(), true);
            throw exc;
        }
    }

    public Date getPreciousMetals_UpdateDate(String metalCode) throws Exception {

        String summary = String.format("Metal: %s", metalCode);
        logger.Log("StockDataHandler", "getPreciousMetals_UpdateDate", summary, "", false);

        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Retrieve_Precious_MetalsPrices_LastUpdate (?)}")) {
            
            stmt.setString(1, metalCode);
            ResultSet rs = stmt.executeQuery();
            
            if (rs.next())
                return rs.getDate(1);
            else {
                Calendar c = GregorianCalendar.getInstance();
                c.set(1990, 1, 1);
                return c.getTime();
            }
            
        } catch (Exception exc) {
            logger.Log("StockDataHandler", "getPreciousMetals_UpdateDate", "Exception", exc.toString(), true);
            throw exc;
        }
    }
    
    public Date getStockIndex_UpdateDate(String stockIndex) throws Exception {

        String summary = String.format("Index: %s", stockIndex);
        logger.Log("StockDataHandler", "getStockIndex_UpdateDate", summary, "", false);

        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Retrieve_Stock_Index_LastUpdate (?)}")) {
            
            stmt.setString(1, stockIndex);
            ResultSet rs = stmt.executeQuery();
            
            if (rs.next())
                return rs.getDate(1);
            else {
                Calendar c = GregorianCalendar.getInstance();
                c.set(1990, 1, 1);
                return c.getTime();
            }
            
        } catch (Exception exc) {
            logger.Log("StockDataHandler", "getStockIndex_UpdateDate", "Exception", exc.toString(), true);
            throw exc;
        }
    }

    public Date getStockQuote_UpdateDate(String stockQuote) throws Exception {

        String summary = String.format("Quote: %s", stockQuote);
        logger.Log("StockDataHandler", "getStockQuote_UpdateDate", summary, "", false);

        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Retrieve_StockQuotes_LastUpdate (?)}")) {
            
            stmt.setString(1, stockQuote);
            ResultSet rs = stmt.executeQuery();
            
            if (rs.next())
                return rs.getDate(1);
            else {
                Calendar c = GregorianCalendar.getInstance();
                c.set(1990, 1, 1);
                return c.getTime();
            }
            
        } catch (Exception exc) {
            logger.Log("StockDataHandler", "getStockQuote_UpdateDate", "Exception", exc.toString(), true);
            throw exc;
        }
    }

    public void setStockFundamentals_Quarter_ValidDates() throws Exception {
        
        logger.Log("StockDataHandler", "setStockFundamentals_Quarter_ValidDates", "", "", false);
        
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Update_Stock_Fundamentals_Quarter_ValidDates ()}")) {

            stmt.executeUpdate();
            
        } catch (Exception exc) {
            logger.Log("StockDataHandler", "setStockFundamentals_Quarter_ValidDates", "Exception", exc.toString(), true);
            throw exc;
        }
    }

    public void setStockFundamentals_Annual_PctChg() throws Exception {

        logger.Log("StockDataHandler", "setStockFundamentals_Annual_PctChg", "", "", false);
        
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Update_AnnualFundamentals_PctChg ()}")) {

            stmt.executeUpdate();
            
        } catch (Exception exc) {
            logger.Log("StockDataHandler", "setStockFundametals_Annual_PctChg", "Excepton", exc.toString(), true);
            throw exc;
        }
    }
    
    public void setStockFundamentals_Quarter_PctChg() throws Exception {

        logger.Log("StockDataHandler", "setStockFundamentals_Quarter_PctChg", "", "", false);

        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Update_QuarterlyFundamentals_PctChg ()}")) {

            stmt.executeUpdate();
            
        } catch (Exception exc) {
            logger.Log("StockDataHandler", "setStockFundamentals_Quarter_PctChg", "Exception", exc.toString(), true);
            throw exc;
        }
    }

    public Date getQtrStockFundamentals_UpdateDate(String ticker) throws Exception {

        logger.Log("StockDataHandler", "getQtrStockFundamentals_UpdateDate", "Ticker: " + ticker, "", false);

        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Retrieve_QtrStockFundamentals_LastUpdate (?)}")) {

            stmt.setString(1, ticker);
            ResultSet rs = stmt.executeQuery();
            
            if (rs.next())
                return rs.getDate(1);
            else {
                Calendar c = GregorianCalendar.getInstance();
                c.set(1990, 1, 1);
                return c.getTime();
            }
            
        } catch (Exception exc) {
            logger.Log("StockDataHandler", "getQtrStockFundamentals_UpdateDate", "Exception", exc.toString(), true);
            throw exc;
        }
    }
    
    public Date getUnemployment_UpdateDate() throws Exception {

        logger.Log("StockDataHandler", "getUnemployment_UpdateDate", "", "", false);

        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Retrieve_Unemployment_LastUpdate ()}")) {
            
            ResultSet rs = stmt.executeQuery();
            
            if (rs.next())
                return rs.getDate(1);
            else {
                Calendar c = GregorianCalendar.getInstance();
                c.set(1990, 1, 1);
                return c.getTime();
            }
            
        } catch (Exception exc) {
            logger.Log("StockDataHandler", "getUnemployment_UpdateDate", "Exception", exc.toString(), true);
            throw exc;
        }
    }
    
    private boolean isDataExpired(Date dt) throws Exception {

        String summary = String.format("Date: %s", dt.toString());
        logger.Log("StockDataHandler", "isDataExpired", summary, "", false);

        Calendar lastRun = Calendar.getInstance();
        lastRun.setTime(dt);
        
        Calendar today = Calendar.getInstance();
        int year = today.get(Calendar.YEAR);
        int month = today.get(Calendar.MONTH);
        int date = today.get(Calendar.DATE);
        today.set(year, month, date, 0, 0, 0); //Get rid of the Hour, Min, Sec
        today.set(Calendar.MILLISECOND, 0); //Get rid of Millis
        
        if (today.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY) 
            today.add(Calendar.DATE, -1); //Back to Friday

        if (today.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY) 
            today.add(Calendar.DATE, -2); //Back to Friday

        if (today.after(lastRun))
            return true;
        else
            return false;
    }
    
    public void downloadFundamentalsAndQuotes() throws Exception {
    
        logger.Log("StockDataHandler", "downloadFundamentalsAndQuotes", "", "", false);

        //Stock Quotes
        Date lastDt;
        List<StockTicker> listOfAllStocks = getAllStockTickers();
        for (StockTicker st : listOfAllStocks) {
            lastDt = getStockQuote_UpdateDate(st.getTicker());
            if (isDataExpired(lastDt)) {
                String stockValues = downloadData(st.getQuandlCode(), lastDt);
                insertStockPricesIntoDB(st.getTicker(), stockValues);
            }        
        }

        //Compute moving averages
        computeMovingAverages();
        computeStockQuoteSlopes();

        //Fundamentals - Premium Data
        for (StockTicker st : listOfAllStocks) {
            
            lastDt = getQtrStockFundamentals_UpdateDate(st.getTicker());
            List<StockFundamentals> qtrFundamentalsList = getQtrStockFundamentals(st.getTicker(), lastDt);
            if (qtrFundamentalsList != null)
                insertQtrStockFundamentalsIntoDB(qtrFundamentalsList);
        }
        setStockFundamentals_Quarter_ValidDates();
        setStockFundamentals_Quarter_PctChg();

        //Remove bad data
        removeAllBadData();
    }    
    
    public void downloadOtherStockData() throws Exception {

        logger.Log("StockDataHandler", "downloadOtherStockData", "", "", false);
        
        //Energy Prices
        final String CRUDE_OIL = "CRUDE-OIL";
        Date lastDt = getEnergyPrices_UpdateDate(CRUDE_OIL);
        if (isDataExpired(lastDt)) {
            String crudeOilPrices = downloadData("SCF/CME_CL1_EN", lastDt); // CHRIS/CME_CL1
            insertEnergyPricesIntoDB(CRUDE_OIL, crudeOilPrices);
        }

        final String NATURAL_GAS = "NATURL-GAS";
        lastDt = getEnergyPrices_UpdateDate(NATURAL_GAS);
        if (isDataExpired(lastDt)) {
            String naturalGasPrices = downloadData("SCF/CME_NG1_EN", lastDt); // CHRIS/CME_NG1
            insertEnergyPricesIntoDB(NATURAL_GAS, naturalGasPrices);
        }

        //M2 - Money Supply
        final String M2 = "M2-Vel";
        lastDt = getEconomicData_UpdateDate(M2);
        if (isDataExpired(lastDt)) {
            String m2Data = downloadData("FRED/M2V", lastDt);
            insertEconomicDataIntoDB(M2, m2Data);
        }
        
        //Mortgage Rates
        lastDt = get30YrMortgageRates_UpdateDate();
        if (isDataExpired(lastDt)) {
            String thirtyYrMtgRates = downloadData("FMAC/FIX30YR", lastDt);
            insertMortgageDataIntoDB(thirtyYrMtgRates);
        }        
        
        //New Home Prices
        lastDt = getAvgNewHomePrices_UpdateDate();
        if (isDataExpired(lastDt)) {
            String newHomePrices = downloadData("FRED/ASPNHSUS", lastDt);
            insertNewHomePriceDataIntoDB(newHomePrices);
        }

        //CPI
        lastDt = getCPI_UpdateDate();
        if (isDataExpired(lastDt)) {
            String cpiInflation = downloadData("RATEINF/CPI_USA", lastDt);
            insertInflationDataIntoDB(cpiInflation);
        }
        
        //Currency Ratios
        final String JAPAN = "JPY";
        lastDt = getCurrencyRatios_UpdateDate(JAPAN);
        if (isDataExpired(lastDt)) {
            String usdJpy = downloadData("CURRFX/USDJPY", lastDt); //Old Code "QUANDL/USDJPY"
            insertCurrencyRatiosIntoDB(JAPAN, usdJpy);
        }        
        
        final String AUSTRALIA = "AUD";
        lastDt = getCurrencyRatios_UpdateDate(AUSTRALIA);
        if (isDataExpired(lastDt)) {
            String usdAud = downloadData("CURRFX/USDAUD", lastDt); //Old Code "QUANDL/USDAUD"
            insertCurrencyRatiosIntoDB(AUSTRALIA, usdAud);
        }        
        
        final String EURO = "EUR";
        lastDt = getCurrencyRatios_UpdateDate(EURO);
        if (isDataExpired(lastDt)) {
            String usdEur = downloadData("CURRFX/USDEUR", lastDt); //Old Code "QUANDL/USDEUR"
            insertCurrencyRatiosIntoDB(EURO, usdEur);
        }        

        //Precious Metals
        final String GOLD = "GOLD";
        lastDt = getPreciousMetals_UpdateDate(GOLD);
        if (isDataExpired(lastDt)) {
            String goldPrices = downloadData("WGC/GOLD_DAILY_USD", lastDt);
            //Backup Source FRED/GOLDPMGBD228NLBM - Federal Reserve
            insertPreciousMetalsPricesIntoDB(GOLD, goldPrices);
        }
        
        final String SILVER = "SILVER";
        lastDt = getPreciousMetals_UpdateDate(SILVER);
        if (isDataExpired(lastDt)) {
            String silverPrices = downloadData("LBMA/SILVER", lastDt);
            insertPreciousMetalsPricesIntoDB(SILVER, silverPrices);
        }

        final String PLATINUM = "PLATINUM";
        lastDt = getPreciousMetals_UpdateDate(PLATINUM);
        if (isDataExpired(lastDt)) {
            String platinumPrices = downloadData("LPPM/PLAT", lastDt);
            insertPreciousMetalsPricesIntoDB(PLATINUM, platinumPrices);
        }        

        //Interest Rates - Prime
        final String PRIME = "PRIME";
        lastDt = getInterestRates_UpdateDate(PRIME);
        if (isDataExpired(lastDt)) {
            String primeRates = downloadData("FRED/DPRIME", lastDt);
            insertInterestRatesIntoDB(PRIME, primeRates);
        }        

        //Interest Rates - Effective Funds Rate
        final String EFF_FUNDS_RT = "EF_FNDS_RT";
        lastDt = getInterestRates_UpdateDate(EFF_FUNDS_RT);
        if (isDataExpired(lastDt)) {
            String effFundsRate = downloadData("FRED/DFF", lastDt);
            insertInterestRatesIntoDB(EFF_FUNDS_RT, effFundsRate);
        }        

        //Interest Rates - 6 Month T Bill
        final String SIX_MO_T_BILL = "6_MO_TBILL";
        lastDt = getInterestRates_UpdateDate(SIX_MO_T_BILL);
        if (isDataExpired(lastDt)) {
            String sixMoTBillRates = downloadData("FRED/DTB6", lastDt);
            insertInterestRatesIntoDB(SIX_MO_T_BILL, sixMoTBillRates);
        }

        //Interest Rates - 5 Year T Rates
        final String FIVE_YR_T_RT = "5_YR_T_RT";
        lastDt = getInterestRates_UpdateDate(FIVE_YR_T_RT);
        if (isDataExpired(lastDt)) {
            String fiveYrTRates = downloadData("FRED/DGS5", lastDt);
            insertInterestRatesIntoDB(FIVE_YR_T_RT, fiveYrTRates);
        }

        //Interest Rates - Credit Card Rates
        final String CREDIT_CARD_RT = "CREDIT_CRD";
        lastDt = getInterestRates_UpdateDate(CREDIT_CARD_RT);
        if (isDataExpired(lastDt)) {
            String cardRates = downloadData("FRED/TERMCBCCALLNS", lastDt);
            insertInterestRatesIntoDB(CREDIT_CARD_RT, cardRates);
        }
        
        //Interest Rates - 5 Year ARM Rates
        final String FIVE_YR_ARM_RT = "5_YR_ARM";
        lastDt = getInterestRates_UpdateDate(FIVE_YR_ARM_RT);
        if (isDataExpired(lastDt)) {
            String fiveYrARM = downloadData("FMAC/ARM5YR", lastDt);
            insertInterestRatesIntoDB(FIVE_YR_ARM_RT, fiveYrARM);
        }

        //Interest Rates - 6 Months LIBOR
        final String SIX_MO_LIBOR = "6_MO_LIBOR";
        lastDt = getInterestRates_UpdateDate(SIX_MO_LIBOR);
        if (isDataExpired(lastDt)) {
            String sixMoLIBOR = downloadData("FRED/USD6MTD156N", lastDt);
            insertInterestRatesIntoDB(SIX_MO_LIBOR, sixMoLIBOR);
        }
        
        //Interest Rates - 5 Year Swaps
        final String FIVE_YR_SWAP = "5_YR_SWAP";
        lastDt = getInterestRates_UpdateDate(FIVE_YR_SWAP);
        if (isDataExpired(lastDt)) {
            String fiveYrSwaps = downloadData("FRED/DSWP5", lastDt);
            insertInterestRatesIntoDB(FIVE_YR_SWAP, fiveYrSwaps);
        }
      
        //GDP
        List<BEA_Data> listBEAData = downloadBEAData();
        insertGDPDataIntoDB(listBEAData);
        
        //Global Stock Indexes
        final String SP500 = "S&P500";
        lastDt = getStockIndex_UpdateDate(SP500);
        if (isDataExpired(lastDt)) {
            String spIndex = downloadData("YAHOO/INDEX_GSPC", lastDt);
            insertStockIndexDataIntoDB(SP500, spIndex);
        }

        final String DAX = "DAX";
        lastDt = getStockIndex_UpdateDate(DAX);
        if (isDataExpired(lastDt)) {
            String daxIndex = downloadData("YAHOO/INDEX_GDAXI", lastDt);
            insertStockIndexDataIntoDB(DAX, daxIndex);
        }        

        final String HANGSENG = "HANGSENG";
        lastDt = getStockIndex_UpdateDate(HANGSENG);
        if (isDataExpired(lastDt)) {
            String hangSengIndex = downloadData("YAHOO/INDEX_HSI", lastDt);
            insertStockIndexDataIntoDB(HANGSENG, hangSengIndex);
        }

        final String NIKEII = "NIKEII";
        lastDt = getStockIndex_UpdateDate(NIKEII);
        if (isDataExpired(lastDt)) {
            String nikeiiIndex = downloadData("YAHOO/INDEX_N225", lastDt);
            insertStockIndexDataIntoDB(NIKEII, nikeiiIndex);
        }        

    }

    private List<StockFundamentals> getQtrStockFundamentals(String ticker, Date lastDt) throws Exception {

        //Revenue
        String quandlCode = "SF1/" + ticker + "_REVENUE_ARQ";
        String stockValues = downloadData(quandlCode, lastDt);

        //See if we have new data to process
        Map<Date, BigDecimal> revenueMap = new HashMap<>();
        if (stockValues.split("\n").length > 1) 
            insertFundamentalValuesIntoMap(stockValues, revenueMap);
        else 
            return null; //No new data

        //Filing Date to Reporting Date
        quandlCode = "SF1/" + ticker + "_FILINGDATE";
        stockValues = downloadData(quandlCode, lastDt);
        Map<Date, BigDecimal> reportingDtMap = new HashMap<>();
        insertFundamentalValuesIntoMap(stockValues, reportingDtMap);
        
        //Assets
        quandlCode = "SF1/" + ticker + "_ASSETS_ARQ";
        stockValues = downloadData(quandlCode, lastDt);
        Map<Date, BigDecimal> assetMap = new HashMap<>();
        insertFundamentalValuesIntoMap(stockValues, assetMap);

        //Debt
        quandlCode = "SF1/" + ticker + "_DEBT_ARQ";
        stockValues = downloadData(quandlCode, lastDt);
        Map<Date, BigDecimal> debtMap = new HashMap<>();
        insertFundamentalValuesIntoMap(stockValues, debtMap);
                
        //Equity
        quandlCode = "SF1/" + ticker + "_EQUITY_ARQ";
        stockValues = downloadData(quandlCode, lastDt);
        Map<Date, BigDecimal> equityMap = new HashMap<>();
        insertFundamentalValuesIntoMap(stockValues, equityMap);
        
        //Liabilities
        quandlCode = "SF1/" + ticker + "_LIABILITIES_ARQ";
        stockValues = downloadData(quandlCode, lastDt);
        Map<Date, BigDecimal> liabilitiesMap = new HashMap<>();
        insertFundamentalValuesIntoMap(stockValues, liabilitiesMap);
        
        //Net Income
        quandlCode = "SF1/" + ticker + "_NETINC_ARQ";
        stockValues = downloadData(quandlCode, lastDt);
        Map<Date, BigDecimal> netIncMap = new HashMap<>();
        insertFundamentalValuesIntoMap(stockValues, netIncMap);
        
        //Net Income Common
        quandlCode = "SF1/" + ticker + "_NETINCCMN_ARQ";
        stockValues = downloadData(quandlCode, lastDt);
        Map<Date, BigDecimal> netIncCmnMap = new HashMap<>();
        insertFundamentalValuesIntoMap(stockValues, netIncCmnMap);
        
        //EBITDA
        quandlCode = "SF1/" + ticker + "_EBITDA_ARQ";
        stockValues = downloadData(quandlCode, lastDt);
        Map<Date, BigDecimal> ebitdaMap = new HashMap<>();
        insertFundamentalValuesIntoMap(stockValues, ebitdaMap);

        //EBT
        quandlCode = "SF1/" + ticker + "_EBT_ARQ";
        stockValues = downloadData(quandlCode, lastDt);
        Map<Date, BigDecimal> ebtMap = new HashMap<>();
        insertFundamentalValuesIntoMap(stockValues, ebtMap);

        //NCFO
        quandlCode = "SF1/" + ticker + "_NCFO_ARQ";
        stockValues = downloadData(quandlCode, lastDt);
        Map<Date, BigDecimal> ncfoMap = new HashMap<>();
        insertFundamentalValuesIntoMap(stockValues, ncfoMap);

        //DPS
        quandlCode = "SF1/" + ticker + "_DPS_ARQ";
        stockValues = downloadData(quandlCode, lastDt);
        Map<Date, BigDecimal> dpsMap = new HashMap<>();
        insertFundamentalValuesIntoMap(stockValues, dpsMap);

        //EPS
        quandlCode = "SF1/" + ticker + "_EPS_ARQ";
        stockValues = downloadData(quandlCode, lastDt);
        Map<Date, BigDecimal> epsMap = new HashMap<>();
        insertFundamentalValuesIntoMap(stockValues, epsMap);

        //EPS Diluted
        quandlCode = "SF1/" + ticker + "_EPSDIL_ARQ";
        stockValues = downloadData(quandlCode, lastDt);
        Map<Date, BigDecimal> epsDilMap = new HashMap<>();
        insertFundamentalValuesIntoMap(stockValues, epsDilMap);

        //Number Shares
        quandlCode = "SF1/" + ticker + "_SHARESBAS";
        stockValues = downloadData(quandlCode, lastDt);
        Map<Date, BigDecimal> numSharesMap = new HashMap<>();
        insertFundamentalValuesIntoMap(stockValues, numSharesMap);

        //Interest Exposure
        quandlCode = "SF1/" + ticker + "_INTEXP_ARQ";
        stockValues = downloadData(quandlCode, lastDt);
        Map<Date, BigDecimal> intExpMap = new HashMap<>();
        insertFundamentalValuesIntoMap(stockValues, intExpMap);

        //Working Capital
        quandlCode = "SF1/" + ticker + "_WORKINGCAPITAL_ARQ";
        stockValues = downloadData(quandlCode, lastDt);
        Map<Date, BigDecimal> workCapMap = new HashMap<>();
        insertFundamentalValuesIntoMap(stockValues, workCapMap);

        //Free Cash Flow
        quandlCode = "SF1/" + ticker + "_FCF_ARQ";
        stockValues = downloadData(quandlCode, lastDt);
        Map<Date, BigDecimal> fcfMap = new HashMap<>();
        insertFundamentalValuesIntoMap(stockValues, fcfMap);

        //Free Cash Flow - Per Share
        quandlCode = "SF1/" + ticker + "_FCFPS_ARQ";
        stockValues = downloadData(quandlCode, lastDt);
        Map<Date, BigDecimal> fcfpsMap = new HashMap<>();
        insertFundamentalValuesIntoMap(stockValues, fcfpsMap);

        //Market Cap
        quandlCode = "SF1/" + ticker + "_MARKETCAP";
        stockValues = downloadData(quandlCode, lastDt);
        Map<Date, BigDecimal> mktCapMap = new HashMap<>();
        insertFundamentalValuesIntoMap(stockValues, mktCapMap);

        //Net Margin
        quandlCode = "SF1/" + ticker + "_NETMARGIN_ART";
        stockValues = downloadData(quandlCode, lastDt);
        Map<Date, BigDecimal> netMarginMap = new HashMap<>();
        insertFundamentalValuesIntoMap(stockValues, netMarginMap);
        
        //Price to Book
        quandlCode = "SF1/" + ticker + "_PB_ARQ";
        stockValues = downloadData(quandlCode, lastDt);
        Map<Date, BigDecimal> pbMap = new HashMap<>();
        insertFundamentalValuesIntoMap(stockValues, pbMap);

        //Price to Earnings
        quandlCode = "SF1/" + ticker + "_PE_ART";
        stockValues = downloadData(quandlCode, lastDt);
        Map<Date, BigDecimal> peMap = new HashMap<>();
        insertFundamentalValuesIntoMap(stockValues, peMap);
        
        //Price to Sales
        quandlCode = "SF1/" + ticker + "_PS_ART";
        stockValues = downloadData(quandlCode, lastDt);
        Map<Date, BigDecimal> psMap = new HashMap<>();
        insertFundamentalValuesIntoMap(stockValues, psMap);

        //Return on Assets
        quandlCode = "SF1/" + ticker + "_ROA_ART";
        stockValues = downloadData(quandlCode, lastDt);
        Map<Date, BigDecimal> roaMap = new HashMap<>();
        insertFundamentalValuesIntoMap(stockValues, roaMap);
        
        //Return on Equity
        quandlCode = "SF1/" + ticker + "_ROE_ART";
        stockValues = downloadData(quandlCode, lastDt);
        Map<Date, BigDecimal> roeMap = new HashMap<>();
        insertFundamentalValuesIntoMap(stockValues, roeMap);

        //Return on Sales
        quandlCode = "SF1/" + ticker + "_ROS_ART";
        stockValues = downloadData(quandlCode, lastDt);
        Map<Date, BigDecimal> rosMap = new HashMap<>();
        insertFundamentalValuesIntoMap(stockValues, rosMap);
        
        //Sales Per Share
        quandlCode = "SF1/" + ticker + "_SPS_ART";
        stockValues = downloadData(quandlCode, lastDt);
        Map<Date, BigDecimal> spsMap = new HashMap<>();
        insertFundamentalValuesIntoMap(stockValues, spsMap);

        //Now create the complete record list - Loop through all available dates
        List<StockFundamentals> listFundamentals = new ArrayList<>();
        Set<Entry<Date, BigDecimal>> entrySet = revenueMap.entrySet();
        for (Entry<Date, BigDecimal> entry : entrySet) {
            StockFundamentals fund = new StockFundamentals();
            
            fund.setTicker(ticker);
            
            Date dt = entry.getKey();
            fund.setDate(dt);

            BigDecimal revenue = entry.getValue();
            fund.setRevenue(revenue);

            //Set the Financial Reporting Date
            try {
                String reportingDt = reportingDtMap.get(dt).toPlainString();
                int year = Integer.parseInt(reportingDt.substring(0, 4));
                int month = Integer.parseInt(reportingDt.substring(4, 6));
                int day = Integer.parseInt(reportingDt.substring(6, 8));

                Calendar calRptDt = Calendar.getInstance();
                calRptDt.set(year, month, day);
                fund.setReportingDt(calRptDt.getTime());
                
            } catch (Exception exc) {
                logger.Log("StockDataHandler", "getQtrStockFundamentals", "Exception", exc.toString(), true);
                fund.setReportingDt(dt);
            }
            
            BigDecimal assets = assetMap.get(dt);
            fund.setAssets(assets);
            
            BigDecimal debt = debtMap.get(dt);
            fund.setDebt(debt);

            BigDecimal equity = equityMap.get(dt);
            fund.setEquity(equity);
            
            BigDecimal liabilities = liabilitiesMap.get(dt);
            fund.setLiabilities(liabilities);
            
            BigDecimal netIncome = netIncMap.get(dt);
            fund.setNetIncome(netIncome);
            
            BigDecimal netIncomeCommon = netIncCmnMap.get(dt);
            fund.setNetIncomeCommon(netIncomeCommon);
            
            BigDecimal ebitda = ebitdaMap.get(dt);
            fund.setEbitda(ebitda);
            
            BigDecimal ebt = ebtMap.get(dt);
            fund.setEbt(ebt);
            
            BigDecimal ncfo = ncfoMap.get(dt);
            fund.setNcfo(ncfo);
            
            BigDecimal dps = dpsMap.get(dt);
            fund.setDps(dps);
            
            BigDecimal eps = epsMap.get(dt);
            fund.setEps(eps);
            
            BigDecimal epsDil = epsDilMap.get(dt);
            fund.setEpsDiluted(epsDil);
            
            BigDecimal numShares = numSharesMap.get(dt);
            fund.setNumShares(numShares);
            
            BigDecimal intExp = intExpMap.get(dt);
            fund.setIntExposure(intExp);
            
            BigDecimal workCap = workCapMap.get(dt);
            fund.setWorkingCapital(workCap);
            
            BigDecimal fcf = fcfMap.get(dt);
            fund.setFcf(fcf);
            
            BigDecimal fcfps = fcfpsMap.get(dt);
            fund.setFcfps(fcfps);
            
            BigDecimal mktCap = mktCapMap.get(dt);
            fund.setMarketCap(mktCap);
            
            BigDecimal netMargin = netMarginMap.get(dt);
            fund.setNetMargin(netMargin);
            
            BigDecimal pb = pbMap.get(dt);
            fund.setPriceBook(pb);
            
            BigDecimal pe = peMap.get(dt);
            fund.setPriceEarnings(pe);
            
            BigDecimal ps = psMap.get(dt);
            fund.setPriceSales(ps);
            
            BigDecimal roa = roaMap.get(dt);
            fund.setRoa(roa);
            
            BigDecimal roe = roeMap.get(dt);
            fund.setRoe(roe);
            
            BigDecimal ros = rosMap.get(dt);
            fund.setRos(ros);
            
            BigDecimal sps = spsMap.get(dt);
            fund.setSps(sps);
            
            listFundamentals.add(fund);
        }
        
        return listFundamentals;
    }
    
    private void insertFundamentalValuesIntoMap(String stockValues, Map<Date, BigDecimal> map) throws Exception {

        logger.Log("StockDataHandler", "insertFundamentalValuesIntoMap", "", "", false);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date dt;
        BigDecimal value;
        String row;
        String[] rows = stockValues.split("\n");

        for (int i = 0; i < rows.length; i++) {
            if (i == 0) //Skip the header row
                continue;

            row = rows[i];
            String[] cells = row.split(",");
            dt = sdf.parse(cells[0]);
            value = new BigDecimal(cells[1]);

            //Insert the record into the Map
            map.put(dt, value);

        } //End for
    }
    
    private List<BEA_Data> downloadBEAData() throws Exception {

        logger.Log("StockDataHandler", "downloadBEAData", "", "", false);

        Quarter qtr = getBEA_UpdateDate();

        String yearStr;
        if (qtr == null)
            yearStr = "X";
        else {
            int year = qtr.getYear();
            yearStr = String.valueOf(year);
        }
        
        StringBuilder sb = new StringBuilder();
        List<BEA_Data> list = new ArrayList<>();

        try {
            URL url = new URL("http://www.bea.gov/api/data/?&UserID=" + BEA_USER_ID + "&method=GetData&DataSetName=NIPA&TableID=1&Frequency=Q&Year=" + yearStr + "&ResultFormat=JSON");
            URLConnection conxn = url.openConnection();

            logger.Log("StockDataHandler", "downloadBEAData", "URL", url.toString(), false);
            
            //Pull back the data as JSON
            try (InputStream is = conxn.getInputStream()) {
                int c;
                for(;;) {
                    c = is.read();
                    if (c == -1)
                        break;

                    sb.append((char)c);
                }
            }
           
            //Now parse the JSON
            JsonParser parser = Json.createParser(new StringReader(sb.toString()));
            String seriesCode = null;
            String lineDesc = null;
            String timePeriod = null;
            String dataValue = null;
            Map<String, BEA_Data> map = new HashMap<>();
            
            while(parser.hasNext()) {
                JsonParser.Event event = parser.next();
                if (event == JsonParser.Event.KEY_NAME) {
                    String tokenStr = parser.getString();
                    switch(tokenStr) {
                        case "SeriesCode":
                            parser.next();
                            seriesCode = parser.getString();
                            break;
                        case "LineDescription":
                            parser.next();
                            lineDesc = parser.getString();
                            break;
                        case "TimePeriod":
                            parser.next();
                            timePeriod = parser.getString();
                            break;
                        case "DataValue":
                            parser.next();
                            dataValue = parser.getString().replaceAll(",", "");
                            break;
                    } //End Switch
                } //End If

                //Save record to DB if we have all elements
                if (seriesCode != null && lineDesc != null && timePeriod != null && dataValue != null) {
                    
                    //See if this time period exists in the Hash Map
                    BEA_Data data = null;
                    if (map.get(timePeriod) == null) {
                        short year = Short.parseShort(timePeriod.substring(0, 4));
                        byte quarter = Byte.parseByte(timePeriod.substring(5, 6));

                        data = new BEA_Data(year, quarter);
                        map.put(timePeriod, data);
                    }
                    else {
                        data = map.get(timePeriod);
                    }
                    
                    //Now save the data to the correct field
                    BigDecimal val = new BigDecimal(dataValue);
                    switch (seriesCode) {
                        case "DDURRL": //Durable goods
                            data.setDurableGoods(val);
                            break;
                        case "Y033RL": //Equipment
                            data.setEquipment(val);
                            break;
                        case "A020RL": //Exports
                            data.setExports(val);
                            break;
                        case "A823RL": //Federal
                            data.setFederal(val);
                            break;
                        case "A007RL": //Fixed investment
                            data.setFixInvestment(val);
                            break;
                        case "DGDSRL": //Goods1
                            data.setGoods1(val);
                            break;
                        case "A255RL": //Goods2
                            data.setGoods2(val);
                            break;
                        case "A253RL": //Goods3
                            data.setGoods3(val);
                            break;
                        case "A822RL": //Government consumption expenditures and gross investment	
                            data.setGovConsExpAndGrossInv(val);
                            break;
                        case "A191RL": //Gross domestic product	
                            data.setGDP(val);
                            break;
                        case "A006RL": //Gross private domestic investment	
                            data.setGrossPrivDomInv(val);
                            break;
                        case "A021RL": //Imports	
                            data.setImports(val);
                            break;
                        case "Y001RL": //Intellectual property products	
                            data.setIntPropProducts(val);
                            break;
                        case "A824RL": //National defense	
                            data.setNatDefense(val);
                            break;
                        case "A825RL": //Nondefense
                            data.setNonDefense(val);
                            break;
                        case "DNDGRL": //Nondurable goods	
                            data.setNonDurGoods(val);
                            break;
                        case "A008RL": //Nonresidential	
                            data.setNonResidential(val);
                            break;
                        case "DPCERL": //Personal consumption expenditures	
                            data.setPersConsExp(val);
                            break;
                        case "A011RL": //Residential	
                            data.setResidential(val);
                            break;
                        case "DSERRL": //Services1
                            data.setServices1(val);
                            break;
                        case "A646RL": //Services2	
                            data.setServices2(val);
                            break;
                        case "A656RL": //Services3	
                            data.setServices3(val);
                            break;
                        case "A829RL": //State and local	
                            data.setStateAndLocal(val);
                            break;
                        case "A009RL": //Structures	
                            data.setStructures(val);
                            break;
                    } //End case
                
                    //Reset Values
                    seriesCode = lineDesc = timePeriod = dataValue = null;

                } //End If
            } //End parsing loop
            
            //Now extract out a list
            Set<Entry<String, BEA_Data>> set = map.entrySet();
            for (Entry<String, BEA_Data> entry : set) {
                list.add(entry.getValue());
            }

        } catch(Exception exc) {
            logger.Log("StockDataHandler", "downloadBEAData", "Exception", exc.toString(), true);
        }
        
        return list;
    }
    
    public void setModelValues(String ticker, String modelType, int daysForecast, double accuracy) throws Exception {

        String summary = String.format("Ticker: %s, Model: %s, Days Forecast: %d, Accuracy: %.3f", ticker, modelType, daysForecast, accuracy);
        logger.Log("StockDataHandler", "setModelValues", summary, "", false);

        java.sql.Date dt = new java.sql.Date(new Date().getTime());
        
        try (Connection conxn = getDBConnection();
             CallableStatement stmtModel = conxn.prepareCall("{call sp_Insert_Model_Runs (?, ?, ?, ?)}")) {

            stmtModel.setString(1, ticker);
            stmtModel.setString(2, modelType);
            stmtModel.setInt(3, daysForecast);
            stmtModel.setDouble(4, accuracy);
            stmtModel.executeUpdate();

        } catch(Exception exc) {
            logger.Log("StockDataHandler", "setModelValues", "Exception", exc.toString(), true);
            throw exc;
        }
    }
    
    public String downloadCensusData() throws Exception {

        logger.Log("StockDataHandler", "downloadCensusData", "", "", false);

        String key = "aff69a193c409c1d534e2e03c908e7a7ce06cb78";
        String retailTradeAndFoodSvc = "http://api.census.gov/data/eits/mrts?get=cell_value&for=us:*&category_code=44X72&data_type_code=SM&time=from+2004-08&key=" + key;
        String housingStarts = "http://api.census.gov/data/eits/resconst?get=cell_value&for=us:*&category_code=STARTS&data_type_code=TOTAL&time=from+2004-08&key=" + key;
        String manuTradeInvAndSales = "http://api.census.gov/data/eits/mtis?get=cell_value&for=us:*&category_code=TOTBUS&data_type_code=IM&time=from+2004-08&key=" + key;
        
        StringBuilder responseStr = new StringBuilder();

        try {
            URL url = new URL(manuTradeInvAndSales);
            URLConnection conxn = url.openConnection();

            System.out.println("Downloading: " + manuTradeInvAndSales);
            
            //Pull back the data as CSV
            try (InputStream is = conxn.getInputStream()) {
                
                int b;
                for(;;) {
                    b = is.read();
                    if (b == -1)
                        break;
                    
                    responseStr.append((char) b);
                }
            }
            
        } catch(Exception exc) {
            logger.Log("StockDataHandler", "downloadCensusData", "Exception", exc.toString(), true);
        }

        return responseStr.toString();
    }
    
    private String downloadData(final String QUANDL_CODE, final Date FROM_DT) throws Exception {

        String summary = String.format("Code: %s, From: %s", QUANDL_CODE, FROM_DT.toString());
        logger.Log("StockDataHandler", "downloadData", summary, "", false);

        //Slow Down
        Thread.sleep(2100);
        
        //Move the date ONE day ahead
        final long DAY_IN_MILLIS = 86400000;
        Date newFromDt = new Date();
        newFromDt.setTime(FROM_DT.getTime() + DAY_IN_MILLIS);
        
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String dtStr = sdf.format(newFromDt);
        
        String quandlQuery = QUANDL_BASE_URL + QUANDL_CODE + ".csv?auth_token=" + QUANDL_AUTH_TOKEN + "&trim_start=" + dtStr + "&sort_order=asc";
        
        StringBuilder responseStr = new StringBuilder();

        try {
            URL url = new URL(quandlQuery);
            URLConnection conxn = url.openConnection();

            logger.Log("StockDataHandler", "downloadData", "Downloading", quandlQuery, false);
            
            //Pull back the data as CSV
            try (InputStream is = conxn.getInputStream()) {
                
                int b;
                for(;;) {
                    b = is.read();
                    if (b == -1)
                        break;
                    
                    responseStr.append((char) b);
                }
            }
            
        } catch(Exception exc) {
            logger.Log("StockDataHandler", "downloadData", "Exception", exc.toString(), true);
        }

        return responseStr.toString();
    }
}
