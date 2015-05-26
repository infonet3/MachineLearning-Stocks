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
import Modeling.PredictionType;
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
import java.net.HttpURLConnection;
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
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

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
    
    final String FRED_KEY;
    
    final int SVC_THROTTLE;

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
            
            FRED_KEY = p.getProperty("fred_key");
            
            SVC_THROTTLE = Integer.parseInt(p.getProperty("throttle"));
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
                if (map.containsKey(dt)) {
                    String errorStr = String.format("Ticker: %s, Date: %s, Duplicate dates found!", p.getTicker(), dt.toString());
                    logger.Log("StockDataHandler", "insertStockPrediction", "Exception", errorStr, true);
                    continue;
                }
                else
                    map.put(dt, dt);
                
                //Write values to DB
                stmt.setString(1, p.getTicker());
                stmt.setDate(2, dt);
                stmt.setDate(3, projDt);
                stmt.setString(4, p.getModelType());
                stmt.setString(5, p.getPredType().toString());
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

        FuturePrice fp = null;
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Retrieve_Predictions_Regression_PctForecast(?, ?, ?, ?)}")) {

            stmt.setString(1, stock);
            
            java.sql.Date dt = new java.sql.Date(date.getTime());
            stmt.setDate(2, dt);

            stmt.setString(3, PRED_TYPE);
            stmt.setString(4, MODEL_TYPE);
            
            ResultSet rs = stmt.executeQuery();
            
            BigDecimal curPrice;
            BigDecimal predPrice;
            double forecastPctChg = 0.0;
            Date projectedDt;
            if (rs.next()) {
                curPrice = rs.getBigDecimal(1);
                predPrice = rs.getBigDecimal(2);
                forecastPctChg = rs.getDouble(3);
                projectedDt = rs.getDate(4);

                fp = new FuturePrice(stock, forecastPctChg, curPrice, predPrice, projectedDt);
            }
            else
                throw new Exception("Method: getTargetValueRegressionPredictions, Description: No data returned!");
                
        } catch(Exception exc) {
            logger.Log("StockDataHandler", "getTargetValueRegressionPredictions", "Exception", exc.toString(), true);
            throw exc;
        }
        
        return fp;
    }

    public List<String> getMagicPicks(final int NUM_STOCKS, final Date RUN_DATE) throws Exception {

        String summary = String.format("Num Stocks: %d, Date: %s", NUM_STOCKS, RUN_DATE.toString());
        logger.Log("StockDataHandler", "getMagicPicks", summary, "", false);

        List<String> stockPicks = new ArrayList<>();
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Retrieve_MagicFormula(?, ?)}")) {

            java.sql.Date dt = new java.sql.Date(RUN_DATE.getTime());
            stmt.setDate(1, dt);

            stmt.setInt(2, NUM_STOCKS);
            
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                String ticker = rs.getString(1);
                stockPicks.add(ticker);
            }
            
            if (stockPicks.isEmpty())
                throw new Exception("No stocks returned!");
                
        } catch(Exception exc) {
            logger.Log("StockDataHandler", "getMagicPicks", "Exception", exc.toString(), true);
            throw exc;
        }
        
        return stockPicks;
    }

    public List<FuturePrice> getTargetValueRegressionPredictionsForAllStocks(final Date RUN_DT, final String PRED_TYPE) throws Exception {

        String summary = String.format("Date: %s", RUN_DT.toString());
        logger.Log("StockDataHandler", "getTargetValueRegressionPredictionsForAllStocks", summary, "", false);

        List<FuturePrice> fpList = new ArrayList<>();
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Retrieve_Predictions_Regression(?, ?)}")) {

            java.sql.Date dt = new java.sql.Date(RUN_DT.getTime());
            stmt.setDate(1, dt);

            stmt.setString(2, PRED_TYPE);
            
            ResultSet rs = stmt.executeQuery();
            
            String ticker;
            BigDecimal curPrice;
            BigDecimal predPrice;
            double forecastPctChg;
            Date projectedDt;
            while (rs.next()) {
                ticker = rs.getString(1);
                curPrice = rs.getBigDecimal(2);
                predPrice = rs.getBigDecimal(3);
                forecastPctChg = rs.getDouble(4);
                projectedDt = rs.getDate(5);
                
                FuturePrice fp = new FuturePrice(ticker, forecastPctChg, curPrice, predPrice, projectedDt);
                fpList.add(fp);
            }
                
        } catch(Exception exc) {
            logger.Log("StockDataHandler", "getTargetValueRegressionPredictionsForAllStocks", "Exception", exc.toString(), true);
            throw exc;
        }
        
        return fpList;
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

    /*Method: getPredToTickersPct
     *Description: Determines how many predictions we have per tickers in the DB
     */
    public double getPredToTickersPct(final Date RUN_DATE) throws Exception {

        String summary = String.format("Run Date: %s", RUN_DATE.toString());
        logger.Log("StockDataHandler", "getPredToTickersPct", summary, "", false);

        double pct = 0.0;
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Retrieve_PredictionToTickerPct(?)}")) {
            
            java.sql.Date dt = new java.sql.Date(RUN_DATE.getTime());
            stmt.setDate(1, dt);

            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                pct = rs.getDouble(1);
            }
                
        } catch(Exception exc) {
            logger.Log("StockDataHandler", "getPredToTickersPct", "Exception", exc.toString(), true);
            throw exc;
        }
        
        return pct;
    }
    
    public String getAllStockFeaturesFromDB(String stockTicker, int daysInFuture, ModelTypes approach, Date fromDt, Date toDt, boolean saveToFile, final PredictionType PRED_TYPE) throws Exception {

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
                case VOTING:
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
                logger.Log("StockDataHandler", "getAllStockFeaturesFromDB", excOutput, "", true);
                
                if (PRED_TYPE == PredictionType.BACKTEST) {
                    throw new Exception("No records returned from the DB!");
                }
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
                    volume = new BigDecimal(cells[5]);
                    adjClosePrice = new BigDecimal(cells[6]);

                    /*ONLY USE IF PULLING FROM QUANDL!!!
                    //Parse NIKEII differently
                    if (stockIndex.equals("NIKEII")) {
                        volume = new BigDecimal(0.0);
                        adjClosePrice = new BigDecimal(0.0);
                        
                    } else {
                        volume = new BigDecimal(cells[5]);
                        adjClosePrice = new BigDecimal(cells[6]);
                    }
                    */ 

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

    private void insertMutualFundDataIntoDB(String fundTicker, String fundPrices) throws Exception {

        String summary = String.format("Fund: %s", fundTicker);
        logger.Log("StockDataHandler", "insertMutualFundDataIntoDB", summary, "", false);
        
        String[] rows = fundPrices.split("\n");

        String row;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date dt;
        java.sql.Date sqlDt;
        BigDecimal adjClosePrice;

        int i = 0;
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Insert_MutualFund_Quote (?, ?, ?)}")) {

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

                    adjClosePrice = new BigDecimal(cells[6]);

                    //Insert the record into the DB
                    stmt.setString(1, fundTicker);
                    stmt.setDate(2, sqlDt);
                    stmt.setBigDecimal(3, adjClosePrice);
                    stmt.addBatch();
                    
                } catch(Exception exc) {
                    logger.Log("StockDataHandler", "insertMutualFundDataIntoDB", "Exception", exc.toString(), true);
                }
            } //End For
            
            //Execute DB commands
            stmt.executeBatch();
            conxn.commit();
            
        } catch(Exception exc) {
            logger.Log("StockDataHandler", "insertMutualFundDataIntoDB", "Exception", exc.toString(), true);
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
                    
                    price = new BigDecimal(cells[4]);

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
    
    private void insertInterestRatesIntoDB(final String RATE_TYPE, String interestRates) throws Exception {

        String summary = String.format("Rate Type: %s", RATE_TYPE);
        logger.Log("StockDataHandler", "insertInterestRatesIntoDB", summary, "", false);

        if (interestRates.isEmpty())
            return;
        
        String[] rows = interestRates.split("\n");

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

                //Parse the record
                try {
                    row = rows[i];
                    String[] cells = row.split(",");

                    dt = sdf.parse(cells[0]);
                    sqlDt = new java.sql.Date(dt.getTime());
                    
                    if (cells[1].equals("."))
                        continue;
                    else
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

        if (econData.isEmpty())
            return;
        
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

        if (thirtyYrMtgRates.isEmpty())
            return;
        
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

        if (newHomePrices.isEmpty())
            return;
        
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

    private void insertAnnualStockFundamentalsIntoDB(List<StockFundamentals> listFund) throws Exception {

        logger.Log("StockDataHandler", "insertAnnualStockFundamentalsIntoDB", "", "", false);

        String row;
        java.sql.Date sqlDt;
       
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Insert_Stock_Fundamentals_Annual (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)}")) {
            
            conxn.setAutoCommit(false);
            final BigDecimal BLANK_VALUE = new BigDecimal("0.0");

            for (int i = 0; i < listFund.size(); i++) {

                StockFundamentals fund = listFund.get(i);
                
                stmt.setString(1, fund.getTicker());
                
                sqlDt = new java.sql.Date(fund.getDate().getTime());
                stmt.setDate(2, sqlDt);

                if (fund.getRevenue() != null)
                    stmt.setBigDecimal(3, fund.getRevenue());
                else
                    stmt.setBigDecimal(3, BLANK_VALUE);
                
                if (fund.getNetIncome() != null)
                    stmt.setBigDecimal(4, fund.getNetIncome());
                else
                    stmt.setBigDecimal(4, BLANK_VALUE);

                if (fund.getNetIncomeCommon() != null)
                    stmt.setBigDecimal(5, fund.getNetIncomeCommon());
                else
                    stmt.setBigDecimal(5, BLANK_VALUE);

                if (fund.getEbitda() != null)
                    stmt.setBigDecimal(6, fund.getEbitda());
                else
                    stmt.setBigDecimal(6, BLANK_VALUE);

                if (fund.getEbt() != null)
                    stmt.setBigDecimal(7, fund.getEbt());
                else
                    stmt.setBigDecimal(7, BLANK_VALUE);
                    
                if (fund.getNcfo() != null)
                    stmt.setBigDecimal(8, fund.getNcfo());
                else
                    stmt.setBigDecimal(8, BLANK_VALUE);

                if (fund.getDps() != null)
                    stmt.setBigDecimal(9, fund.getDps());
                else
                    stmt.setBigDecimal(9, BLANK_VALUE);
                    
                if (fund.getEps() != null)
                    stmt.setBigDecimal(10, fund.getEps());
                else
                    stmt.setBigDecimal(10, BLANK_VALUE);

                if (fund.getEpsDiluted() != null)
                    stmt.setBigDecimal(11, fund.getEpsDiluted());
                else
                    stmt.setBigDecimal(11, BLANK_VALUE);

                if (fund.getIntExposure() != null)
                    stmt.setBigDecimal(12, fund.getIntExposure());
                else
                    stmt.setBigDecimal(12, BLANK_VALUE);
                    
                if (fund.getFcf() != null)
                    stmt.setBigDecimal(13, fund.getFcf());
                else
                    stmt.setBigDecimal(13, BLANK_VALUE);

                if (fund.getFcfps() != null)
                    stmt.setBigDecimal(14, fund.getFcfps());
                else
                    stmt.setBigDecimal(14, BLANK_VALUE);
                
                stmt.addBatch();
            }
            
            //Save to DB
            stmt.executeBatch();
            conxn.commit();
            
        } catch(Exception exc) {
            logger.Log("StockDataHandler", "insertAnnualStockFundamentalsIntoDB", "Exception", exc.toString(), true);
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

    public Date getMutualFund_UpdateDate(String fundTicker) throws Exception {

        String summary = String.format("Fund: %s", fundTicker);
        logger.Log("StockDataHandler", "getMutualFund_UpdateDate", summary, "", false);

        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Retrieve_MutualFunds_LastUpdate (?)}")) {
            
            stmt.setString(1, fundTicker);
            ResultSet rs = stmt.executeQuery();
            
            if (rs.next())
                return rs.getDate(1);
            else {
                Calendar c = GregorianCalendar.getInstance();
                c.set(1990, 1, 1);
                return c.getTime();
            }
            
        } catch (Exception exc) {
            logger.Log("StockDataHandler", "getMutualFund_UpdateDate", "Exception", exc.toString(), true);
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

    public void setStockFundamentals_Annual_ValidDates() throws Exception {
        
        logger.Log("StockDataHandler", "setStockFundamentals_Annual_ValidDates", "", "", false);
        
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Update_Stock_Fundamentals_Annual_ValidDates ()}")) {

            stmt.executeUpdate();
            
        } catch (Exception exc) {
            logger.Log("StockDataHandler", "setStockFundamentals_Annual_ValidDates", "Exception", exc.toString(), true);
            throw exc;
        }
    }

    public void setEconomicData_ValidDates() throws Exception {
        
        logger.Log("StockDataHandler", "setEconomicData_ValidDates", "", "", false);
        
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Update_EconomicData_ValidDates ()}")) {

            stmt.executeUpdate();
            
        } catch (Exception exc) {
            logger.Log("StockDataHandler", "setEconomicData_ValidDates", "Exception", exc.toString(), true);
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

    public void setStockFundamentals_Annual_PctChg() throws Exception {

        logger.Log("StockDataHandler", "setStockFundamentals_Annual_PctChg", "", "", false);

        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Update_AnnualFundamentals_PctChg ()}")) {

            stmt.executeUpdate();
            
        } catch (Exception exc) {
            logger.Log("StockDataHandler", "setStockFundamentals_Annual_PctChg", "Exception", exc.toString(), true);
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

    public Date getAnnualStockFundamentals_UpdateDate(String ticker) throws Exception {

        logger.Log("StockDataHandler", "getAnnualStockFundamentals_UpdateDate", "Ticker: " + ticker, "", false);

        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Retrieve_AnnualStockFundamentals_LastUpdate (?)}")) {

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
            logger.Log("StockDataHandler", "getAnnualStockFundamentals_UpdateDate", "Exception", exc.toString(), true);
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

        //Fundamentals - Quarterly
        for (StockTicker st : listOfAllStocks) {
            
            lastDt = getQtrStockFundamentals_UpdateDate(st.getTicker());
            List<StockFundamentals> qtrFundamentalsList = getQtrStockFundamentals(st.getTicker(), lastDt);
            if (qtrFundamentalsList != null)
                insertQtrStockFundamentalsIntoDB(qtrFundamentalsList);
        }
        setStockFundamentals_Quarter_ValidDates();
        setStockFundamentals_Quarter_PctChg();

        //Fundamentals - Annual
        for (StockTicker st : listOfAllStocks) {
            
            lastDt = getAnnualStockFundamentals_UpdateDate(st.getTicker());
            List<StockFundamentals> annualFundamentalsList = getAnnualStockFundamentals(st.getTicker(), lastDt);
            if (annualFundamentalsList != null)
                insertAnnualStockFundamentalsIntoDB(annualFundamentalsList);
        }
        setStockFundamentals_Annual_ValidDates();
        setStockFundamentals_Annual_PctChg();
        
        //Remove bad data
        removeAllBadData();
    }    
    
    public void downloadOtherStockData() throws Exception {

        logger.Log("StockDataHandler", "downloadOtherStockData", "", "", false);

        //Precious Metals
        final String GOLD = "GOLD";
        Date lastDt = getPreciousMetals_UpdateDate(GOLD);
        if (isDataExpired(lastDt)) {
            String goldPrices = downloadData("SCF/CME_GC1_EN", lastDt); //Old Code "WGC/GOLD_DAILY_USD"
            insertPreciousMetalsPricesIntoDB(GOLD, goldPrices);
        }
        
        final String SILVER = "SILVER";
        lastDt = getPreciousMetals_UpdateDate(SILVER);
        if (isDataExpired(lastDt)) {
            String silverPrices = downloadData("SCF/CME_SI1_EN", lastDt); //Old Code "LBMA/SILVER"
            insertPreciousMetalsPricesIntoDB(SILVER, silverPrices);
        }

        final String PLATINUM = "PLATINUM";
        lastDt = getPreciousMetals_UpdateDate(PLATINUM);
        if (isDataExpired(lastDt)) {
            String platinumPrices = downloadData("SCF/CME_PL1_EN", lastDt); //Old Code "LPPM/PLAT"
            insertPreciousMetalsPricesIntoDB(PLATINUM, platinumPrices);
        }        

        //Energy Prices
        final String CRUDE_OIL = "CRUDE-OIL";
        lastDt = getEnergyPrices_UpdateDate(CRUDE_OIL);
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

        //START FRED DATA----------------------------------------------------------------------------------------------------------

        //Mortgage Rates
        final String MORT_RATES = "MORT-RATES";
        lastDt = getEconomicData_UpdateDate(MORT_RATES);
        if (isDataExpired(lastDt)) {
            String thirtyYrMtgRates = downloadFREDData("MORTGAGE30US", lastDt);
            insertEconomicDataIntoDB(MORT_RATES, thirtyYrMtgRates);
        }
        
        //M2 - Values
        final String M2_VAL = "M2-VAL";
        lastDt = getEconomicData_UpdateDate(M2_VAL);
        if (isDataExpired(lastDt)) {
            String m2ValueData = downloadFREDData("M2", lastDt);
            insertEconomicDataIntoDB(M2_VAL, m2ValueData);
        }

        //GDP - Values
        final String GDP_VAL = "GDP-VAL";
        lastDt = getEconomicData_UpdateDate(GDP_VAL);
        if (isDataExpired(lastDt)) {
            String gdpValueData = downloadFREDData("GDP", lastDt);
            insertEconomicDataIntoDB(GDP_VAL, gdpValueData);
        }
        
        //GDP - Percent
        final String GDP_PCT = "GDP-PCT";
        lastDt = getEconomicData_UpdateDate(GDP_PCT);
        if (isDataExpired(lastDt)) {
            String gdpPctData = downloadFREDData("A191RL1Q225SBEA", lastDt);
            insertEconomicDataIntoDB(GDP_PCT, gdpPctData);
        }

        //IMPORTS - Values
        final String IMPORTS_VAL = "IMPORTS-VAL";
        lastDt = getEconomicData_UpdateDate(IMPORTS_VAL);
        if (isDataExpired(lastDt)) {
            String importsValueData = downloadFREDData("IMPGS", lastDt);
            insertEconomicDataIntoDB(IMPORTS_VAL, importsValueData);
        }

        //IMPORTS - Percent
        final String IMPORTS_PCT = "IMPORTS-PCT";
        lastDt = getEconomicData_UpdateDate(IMPORTS_PCT);
        if (isDataExpired(lastDt)) {
            String importsPctData = downloadFREDData("A021RL1Q158SBEA", lastDt);
            insertEconomicDataIntoDB(IMPORTS_PCT, importsPctData);
        }
        
        //EXPORTS - Value
        final String EXPORTS_VAL = "EXPORTS-VAL";
        lastDt = getEconomicData_UpdateDate(EXPORTS_VAL);
        if (isDataExpired(lastDt)) {
            String exportsValueData = downloadFREDData("EXPGS", lastDt);
            insertEconomicDataIntoDB(EXPORTS_VAL, exportsValueData);
        }
        
        //EXPORTS - Percent
        final String EXPORTS_PCT = "EXPORTS-PCT";
        lastDt = getEconomicData_UpdateDate(EXPORTS_PCT);
        if (isDataExpired(lastDt)) {
            String exportsPctData = downloadFREDData("A020RL1Q158SBEA", lastDt);
            insertEconomicDataIntoDB(EXPORTS_PCT, exportsPctData);
        }
        
        //NET EXPORTS
        final String NET_EXPORTS = "NET-EXPORTS";
        lastDt = getEconomicData_UpdateDate(NET_EXPORTS);
        if (isDataExpired(lastDt)) {
            String netExports = downloadFREDData("NETEXP", lastDt);
            insertEconomicDataIntoDB(NET_EXPORTS, netExports);
        }
        
        //GOV CONS - Value
        final String GOV_VAL = "GOV-VAL";
        lastDt = getEconomicData_UpdateDate(GOV_VAL);
        if (isDataExpired(lastDt)) {
            String govValueData = downloadFREDData("GCE", lastDt);
            insertEconomicDataIntoDB(GOV_VAL, govValueData);
        }
        
        //GOV CONS - Percent
        final String GOV_PCT = "GOV-PCT";
        lastDt = getEconomicData_UpdateDate(GOV_PCT);
        if (isDataExpired(lastDt)) {
            String govPctData = downloadFREDData("A822RL1Q225SBEA", lastDt);
            insertEconomicDataIntoDB(GOV_PCT, govPctData);
        }
        
        //PERSONAL CONS - Value
        final String PERS_VAL = "PERS-VAL";
        lastDt = getEconomicData_UpdateDate(PERS_VAL);
        if (isDataExpired(lastDt)) {
            String persValueData = downloadFREDData("PCEC", lastDt);
            insertEconomicDataIntoDB(PERS_VAL, persValueData);
        }
        
        //PERSONAL CONS - Percent
        final String PERS_PCT = "PERS-PCT";
        lastDt = getEconomicData_UpdateDate(PERS_PCT);
        if (isDataExpired(lastDt)) {
            String persPctData = downloadFREDData("DPCERL1Q225SBEA", lastDt);
            insertEconomicDataIntoDB(PERS_PCT, persPctData);
        }
        
        //GROSS PRIV INV - Value
        final String PRIVINV_VAL = "PRIVINV-VAL";
        lastDt = getEconomicData_UpdateDate(PRIVINV_VAL);
        if (isDataExpired(lastDt)) {
            String privInvValueData = downloadFREDData("GPDI", lastDt);
            insertEconomicDataIntoDB(PRIVINV_VAL, privInvValueData);
        }
        
        //GROSS PRIV INV - Percent
        final String PRIVINV_PCT = "PRIVINV-PCT";
        lastDt = getEconomicData_UpdateDate(PRIVINV_PCT);
        if (isDataExpired(lastDt)) {
            String privInvPctData = downloadFREDData("A006RL1Q225SBEA", lastDt);
            insertEconomicDataIntoDB(PRIVINV_PCT, privInvPctData);
        }
        
        //Unemployment
        final String UNEMPLOYMENT = "UNEMPLOY";
        lastDt = getEconomicData_UpdateDate(UNEMPLOYMENT);
        if (isDataExpired(lastDt)) {
            String unemploymentData = downloadFREDData("UNRATE", lastDt);
            insertEconomicDataIntoDB(UNEMPLOYMENT, unemploymentData);
        }
        
        //Set the expiration date for the Economic Data Set
        setEconomicData_ValidDates();
        
        //END FRED ECONOMIC DATA****************************************************

        //New Home Prices
        lastDt = getAvgNewHomePrices_UpdateDate();
        if (isDataExpired(lastDt)) {
            String newHomePrices = downloadFREDData("ASPNHSUS", lastDt);
            insertNewHomePriceDataIntoDB(newHomePrices);
        }
        
        //Interest Rates - Prime
        final String PRIME = "PRIME";
        lastDt = getInterestRates_UpdateDate(PRIME);
        if (isDataExpired(lastDt)) {
            String primeRates = downloadFREDData("DPRIME", lastDt);
            insertInterestRatesIntoDB(PRIME, primeRates);
        }        

        //Interest Rates - Effective Funds Rate
        final String EFF_FUNDS_RT = "EF_FNDS_RT";
        lastDt = getInterestRates_UpdateDate(EFF_FUNDS_RT);
        if (isDataExpired(lastDt)) {
            String effFundsRate = downloadFREDData("DFF", lastDt);
            insertInterestRatesIntoDB(EFF_FUNDS_RT, effFundsRate);
        }        

        //Interest Rates - 6 Month T Bill
        final String SIX_MO_T_BILL = "6_MO_TBILL";
        lastDt = getInterestRates_UpdateDate(SIX_MO_T_BILL);
        if (isDataExpired(lastDt)) {
            String sixMoTBillRates = downloadFREDData("DTB6", lastDt);
            insertInterestRatesIntoDB(SIX_MO_T_BILL, sixMoTBillRates);
        }

        //Interest Rates - 5 Year T Rates
        final String FIVE_YR_T_RT = "5_YR_T_RT";
        lastDt = getInterestRates_UpdateDate(FIVE_YR_T_RT);
        if (isDataExpired(lastDt)) {
            String fiveYrTRates = downloadFREDData("DGS5", lastDt);
            insertInterestRatesIntoDB(FIVE_YR_T_RT, fiveYrTRates);
        }

        //Interest Rates - Credit Card Rates
        final String CREDIT_CARD_RT = "CREDIT_CRD";
        lastDt = getInterestRates_UpdateDate(CREDIT_CARD_RT);
        if (isDataExpired(lastDt)) {
            String cardRates = downloadFREDData("TERMCBCCALLNS", lastDt);
            insertInterestRatesIntoDB(CREDIT_CARD_RT, cardRates);
        }
        
        /*
        //Interest Rates - 5 Year ARM Rates
        final String FIVE_YR_ARM_RT = "5_YR_ARM";
        lastDt = getInterestRates_UpdateDate(FIVE_YR_ARM_RT);
        if (isDataExpired(lastDt)) {
            String fiveYrARM = downloadFREDData("ARM5YR", lastDt);
            insertInterestRatesIntoDB(FIVE_YR_ARM_RT, fiveYrARM);
        }
        */ 
        
        //Interest Rates - 6 Months LIBOR
        final String SIX_MO_LIBOR = "6_MO_LIBOR";
        lastDt = getInterestRates_UpdateDate(SIX_MO_LIBOR);
        if (isDataExpired(lastDt)) {
            String sixMoLIBOR = downloadFREDData("USD6MTD156N", lastDt);
            insertInterestRatesIntoDB(SIX_MO_LIBOR, sixMoLIBOR);
        }
        
        //Interest Rates - 5 Year Swaps
        final String FIVE_YR_SWAP = "5_YR_SWAP";
        lastDt = getInterestRates_UpdateDate(FIVE_YR_SWAP);
        if (isDataExpired(lastDt)) {
            String fiveYrSwaps = downloadFREDData("DSWP5", lastDt);
            insertInterestRatesIntoDB(FIVE_YR_SWAP, fiveYrSwaps);
        }
        //END FRED DATA-------------------------------------------------------------------------------------------------
        
        //START YAHOO DATA-------------------------------------------------------------------------------------------------
        //Global Stock Indexes
        final String SP500 = "S&P500";
        lastDt = getStockIndex_UpdateDate(SP500);
        if (isDataExpired(lastDt)) {
            //String spIndex = downloadData("YAHOO/INDEX_GSPC", lastDt); //Quandl Data
            String spIndex = downloadYahooData("%5EGSPC", lastDt);
            insertStockIndexDataIntoDB(SP500, spIndex);
        }

        final String DAX = "DAX";
        lastDt = getStockIndex_UpdateDate(DAX);
        if (isDataExpired(lastDt)) {
            //String daxIndex = downloadData("YAHOO/INDEX_GDAXI", lastDt); //Quandl Data
            String daxIndex = downloadYahooData("%5EGDAXI", lastDt);
            insertStockIndexDataIntoDB(DAX, daxIndex);
        }        

        final String HANGSENG = "HANGSENG";
        lastDt = getStockIndex_UpdateDate(HANGSENG);
        if (isDataExpired(lastDt)) {
            //String hangSengIndex = downloadData("YAHOO/INDEX_HSI", lastDt); //Quandl Data
            String hangSengIndex = downloadYahooData("%5EHSI", lastDt);
            insertStockIndexDataIntoDB(HANGSENG, hangSengIndex);
        }

        final String NIKEII = "NIKEII";
        lastDt = getStockIndex_UpdateDate(NIKEII);
        if (isDataExpired(lastDt)) {
            //String nikeiiIndex = downloadData("YAHOO/INDEX_N225", lastDt); //Quandl Data
            String nikeiiIndex = downloadYahooData("%5EN225", lastDt);
            insertStockIndexDataIntoDB(NIKEII, nikeiiIndex);
        }        

        //Sector Data===================================================================================
        final String ENERGY_SECTOR = "VGENX";
        lastDt = getMutualFund_UpdateDate(ENERGY_SECTOR);
        if (isDataExpired(lastDt)) {
            String fundData = downloadYahooData("VGENX", lastDt);
            insertMutualFundDataIntoDB(ENERGY_SECTOR, fundData);
        }
     
        final String REAL_ESTATE_SECTOR = "VGSLX";
        lastDt = getMutualFund_UpdateDate(REAL_ESTATE_SECTOR);
        if (isDataExpired(lastDt)) {
            String fundData = downloadYahooData("VGSLX", lastDt);
            insertMutualFundDataIntoDB(REAL_ESTATE_SECTOR, fundData);
        }        

        final String METALS_SECTOR = "VGPMX";
        lastDt = getMutualFund_UpdateDate(METALS_SECTOR);
        if (isDataExpired(lastDt)) {
            String fundData = downloadYahooData("VGPMX", lastDt);
            insertMutualFundDataIntoDB(METALS_SECTOR, fundData);
        }        

        final String HEALTH_CARE_SECTOR = "VGHCX";
        lastDt = getMutualFund_UpdateDate(HEALTH_CARE_SECTOR);
        if (isDataExpired(lastDt)) {
            String fundData = downloadYahooData("VGHCX", lastDt);
            insertMutualFundDataIntoDB(HEALTH_CARE_SECTOR, fundData);
        }        

        final String TECH_SECTOR = "XLK";
        lastDt = getMutualFund_UpdateDate(TECH_SECTOR);
        if (isDataExpired(lastDt)) {
            String fundData = downloadYahooData("XLK", lastDt);
            insertMutualFundDataIntoDB(TECH_SECTOR, fundData);
        }        

        final String FINANCE_SECTOR = "XLF";
        lastDt = getMutualFund_UpdateDate(FINANCE_SECTOR);
        if (isDataExpired(lastDt)) {
            String fundData = downloadYahooData("XLF", lastDt);
            insertMutualFundDataIntoDB(FINANCE_SECTOR, fundData);
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

    private List<StockFundamentals> getAnnualStockFundamentals(String ticker, Date lastDt) throws Exception {

        //Revenue
        String quandlCode = "SF1/" + ticker + "_REVENUE_ARY";
        String stockValues = downloadData(quandlCode, lastDt);

        //See if we have new data to process
        Map<Date, BigDecimal> revenueMap = new HashMap<>();
        if (stockValues.split("\n").length > 1) 
            insertFundamentalValuesIntoMap(stockValues, revenueMap);
        else 
            return null; //No new data
        
        //Net Income
        quandlCode = "SF1/" + ticker + "_NETINC_ARY";
        stockValues = downloadData(quandlCode, lastDt);
        Map<Date, BigDecimal> netIncMap = new HashMap<>();
        insertFundamentalValuesIntoMap(stockValues, netIncMap);
        
        //Net Income Common
        quandlCode = "SF1/" + ticker + "_NETINCCMN_ARY";
        stockValues = downloadData(quandlCode, lastDt);
        Map<Date, BigDecimal> netIncCmnMap = new HashMap<>();
        insertFundamentalValuesIntoMap(stockValues, netIncCmnMap);
        
        //EBITDA
        quandlCode = "SF1/" + ticker + "_EBITDA_ARY";
        stockValues = downloadData(quandlCode, lastDt);
        Map<Date, BigDecimal> ebitdaMap = new HashMap<>();
        insertFundamentalValuesIntoMap(stockValues, ebitdaMap);

        //EBT
        quandlCode = "SF1/" + ticker + "_EBT_ARY";
        stockValues = downloadData(quandlCode, lastDt);
        Map<Date, BigDecimal> ebtMap = new HashMap<>();
        insertFundamentalValuesIntoMap(stockValues, ebtMap);

        //NCFO
        quandlCode = "SF1/" + ticker + "_NCFO_ARY";
        stockValues = downloadData(quandlCode, lastDt);
        Map<Date, BigDecimal> ncfoMap = new HashMap<>();
        insertFundamentalValuesIntoMap(stockValues, ncfoMap);

        //DPS
        quandlCode = "SF1/" + ticker + "_DPS_ARY";
        stockValues = downloadData(quandlCode, lastDt);
        Map<Date, BigDecimal> dpsMap = new HashMap<>();
        insertFundamentalValuesIntoMap(stockValues, dpsMap);

        //EPS
        quandlCode = "SF1/" + ticker + "_EPS_ARY";
        stockValues = downloadData(quandlCode, lastDt);
        Map<Date, BigDecimal> epsMap = new HashMap<>();
        insertFundamentalValuesIntoMap(stockValues, epsMap);

        //EPS Diluted
        quandlCode = "SF1/" + ticker + "_EPSDIL_ARY";
        stockValues = downloadData(quandlCode, lastDt);
        Map<Date, BigDecimal> epsDilMap = new HashMap<>();
        insertFundamentalValuesIntoMap(stockValues, epsDilMap);

        //Interest Exposure
        quandlCode = "SF1/" + ticker + "_INTEXP_ARY";
        stockValues = downloadData(quandlCode, lastDt);
        Map<Date, BigDecimal> intExpMap = new HashMap<>();
        insertFundamentalValuesIntoMap(stockValues, intExpMap);

        //Free Cash Flow
        quandlCode = "SF1/" + ticker + "_FCF_ARY";
        stockValues = downloadData(quandlCode, lastDt);
        Map<Date, BigDecimal> fcfMap = new HashMap<>();
        insertFundamentalValuesIntoMap(stockValues, fcfMap);

        //Free Cash Flow - Per Share
        quandlCode = "SF1/" + ticker + "_FCFPS_ARY";
        stockValues = downloadData(quandlCode, lastDt);
        Map<Date, BigDecimal> fcfpsMap = new HashMap<>();
        insertFundamentalValuesIntoMap(stockValues, fcfpsMap);

        //Now create the complete record list - Loop through all available dates
        List<StockFundamentals> listFundamentals = new ArrayList<>();
        Set<Entry<Date, BigDecimal>> entrySet = revenueMap.entrySet();
        for (Entry<Date, BigDecimal> entry : entrySet) {
            StockFundamentals fund = new StockFundamentals();
            
            fund.setTicker(ticker);
            
            Date dt = entry.getKey();
            fund.setDate(dt);
            fund.setReportingDt(dt);

            BigDecimal revenue = entry.getValue();
            fund.setRevenue(revenue);
            
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
            
            BigDecimal intExp = intExpMap.get(dt);
            fund.setIntExposure(intExp);
            
            BigDecimal fcf = fcfMap.get(dt);
            fund.setFcf(fcf);
            
            BigDecimal fcfps = fcfpsMap.get(dt);
            fund.setFcfps(fcfps);
            
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
    
    private String downloadYahooData(final String CODE, final Date FROM_DT) throws Exception {
        
        String summary = String.format("Code: %s, From: %s", CODE, FROM_DT.toString());
        logger.Log("StockDataHandler", "downloadYahooData", summary, "", false);
        
        //Move the date ONE day ahead
        final long DAY_IN_MILLIS = 86400000;
        Date newFromDt = new Date();
        newFromDt.setTime(FROM_DT.getTime() + DAY_IN_MILLIS);
        
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        String dtStr = sdf.format(newFromDt);
        String sYear = dtStr.substring(0, 4);
        String sMonth = dtStr.substring(5, 7);
        sMonth = String.valueOf(Integer.parseInt(sMonth) - 1);
        String sDay = dtStr.substring(8, 10);
        
        String curDtStr = sdf.format(new Date());
        String eYear = curDtStr.substring(0, 4);
        String eMonth = curDtStr.substring(5, 7);
        eMonth = String.valueOf(Integer.parseInt(eMonth) - 1);
        String eDay = curDtStr.substring(8, 10);
        
        String yahooQuery = "http://real-chart.finance.yahoo.com/table.csv?s=" + CODE + "&a=" + sMonth + "&b=" + sDay + "&c=" + sYear + 
                                                                                        "&d=" + eMonth + "&e=" + eDay + "&f=" + eYear + "&g=d";
        
        StringBuilder responseStr = new StringBuilder();

        try {
            URL url = new URL(yahooQuery);

            //Try 3 times for a good response
            URLConnection conxn = null;
            boolean isGood = false;
            for (int i = 0; i < 3; i++) {
                conxn = url.openConnection();
                HttpURLConnection httpConxn = (HttpURLConnection)conxn;
                if (httpConxn.getResponseCode() == HttpURLConnection.HTTP_OK) {
                    isGood = true;
                    break;
                }
                
                Thread.sleep(5000);
                logger.Log("StockDataHandler", "downloadYahooData", "Retrying", url.toString(), true);
            }

            //Ensure we finally got a good response
            if (!isGood) 
                throw new Exception("Bad response from URL: " + url.toString());
            
            logger.Log("StockDataHandler", "downloadYahooData", "Downloading", yahooQuery, false);
            
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
            logger.Log("StockDataHandler", "downloadYahooData", "Exception", exc.toString(), true);
        }

        return responseStr.toString();
    }

    private String downloadFREDData(final String CODE, final Date FROM_DT) throws Exception {
        
        String summary = String.format("Code: %s, From: %s", CODE, FROM_DT.toString());
        logger.Log("StockDataHandler", "downloadFREDData", summary, "", false);
        
        //Move the date ONE day ahead
        final long DAY_IN_MILLIS = 86400000;
        Date newFromDt = new Date();
        newFromDt.setTime(FROM_DT.getTime() + DAY_IN_MILLIS);
        
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        String dtStr = sdf.format(newFromDt);
        String sYear = dtStr.substring(0, 4);
        String sMonth = dtStr.substring(5, 7);
        String sDay = dtStr.substring(8, 10);
        
        String fredQuery = "http://api.stlouisfed.org/fred/series/observations?series_id=" + CODE + "&api_key=" + FRED_KEY + "&observation_start=" + sYear + "-" + sMonth + "-" + sDay;

        StringBuilder responseStr = new StringBuilder();

        try {
            URL url = new URL(fredQuery);

            //Try 3 times for a good response
            URLConnection conxn = null;
            boolean isGood = false;
            for (int i = 0; i < 3; i++) {
                conxn = url.openConnection();
                HttpURLConnection httpConxn = (HttpURLConnection)conxn;
                if (httpConxn.getResponseCode() == HttpURLConnection.HTTP_OK) {
                    isGood = true;
                    break;
                }
                
                Thread.sleep(5000);
                logger.Log("StockDataHandler", "downloadFREDData", "Retrying", url.toString(), true);
            }

            //Ensure we finally got a good response
            if (!isGood) 
                throw new Exception("Bad response from URL: " + url.toString());
            
            logger.Log("StockDataHandler", "downloadFREDData", "Downloading", fredQuery, false);
            
            //Now Convert XML to CSV
            try (InputStream is = conxn.getInputStream()) {

                DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
                DocumentBuilder docBuilder = dbFactory.newDocumentBuilder();
                Document doc = docBuilder.parse(conxn.getInputStream());
                doc.getDocumentElement().normalize();

                NodeList nList = doc.getElementsByTagName("observation");
                int listSize = nList.getLength();
                for (int i = 0; i < listSize; i++) {

                    Node nNode = nList.item(i);
                    if (nNode.getNodeType() == Node.ELEMENT_NODE) {

                        Element eElement = (Element) nNode;
                        String date = eElement.getAttribute("date");
                        String value = eElement.getAttribute("value");
                        
                        responseStr.append(date).append(",").append(value).append("\n");
                    }
                }
            }
            
        } catch(Exception exc) {
            logger.Log("StockDataHandler", "downloadFREDData", "Exception", exc.toString(), true);
        }

        return responseStr.toString();
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
    
    private String downloadData(final String QUANDL_CODE, final Date FROM_DT) throws Exception {

        String summary = String.format("Code: %s, From: %s", QUANDL_CODE, FROM_DT.toString());
        logger.Log("StockDataHandler", "downloadData", summary, "", false);

        //Slow Down
        Thread.sleep(SVC_THROTTLE);
        
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

            //Try 3 times for a good response
            URLConnection conxn = null;
            boolean isGood = false;
            for (int i = 0; i < 3; i++) {
                conxn = url.openConnection();
                HttpURLConnection httpConxn = (HttpURLConnection)conxn;
                int httpResponseCode = httpConxn.getResponseCode();
                if (httpResponseCode == HttpURLConnection.HTTP_OK) {
                    isGood = true;
                    break;
                }
                
                Thread.sleep(5000);
                logger.Log("StockDataHandler", "downloadData", "Retrying, HTTP Code: " + httpResponseCode, url.toString(), false);
            }

            //Ensure we finally got a good response
            if (!isGood) 
                throw new Exception("Bad response from URL: " + url.toString());
            
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
