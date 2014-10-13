/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package StockData;

import Modeling.CostResults;
import Modeling.FuturePrice;
import Modeling.ModelTypes;
import static Modeling.ModelTypes.LOGIST_REG;
import static Modeling.ModelTypes.RAND_FORST;
import static Modeling.ModelTypes.SVM;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import com.mysql.jdbc.jdbc2.optional.*;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
    
    final String CONF_FILE = "settings.conf";
    final String STOCK_TICKERS_PATH;

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
            
            QUANDL_AUTH_TOKEN = p.getProperty("quandl_auth_token");
            QUANDL_BASE_URL = p.getProperty("quandl_base_url");
            
            BEA_USER_ID = p.getProperty("bea_user_id");
        }
    }
    
    private Connection getDBConnection() throws Exception {
        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setServerName(MYSQL_SERVER_HOST);
        dataSource.setPort(Integer.parseInt(MYSQL_SERVER_PORT));
        dataSource.setDatabaseName(MYSQL_SERVER_DB);
        return dataSource.getConnection(MYSQL_SERVER_LOGIN, MYSQL_SERVER_PASSWORD);
    }

    public void computeMovingAverages(final int DAYS_BACK) throws Exception {
        List<StockTicker> tickers = getAllStockTickers(); 
        
        //Iterate through all stock tickers
        for (StockTicker stockTicker : tickers) {
            
            //Iterate through the stock prices
            List<MovingAverage> listMAs = new ArrayList<>();
            List<StockPrice> priceList = getAllStockQuotes(stockTicker.getTicker(), DAYS_BACK);
            Queue<StockPrice> fiveDayMAQueue = new LinkedList<>();
            Queue<StockPrice> twentyDayMAQueue = new LinkedList<>();
            Queue<StockPrice> sixtyDayMAQueue = new LinkedList<>();
            BigDecimal fiveDayMA = null;
            BigDecimal twentyDayMA = null;
            BigDecimal sixtyDayMA = null;
            final int FIVE = 5;
            final int TWENTY = 20;
            final int SIXTY = 60;

            System.out.println("Method: computeMovingAverages, Ticker: " + stockTicker.getTicker());
            
            //Look through a stock's price history
            for (StockPrice price : priceList) {

                //5 Day MA
                fiveDayMAQueue.add(price);
                if (fiveDayMAQueue.size() >= FIVE) {
                    BigDecimal sum = new BigDecimal(0.0);
                    for (StockPrice sp : fiveDayMAQueue) {
                        sum = sum.add(sp.getPrice());
                    }
                
                    fiveDayMA = sum.divide(new BigDecimal(FIVE), 2, RoundingMode.HALF_UP);
                    fiveDayMAQueue.remove();
                }
                    
                //20 Day MA
                twentyDayMAQueue.add(price);
                if (twentyDayMAQueue.size() >= TWENTY) {
                    BigDecimal sum = new BigDecimal(0.0);
                    for (StockPrice sp : twentyDayMAQueue) {
                        sum = sum.add(sp.getPrice());
                    }
                
                    twentyDayMA = sum.divide(new BigDecimal(TWENTY), 2, RoundingMode.HALF_UP);
                    twentyDayMAQueue.remove();
                }
                
                //60 Day MA
                sixtyDayMAQueue.add(price);
                if (sixtyDayMAQueue.size() >= SIXTY) {
                    BigDecimal sum = new BigDecimal(0.0);
                    for (StockPrice sp : sixtyDayMAQueue) {
                        sum = sum.add(sp.getPrice());
                    }
                
                    sixtyDayMA = sum.divide(new BigDecimal(SIXTY), 2, RoundingMode.HALF_UP);
                    sixtyDayMAQueue.remove();
                }
                
                //Save MAs to list
                MovingAverage avg = new MovingAverage(stockTicker.getTicker(), price.getDate(), fiveDayMA, twentyDayMA, sixtyDayMA);
                listMAs.add(avg);
                
                
            } //End of PriceList loop

            //Save MAs to DB
            setMovingAverages(listMAs);
            System.gc();
        } //End of ticker loop
    }

    public void computeStockQuoteSlopes(final int DAYS_BACK) throws Exception {
        List<StockTicker> tickers = getAllStockTickers(); 
        
        //Iterate through all stock tickers
        for (StockTicker stockTicker : tickers) {
            
            //Iterate through the 5 Day Moving Averages
            List<StockPrice> priceList = getAll5DayMAs(stockTicker.getTicker(), DAYS_BACK);
            List<StockQuoteSlope> slopeList = new ArrayList<>();
            BigDecimal fiveDayDelta = null;
            BigDecimal twentyDayDelta = null;
            BigDecimal sixtyDayDelta = null;
            BigDecimal curMA = null;
            BigDecimal slope = null;
            final int FIVE = 5;
            final int TWENTY = 20;
            final int SIXTY = 60;

            System.out.println("Method: computeStockQuoteSlopes, Ticker: " + stockTicker.getTicker());

            //Look through the 5 Day MAs
            for (int i = 0; i < priceList.size() - FIVE; i++) {
                curMA = priceList.get(i).getPrice();
                fiveDayDelta = priceList.get(i + FIVE).getPrice();
                
                slope = curMA.add(fiveDayDelta.negate()).divide(new BigDecimal(FIVE), 5, RoundingMode.HALF_UP);
                StockQuoteSlope sqSlope = new StockQuoteSlope(stockTicker.getTicker(), priceList.get(i).getDate(), FIVE, slope);
                slopeList.add(sqSlope);
            } 
            
            //Look through the 20 Day MAs
            for (int i = 0; i < priceList.size() - TWENTY; i++) {
                curMA = priceList.get(i).getPrice();
                twentyDayDelta = priceList.get(i + TWENTY).getPrice();

                slope = curMA.add(twentyDayDelta.negate()).divide(new BigDecimal(TWENTY), 5, RoundingMode.HALF_UP);
                StockQuoteSlope sqSlope = new StockQuoteSlope(stockTicker.getTicker(), priceList.get(i).getDate(), TWENTY, slope);
                slopeList.add(sqSlope);
            } 
            
            //Look through the 60 Day MAs
            for (int i = 0; i < priceList.size() - SIXTY; i++) {
                curMA = priceList.get(i).getPrice();
                sixtyDayDelta = priceList.get(i + SIXTY).getPrice();

                slope = curMA.add(sixtyDayDelta.negate()).divide(new BigDecimal(SIXTY), 5, RoundingMode.HALF_UP);
                StockQuoteSlope sqSlope = new StockQuoteSlope(stockTicker.getTicker(), priceList.get(i).getDate(), SIXTY, slope);
                slopeList.add(sqSlope);
            } 

            //Send data to DB
            setStockQuoteSlope(slopeList);
            
            System.gc();
        } //End of ticker loop
    }

    private void setStockQuoteSlope(List<StockQuoteSlope> slopeList) throws Exception {

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
            System.out.println("Exception in setStockQuoteSlope");
            throw exc;
        }
    }
    
    public void insertStockPredictions(List<PredictionValues> predictionList) throws Exception {

        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Insert_StockPrediction(?, ?, ?, ?, ?, ?)}")) {

            conxn.setAutoCommit(false);
            
            int dupCount = 0;
            Map<Date, Date> map = new HashMap<>();
            for (PredictionValues p : predictionList) {

                java.sql.Date dt = new java.sql.Date(p.getDate().getTime());
                java.sql.Date projDt = new java.sql.Date(p.getProjectedDate().getTime());
                
                //Dedup Check
                if (map.containsKey(dt)) {
                    dupCount++;
                    continue;
                }
                else 
                    map.put(dt, dt);

                if (dupCount > 0)
                    System.out.println("Method: setStockPredictions, Dup Count: " + dupCount);
                
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
            System.out.println("Exception in setStockPredictions");
            throw exc;
        }

    }
    
    private void setMovingAverages(List<MovingAverage> listMAs) throws Exception {

        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Update_StockQuote(?, ?, ?, ?, ?)}")) {

            conxn.setAutoCommit(false);
            
            for (MovingAverage ma : listMAs) {

                stmt.setString(1, ma.getStockTicker());

                java.sql.Date sqlDate = new java.sql.Date(ma.getDate().getTime());
                stmt.setDate(2, sqlDate);
                stmt.setBigDecimal(3, ma.getFiveDayMA());
                stmt.setBigDecimal(4, ma.getTwentyDayMA());
                stmt.setBigDecimal(5, ma.getSixtyDayMA());
                stmt.addBatch();
            }
            
            stmt.executeBatch();
            conxn.commit();

        } catch(Exception exc) {
            System.out.println("Exception in updateMovingAverages");
            throw exc;
        }
    }

    public FuturePrice getTargetValueRegressionPredictions(String stock, Date date) throws Exception {

        FuturePrice fp;
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Retrieve_Predictions_Regression_PctForecast(?, ?)}")) {

            stmt.setString(1, stock);
            
            java.sql.Date dt = new java.sql.Date(date.getTime());
            stmt.setDate(2, dt);

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
            System.out.println("Exception in getTargetValueRegressionPredictions");
            throw exc;
        }
        
        return fp;
    }
    
    public List<String> getPostiveClassificationPredictions(Date date) throws Exception {

        List<String> stockList = new ArrayList<>();

        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Retrieve_Predictions_Classification_Upward (?)}")) {

            java.sql.Date dt = new java.sql.Date(date.getTime());
            stmt.setDate(1, dt);

            ResultSet rs = stmt.executeQuery();
            
            String stock;
            while(rs.next()) {
                stock = rs.getString(1);
                stockList.add(stock);
            }
                
        } catch(Exception exc) {
            System.out.println("Exception in getPostiveClassificationPredictions");
            throw exc;
        }
        
        return stockList;
    }
    
    private List<StockPrice> getAllStockQuotes(String ticker, int daysBack) throws Exception {

        List<StockPrice> stockPrices = new ArrayList<>();

        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_RetrieveAll_StockQuotes(?, ?)}")) {
            
            stmt.setString(1, ticker);
            stmt.setInt(2, daysBack);

            ResultSet rs = stmt.executeQuery();
            
            StockPrice price;
            while(rs.next()) {
                price = new StockPrice(rs.getDate(1), rs.getBigDecimal(2), rs.getBigDecimal(3));
                stockPrices.add(price);
            }
                
        } catch(Exception exc) {
            System.out.println("Exception in getAllStockQuotes");
            throw exc;
        }
        
        return stockPrices;
    }

    public List<PredictionValues> getStockBackTesting(String ticker, String modelType, Date fromDate, Date toDate) throws Exception {

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
            System.out.println("Exception in getAllStockQuotes");
            throw exc;
        }
    }

    public List<Weight> getWeights(String ticker, ModelTypes modelType) throws Exception {
        
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
            System.out.println("Exception in getWeights");
            throw exc;
        }
    }
    
    private List<StockPrice> getAll5DayMAs(String ticker, int daysBack) throws Exception {

        List<StockPrice> stockPrices = new ArrayList<>();

        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_RetrieveAll_5DayMovingAvgs(?, ?)}")) {
            
            stmt.setString(1, ticker);
            stmt.setInt(2, daysBack);

            ResultSet rs = stmt.executeQuery();
            
            StockPrice price;
            while(rs.next()) {
                price = new StockPrice(rs.getDate(1), rs.getBigDecimal(2));
                stockPrices.add(price);
            }
                
        } catch(Exception exc) {
            System.out.println("Exception in getAll5DayMAs");
            throw exc;
        }
        
        return stockPrices;
    }
    
    public String getAllStockFeaturesFromDB(String stockTicker, int daysInFuture, ModelTypes approach, Date fromDt, Date toDt) throws Exception {
        
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
            System.out.println("Method - getAllStockFeaturesFromDB: Stock: " + stockTicker + ", Feature Count = " + colCount);

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
            while(rs.next()) {

                String s;
                for (int i = 1; i <= colCount; i++) {
                    double d = rs.getDouble(i);
                    
                    if (i < colCount)
                        s = String.valueOf(d) + ",";
                    else
                        s = String.valueOf(d);
                    
                    dataExamples.append(s);
                }
                dataExamples.append("\n");

            }
            
        } catch(Exception exc) {
            System.out.println("Exception in getAllStockFeaturesFromDB");
            throw exc;
        }
        
        return dataExamples.toString();
    }
    
    private void insertStockIndexDataIntoDB(String stockIndex, String indexPrices) throws Exception {
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
                    System.out.println("Method: insertStockIndexDataIntoDB, Index: " + stockIndex + ", Row: " + i);
                }
            } //End For
            
            //Execute DB commands
            stmt.executeBatch();
            conxn.commit();
            
        } catch(Exception exc) {
            System.out.println("Method: insertStockIndexDataIntoDB, Index: " + stockIndex + ", Description: " + exc);
            throw exc;
        }
        
    }

    private void insertEnergyPricesIntoDB(String energyCode, String energyPrices) throws Exception {
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
                    System.out.println("Method: insertEnergyPricesIntoDB, EnergyCode: " + energyCode + ", Row: " + i);
                }
            } //End For
            
            //Send commands to DB
            stmt.executeBatch();
            conxn.commit();
            
        } catch(Exception exc) {
            System.out.println("Method: insertEnergyPricesIntoDB, EnergyCode: " + energyCode + ", Description: " + exc);
            throw exc;
        }
        
    }

    private void insertCurrencyRatiosIntoDB(String currency, String currencyRatios) throws Exception {
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
                    System.out.println("Method: insertCurrencyRatiosIntoDB, Currency: " + currency + ", Row: " + i);
                }
            } //End for
            
            //Send commands to DB
            stmt.executeBatch();
            conxn.commit();

        } catch(Exception exc) {
            System.out.println("Method: insertCurrencyRatiosIntoDB, Currency: " + currency + ", Description: " + exc);
            throw exc;
        }
        
    }

    public void setStockBacktestingIntoDB(List<BacktestingResults> listResults) throws Exception {

        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Insert_BackTesting (?, ?, ?, ?, ?, ?, ?)}")) {
            
            conxn.setAutoCommit(false);
            
            for (BacktestingResults r : listResults) {
                java.sql.Date startDt = new java.sql.Date(r.getStartDt().getTime());
                java.sql.Date endDt = new java.sql.Date(r.getEndDt().getTime());
                
                stmt.setString(1, r.getTicker());
                stmt.setString(2, r.getModelType());
                stmt.setDate(3, startDt);
                stmt.setDate(4, endDt);
                stmt.setInt(5, r.getNumTrades());
                stmt.setBigDecimal(6, r.getAssetValuePctChg());
                stmt.setBigDecimal(7, r.getBuyAndHoldPctChg());
                
                stmt.addBatch();
            }
            
            //Send commands to DB
            stmt.executeBatch();
            conxn.commit();

        } catch(Exception exc) {
            System.out.println("Method: insertStockBacktestingIntoDB, Description: " + exc);
            throw exc;
        }
        
    }

    
    private void insertPreciousMetalsPricesIntoDB(String metal, String goldPrices) throws Exception {
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
                    System.out.println("Method: insertPreciousMetalsPricesIntoDB, Metal: " + metal + ", Row: " + i);
                }
            } //End For
            
            //Send Commands to DB
            stmt.executeBatch();
            conxn.commit();

        } catch(Exception exc) {
            System.out.println("Method: insertPreciousMetalsPricesIntoDB, Metal: " + metal + ", Description: " + exc);
            throw exc;
        }
        
    }

    private void removeAllBadData() throws Exception {
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_RemoveAll_BadData()}")) {
            stmt.executeUpdate();
        }
    }
    
    private void insertInflationDataIntoDB(String cpiInflation) throws Exception {
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
                    System.out.println("Method: insertInflationDataIntoDB, Row: " + i);
                }
            } //End For
            
            //Send Command to DB
            stmt.executeBatch();
            conxn.commit();
            
        } catch(Exception exc) {
            System.out.println("Method: insertInflationDataIntoDB, Description: " + exc);
            throw exc;
        }
    }

    private void insertGDPDataIntoDB(List<BEA_Data> listData) throws Exception {

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
            System.out.println("Method: insertGDPDataIntoDB, Desc:" + exc);
            throw exc;
        }
    }
    
    private void insertUnemploymentRatesIntoDB(String unemploymentRates) throws Exception {
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
                    System.out.println("Method: insertUnemploymentRatesIntoDB, Row: " + i);
                }
            }
        } catch(Exception exc) {
            System.out.println("Method: insertUnemploymentRatesIntoDB, Description: " + exc);
            throw exc;
        }
    }
    
    private void insertInterestRatesIntoDB(final String RATE_TYPE, String primeRates) throws Exception {
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
                    System.out.println("Method: insertInterestRatesIntoDB, Row: " + i + ", Desc: " + exc);
                }
            } //End For
            
            //Send Commands to DB
            stmt.executeBatch();
            conxn.commit();
            
        } catch(Exception exc) {
            System.out.println("Method: insertInterestRatesIntoDB, Description: " + exc);
            throw exc;
        }
    }
    
    private void insertEconomicDataIntoDB(String indicator, String econData) throws Exception {
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
                    System.out.println("Method: insertEconomicDataIntoDB, Row: " + i);
                }
            } //End For
            
            //Send Commands to DB
            stmt.executeBatch();
            conxn.commit();
            
        } catch(Exception exc) {
            System.out.println("Method: insertEconomicDataIntoDB, Description: " + exc);
            throw exc;
        }
    }
    
    private void insertMortgageDataIntoDB(String thirtyYrMtgRates) throws Exception {
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
                    System.out.println("Method: insertMortgateDataIntoDB, Row: " + i);
                }
            } //End For
            
            //Send Commands to DB
            stmt.executeBatch();
            conxn.commit();
            
        } catch(Exception exc) {
            System.out.println("Method: insertMortgateDataIntoDB, Description: " + exc);
            throw exc;
        }
    }
    
    private void insertNewHomePriceDataIntoDB(String newHomePrices) throws Exception {
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
                    System.out.println("Method: insertNewHomePriceDataIntoDB, Row: " + i);
                }
            } //End For
            
            //Send Commands to DB
            stmt.executeBatch();
            conxn.commit();
            
        } catch(Exception exc) {
            System.out.println("Method: insertNewHomePriceDataIntoDB, Description: " + exc);
            throw exc;
        }
    }
    
    private void insertStockPricesIntoDB(String stockTicker, String stockValues) throws Exception {
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
                    System.out.println("Method: insertStockPricesIntoDB, Ticker: " + stockTicker + "Row: " + i);
                }

            } //End for
            
            stmt.executeBatch();
            conxn.commit();
            
        } catch(Exception exc) {
            System.out.println("Method: insertStockPricesIntoDB, Description: " + exc);
            throw exc;
        }
    }

    private void insertStockFundamentalsIntoDB(List<StockFundamentals_Annual> listStockFund) throws Exception {

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
            System.out.println("Method: insertStockFundamentalsIntoDB, Description: " + exc);
            throw exc;
        }
    }
    
    public List<StockTicker> getAllStockTickers() throws Exception {

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
            throw exc;
        }
        
        return tickerList;
    }
    
    public void loadStockTickers() {
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
            System.out.println("Row:" + i + ", " + exc);
        }

    }

    private void insertStockTickersIntoDB(String[] cells) throws Exception {
        if (cells.length != 3)
            throw new Exception("Method: insertStockTickersIntoDB, invalid number of paramaters");
        
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Insert_StockTicker (?, ?, ?)}")) {
            
            stmt.setString(1, cells[0]);
            stmt.setString(2, cells[1]);
            stmt.setString(3, cells[2]);

            stmt.executeUpdate();
            
        } catch (Exception exc) {
            throw exc;
        }
    }

    
    private void insertStockFundamentals_Annual_IntoDB(List<StockFundamentals_Annual> listStockFund) throws Exception {
        
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
            System.out.println("Method: insertStockFundamentals_Annual_IntoDB, Description: " + exc);
            throw exc;
        }
    }

    private void insertStockFundamentals_Quarter_IntoDB(List<StockFundamentals_Quarter> listStockFund) throws Exception {
        
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
            System.out.println("Method: insertStockFundamentals_Quarter_IntoDB, Description: " + exc);
            throw exc;
        }

    }

    
    public Date get30YrMortgageRates_UpdateDate() throws Exception {
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
            System.out.println("Exception in get30YrMortgageRates_UpdateDate");
            throw exc;
        }
    }

    public Date getAvgNewHomePrices_UpdateDate() throws Exception {
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
            System.out.println("Exception in getAvgNewHomePrices_UpdateDate");
            throw exc;
        }
    }
    
    public StockQuote getStockQuote(String ticker, Date date) throws Exception {

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
            System.out.println("Exception in getStockQuote");
            throw exc;
        }
        
        return quote;
    }
    
    public Date getCPI_UpdateDate() throws Exception {
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
            System.out.println("Exception in getCPI_UpdateDate");
            throw exc;
        }
    }

    public Date getEconomicData_UpdateDate(String econInd) throws Exception {
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
            System.out.println("Exception in getEconomicData_UpdateDate");
            throw exc;
        }
    }

    public Date getCurrencyRatios_UpdateDate(String currencyCode) throws Exception {
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
            System.out.println("Exception in getCurrencyRatios_UpdateDate");
            throw exc;
        }
    }

    public Date getEnergyPrices_UpdateDate(String energyCode) throws Exception {
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
            System.out.println("Exception in getEnergyPrices_UpdateDate");
            throw exc;
        }
    }

    public Quarter getBEA_UpdateDate() throws Exception {
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
            System.out.println("Exception in getBEA_UpdateDate");
            throw exc;
        }
    }

    public Date getStockFundamentals_UpdateDate(String stockTicker, String indicator) throws Exception {
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
            System.out.println("Exception in getStockFundamentals_UpdateDate");
            throw exc;
        }
    }

    public Date getInterestRates_UpdateDate(final String INT_RATE_TYPE) throws Exception {
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
            System.out.println("Exception in getInterestRates_UpdateDate");
            throw exc;
        }
    }

    public Date getPreciousMetals_UpdateDate(String metalCode) throws Exception {
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
            System.out.println("Exception in getPreciousMetals_UpdateDate");
            throw exc;
        }
    }
    
    public Date getStockIndex_UpdateDate(String stockIndex) throws Exception {
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
            System.out.println("Exception in getStockIndex_UpdateDate");
            throw exc;
        }
    }

    public Date getStockQuote_UpdateDate(String stockQuote) throws Exception {
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
            System.out.println("Exception in getStockQuote_UpdateDate");
            throw exc;
        }
    }

    public void setStockFundamentals_Quarter_ValidDates() throws Exception {
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Update_Stock_Fundamentals_Quarter_ValidDates ()}")) {

            stmt.executeUpdate();
            
        } catch (Exception exc) {
            System.out.println("Exception in setStockFundamentals_Quarter_ValidDates");
            throw exc;
        }
    }

    public void setStockFundamentals_Annual_PctChg() throws Exception {
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Update_AnnualFundamentals_PctChg ()}")) {

            stmt.executeUpdate();
            
        } catch (Exception exc) {
            System.out.println("Exception in setStockFundamentals_Annual_PctChg");
            throw exc;
        }
    }
    
    public void setStockFundamentals_Quarter_PctChg() throws Exception {
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Update_QuarterlyFundamentals_PctChg ()}")) {

            stmt.executeUpdate();
            
        } catch (Exception exc) {
            System.out.println("Exception in setStockFundamentals_Quarter_PctChg");
            throw exc;
        }
    }

    public Date getUnemployment_UpdateDate() throws Exception {
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
            System.out.println("Exception in getUnemployment_UpdateDate");
            throw exc;
        }
    }
    
    private boolean isDataExpired(Date dt) {

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
    
    public void downloadAllStockData() throws Exception {

        //M2 - Money Supply
        Date lastDt;
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
            String usdJpy = downloadData("QUANDL/USDJPY", lastDt);
            insertCurrencyRatiosIntoDB(JAPAN, usdJpy);
        }        
        
        final String AUSTRALIA = "AUD";
        lastDt = getCurrencyRatios_UpdateDate(AUSTRALIA);
        if (isDataExpired(lastDt)) {
            String usdAud = downloadData("QUANDL/USDAUD", lastDt);
            insertCurrencyRatiosIntoDB(AUSTRALIA, usdAud);
        }        
        
        final String EURO = "EUR";
        lastDt = getCurrencyRatios_UpdateDate(EURO);
        if (isDataExpired(lastDt)) {
            String usdEur = downloadData("QUANDL/USDEUR", lastDt);
            insertCurrencyRatiosIntoDB(EURO, usdEur);
        }        

        //Energy Prices
        final String CRUDE_OIL = "CRUDE-OIL";
        lastDt = getEnergyPrices_UpdateDate(CRUDE_OIL);
        if (isDataExpired(lastDt)) {
            String crudeOilPrices = downloadData("OFDP/FUTURE_CL1", lastDt);
            insertEnergyPricesIntoDB(CRUDE_OIL, crudeOilPrices);
        }

        final String NATURAL_GAS = "NATURL-GAS";
        lastDt = getEnergyPrices_UpdateDate(NATURAL_GAS);
        if (isDataExpired(lastDt)) {
            String naturalGasPrices = downloadData("OFDP/FUTURE_NG1", lastDt);
            insertEnergyPricesIntoDB(NATURAL_GAS, naturalGasPrices);
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
            String nikeiiIndex = downloadData("NIKKEI/INDEX", lastDt);
            insertStockIndexDataIntoDB(NIKEII, nikeiiIndex);
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

        //Stock Quotes
        List<StockTicker> listOfAllStocks = getAllStockTickers();
        for (StockTicker st : listOfAllStocks) {
            lastDt = getStockQuote_UpdateDate(st.getTicker());
            if (isDataExpired(lastDt)) {
                String stockValues = downloadData(st.getQuandlCode(), lastDt);
                insertStockPricesIntoDB(st.getTicker(), stockValues);
            }        
        }

        //Fundamentals - Annual
        MorningstarData mstar = new MorningstarData();
        List<StockFundamentals_Annual> listStockFundAnnual = new ArrayList<>();
        for (StockTicker st : listOfAllStocks) {
            StockFundamentals_Annual fundamentals = mstar.getStockFundamentals_Annual(st);
            if (fundamentals != null) 
                listStockFundAnnual.add(fundamentals);
        }
        insertStockFundamentals_Annual_IntoDB(listStockFundAnnual);
        setStockFundamentals_Annual_PctChg();

        //Fundamentals - Quarterly
        List<StockFundamentals_Quarter> listStockFundQtr = new ArrayList<>();
        for (StockTicker st : listOfAllStocks) {
            StockFundamentals_Quarter fundamentals = mstar.getStockFundamentals_Quarterly(st);
            if (fundamentals != null) 
                listStockFundQtr.add(fundamentals);
        }
        insertStockFundamentals_Quarter_IntoDB(listStockFundQtr);
        setStockFundamentals_Quarter_ValidDates();
        setStockFundamentals_Quarter_PctChg();

        //Remove bad data
        removeAllBadData();
    }

    private List<BEA_Data> downloadBEAData() throws Exception {
        
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

            System.out.println(url);
            
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
            System.out.println(exc);
        }
        
        return list;
    }
    
    public void setModelValues(String ticker, String modelType, int daysForecast, double accuracy) throws Exception {

        java.sql.Date dt = new java.sql.Date(new Date().getTime());
        
        try (Connection conxn = getDBConnection();
             CallableStatement stmtModel = conxn.prepareCall("{call sp_Insert_Model_Runs (?, ?, ?, ?)}")) {

            stmtModel.setString(1, ticker);
            stmtModel.setString(2, modelType);
            stmtModel.setInt(3, daysForecast);
            stmtModel.setDouble(4, accuracy);
            stmtModel.executeUpdate();

        } catch(Exception exc) {
            System.out.println("Method: insertWeightsIntoDB, Description: " + exc);
            throw exc;
        }
    }
    
    public String downloadCensusData() {

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
            System.out.println(exc);
        }

        return responseStr.toString();
    }
    
    private String downloadData(final String QUANDL_CODE, final Date fromDt) {

        //Move the date ONE day ahead
        final long DAY_IN_MILLIS = 86400000;
        fromDt.setTime(fromDt.getTime() + DAY_IN_MILLIS);
        
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String dtStr = sdf.format(fromDt);
        
        String quandlQuery = QUANDL_BASE_URL + QUANDL_CODE + ".csv?auth_token=" + QUANDL_AUTH_TOKEN + "&trim_start=" + dtStr + "&sort_order=asc";
        
        StringBuilder responseStr = new StringBuilder();

        try {
            URL url = new URL(quandlQuery);
            URLConnection conxn = url.openConnection();

            System.out.println("Downloading: " + quandlQuery);
            
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
            System.out.println(exc);
        }

        return responseStr.toString();
    }
}
