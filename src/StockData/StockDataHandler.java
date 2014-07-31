/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package StockData;

import Modeling.ModelApproach;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import com.mysql.jdbc.jdbc2.optional.*;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.math.BigDecimal;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Queue;

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

    public void computeMovingAverages() throws Exception {
        List<StockTicker> tickers = getAllStockTickers();
        
        //Iterate through all stock tickers
        for (StockTicker stockTicker : tickers) {
            
            //Iterate through the stock prices
            List<StockPrice> priceList = getAllStockQuotes(stockTicker.getTicker());
            Queue<StockPrice> fiveDayMAQueue = new LinkedList<>();
            Queue<StockPrice> twentyDayMAQueue = new LinkedList<>();
            Queue<StockPrice> sixtyDayMAQueue = new LinkedList<>();
            BigDecimal fiveDayMA = null;
            BigDecimal twentyDayMA = null;
            BigDecimal sixtyDayMA = null;
            
            for (StockPrice price : priceList) {

                //5 Day MA
                fiveDayMAQueue.add(price);
                if (fiveDayMAQueue.size() >= 5) {
                    BigDecimal sum = new BigDecimal(0.0);
                    for (StockPrice sp : fiveDayMAQueue) {
                        sum = sum.add(sp.getPrice());
                    }
                
                    fiveDayMA = sum.divide(new BigDecimal(5));
                    fiveDayMAQueue.remove();
                }
                    
                //20 Day MA
                twentyDayMAQueue.add(price);
                if (twentyDayMAQueue.size() >= 20) {
                    BigDecimal sum = new BigDecimal(0.0);
                    for (StockPrice sp : twentyDayMAQueue) {
                        sum = sum.add(sp.getPrice());
                    }
                
                    twentyDayMA = sum.divide(new BigDecimal(20));
                    twentyDayMAQueue.remove();
                }
                
                //60 Day MA
                sixtyDayMAQueue.add(price);
                if (sixtyDayMAQueue.size() >= 60) {
                    BigDecimal sum = new BigDecimal(0.0);
                    for (StockPrice sp : sixtyDayMAQueue) {
                        sum = sum.add(sp.getPrice());
                    }
                
                    sixtyDayMA = sum.divide(new BigDecimal(60));
                    sixtyDayMAQueue.remove();
                }
                
                //Save MAs to DB
                this.setMovingAverages(stockTicker.getTicker(), price.getDate(), fiveDayMA, twentyDayMA, sixtyDayMA);
                
            } //End of PriceList loop
        } //End of ticker loop
    }

    private void setMovingAverages(String ticker, Date date, BigDecimal fiveDayMA, BigDecimal twentyDayMA, BigDecimal sixtyDayMA) throws Exception {

        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Update_StockQuote(?, ?, ?, ?, ?)}")) {

            java.sql.Date sqlDate = new java.sql.Date(date.getTime());
            
            stmt.setString(1, ticker);
            stmt.setDate(2, sqlDate);
            stmt.setBigDecimal(3, fiveDayMA);
            stmt.setBigDecimal(4, twentyDayMA);
            stmt.setBigDecimal(5, sixtyDayMA);

            stmt.executeUpdate();

        } catch(Exception exc) {
            System.out.println("Exception in updateMovingAverages");
            throw exc;
        }
    }
    
    private List<StockPrice> getAllStockQuotes(String ticker) throws Exception {

        List<StockPrice> stockPrices = new ArrayList<>();

        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_RetrieveAll_StockQuotes(?)}")) {
            
            stmt.setString(1, ticker);

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
    
    public List<double[]> getAllStockFeaturesFromDB(String stockTicker, int daysInFuture, ModelApproach approach) throws Exception {
        
        List<double[]> stockFeatureMatrix = new ArrayList<>();

        try (Connection conxn = getDBConnection()) {
            CallableStatement stmt = null;
            
            if (approach == ModelApproach.VALUES)
                stmt = conxn.prepareCall("{call sp_Retrieve_CompleteFeatureSetForStockTicker_ProjectedValue(?, ?)}");
            else if (approach == ModelApproach.CLASSIFICATION)
                stmt = conxn.prepareCall("{call sp_Retrieve_CompleteFeatureSetForStockTicker_Classification(?, ?)}");
            
            stmt.setString(1, stockTicker);
            stmt.setInt(2, daysInFuture);

            ResultSet rs = stmt.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            int colCount = metaData.getColumnCount();
            
            System.out.println("Method - getAllStockFeaturesFromDB: Stock: " + stockTicker + ", Feature Count = " + colCount);
            
            double[] featureArray;
            while(rs.next()) {
                featureArray = new double[colCount];
                for (int i = 0; i < colCount; i++) {
                    featureArray[i] = rs.getDouble(i + 1);
                }
                
                stockFeatureMatrix.add(featureArray);
            }
        } catch(Exception exc) {
            System.out.println("Exception in getAllStockFeaturesFromDB");
            throw exc;
        }
        
        return stockFeatureMatrix;
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
                    
                    stmt.executeUpdate();
                    
                } catch(Exception exc) {
                    System.out.println("Method: insertStockIndexDataIntoDB, Index: " + stockIndex + ", Row: " + i);
                }
            }
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
                    
                    stmt.executeUpdate();
                    
                } catch(Exception exc) {
                    System.out.println("Method: insertEnergyPricesIntoDB, EnergyCode: " + energyCode + ", Row: " + i);
                }
            }
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
                    
                    stmt.executeUpdate();
                    
                } catch(Exception exc) {
                    System.out.println("Method: insertCurrencyRatiosIntoDB, Currency: " + currency + ", Row: " + i);
                }
            }
        } catch(Exception exc) {
            System.out.println("Method: insertCurrencyRatiosIntoDB, Currency: " + currency + ", Description: " + exc);
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
                    
                    stmt.executeUpdate();
                    
                } catch(Exception exc) {
                    System.out.println("Method: insertPreciousMetalsPricesIntoDB, Metal: " + metal + ", Row: " + i);
                }
            }
        } catch(Exception exc) {
            System.out.println("Method: insertPreciousMetalsPricesIntoDB, Metal: " + metal + ", Description: " + exc);
            throw exc;
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
                    System.out.println("Method: insertInflationDataIntoDB, Row: " + i);
                }
            }
        } catch(Exception exc) {
            System.out.println("Method: insertInflationDataIntoDB, Description: " + exc);
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
    
    private void insertInterestRatesIntoDB(String primeRates) throws Exception {
        String[] rows = primeRates.split("\n");

        String row;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date dt;
        java.sql.Date sqlDt;
        BigDecimal rate;

        int i = 0;
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Insert_InterestRates (?, ?)}")) {
            
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
                    System.out.println("Method: insertInterestRatesIntoDB, Row: " + i);
                }
            }
        } catch(Exception exc) {
            System.out.println("Method: insertInterestRatesIntoDB, Description: " + exc);
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
                    
                    stmt.executeUpdate();
                    
                } catch(Exception exc) {
                    System.out.println("Method: insertMortgateDataIntoDB, Row: " + i);
                }
            }
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
                    
                    stmt.executeUpdate();
                    
                } catch(Exception exc) {
                    System.out.println("Method: insertNewHomePriceDataIntoDB, Row: " + i);
                }
            }
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
                    stmt.executeUpdate();
                    
                } catch(Exception exc) {
                    System.out.println("Method: insertStockPricesIntoDB, Ticker: " + stockTicker + "Row: " + i);
                }
            }
        } catch(Exception exc) {
            System.out.println("Method: insertStockPricesIntoDB, Description: " + exc);
            throw exc;
        }
    }

        private void insertStockFundamentalsIntoDB(String stockTicker, String indicator, String stockFundamentals) throws Exception {
        String[] rows = stockFundamentals.split("\n");

        String row;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date dt;
        BigDecimal value;
        java.sql.Date sqlDt;
        int i = 0;
        
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Insert_StockFundamentals (?, ?, ?, ?)}")) {
            
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
                    stmt.setString(1, stockTicker);
                    stmt.setDate(2, sqlDt);
                    stmt.setString(3, indicator);
                    stmt.setBigDecimal(4, value);
                    stmt.executeUpdate();
                    
                } catch(Exception exc) {
                    System.out.println("Method: insertStockFundamentalsIntoDB, Ticker: " + stockTicker + "Row: " + i);
                }
            }
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
            
            while(rs.next()) {
                ticker = rs.getString(1);
                quandlCode = rs.getString(2);
                description = rs.getString(3);
                
                StockTicker st = new StockTicker(ticker, quandlCode, description);
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

    public Date getGDP_UpdateDate() throws Exception {
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Retrieve_GDP_LastUpdate ()}")) {
            
            ResultSet rs = stmt.executeQuery();
            
            if (rs.next())
                return rs.getDate(1);
            else {
                Calendar c = GregorianCalendar.getInstance();
                c.set(1990, 1, 1);
                return c.getTime();
            }
            
        } catch (Exception exc) {
            System.out.println("Exception in getGDP_UpdateDate");
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

    public Date getInterestRates_UpdateDate() throws Exception {
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Retrieve_InterestRates_LastUpdate ()}")) {

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
    
    private void getAllFundamentalsData(String stockTicker) throws Exception {
                
        String[] fundamentalsArray = {"NET_INCOME", 
            "TOTAL_ASSETS", "ACCOUNTS_PAYABLE", "RETAINED_EARNINGS_ACCUMULATED_DEFICIT", "TOTAL_LIABILITIES_SHAREHOLDERS_EQUITY", 
            "NET_INCOME_STARTING_LINE", "DEPRECIATION_DEPLETION", "CASH_FROM_OPERATING_ACTIVITIES", "CASH_FROM_FINANCING_ACTIVITIES", 
            "NET_CHANGE_IN_CASH", "CASH_FROM_INVESTING_ACTIVITIES"};

        Date lastDt;
        String queryString = "";
        String responseData = "";
        for (String fundamentalIndicator : fundamentalsArray) {

            stockTicker = "IBM";
            
            //Find the last updated date
            lastDt = getStockFundamentals_UpdateDate(stockTicker, fundamentalIndicator);

            //Retrieve the data
            queryString = "RAYMOND/" + stockTicker + "_" + fundamentalIndicator + "_Q";
            responseData = downloadData(queryString, lastDt);
            
            //Insert the data
            insertStockFundamentalsIntoDB(stockTicker, fundamentalIndicator, responseData);
        }
    }
    
    public void downloadAllStockData() throws Exception {
        /*
        //Mortgage Rates
        Date lastDt;
        lastDt = get30YrMortgageRates_UpdateDate();
        String thirtyYrMtgRates = downloadData("FMAC/FIX30YR", lastDt);
        insertMortgageDataIntoDB(thirtyYrMtgRates);
        
        //New Home Prices
        lastDt = getAvgNewHomePrices_UpdateDate();
        String newHomePrices = downloadData("FRED/ASPNHSUS", lastDt);
        insertNewHomePriceDataIntoDB(newHomePrices);

        //CPI
        lastDt = getCPI_UpdateDate();
        String cpiInflation = downloadData("RATEINF/CPI_USA", lastDt);
        insertInflationDataIntoDB(cpiInflation);

        //Currency Ratios
        final String JAPAN = "JPY";
        lastDt = getCurrencyRatios_UpdateDate(JAPAN);
        String usdJpy = downloadData("QUANDL/USDJPY", lastDt);
        insertCurrencyRatiosIntoDB(JAPAN, usdJpy);
        
        final String AUSTRALIA = "AUD";
        lastDt = getCurrencyRatios_UpdateDate(AUSTRALIA);
        String usdAud = downloadData("QUANDL/USDAUD", lastDt);
        insertCurrencyRatiosIntoDB(AUSTRALIA, usdAud);
        
        final String EURO = "EUR";
        lastDt = getCurrencyRatios_UpdateDate(EURO);
        String usdEur = downloadData("QUANDL/USDEUR", lastDt);
        insertCurrencyRatiosIntoDB(EURO, usdEur);

        //Energy Prices
        final String CRUDE_OIL = "CRUDE-OIL";
        lastDt = getEnergyPrices_UpdateDate(CRUDE_OIL);
        String crudeOilPrices = downloadData("OFDP/FUTURE_CL1", lastDt);
        insertEnergyPricesIntoDB(CRUDE_OIL, crudeOilPrices);

        final String NATURAL_GAS = "NATURL-GAS";
        lastDt = getEnergyPrices_UpdateDate(NATURAL_GAS);
        String naturalGasPrices = downloadData("OFDP/FUTURE_NG1", lastDt);
        insertEnergyPricesIntoDB(NATURAL_GAS, naturalGasPrices);

        //Precious Metals
        final String GOLD = "GOLD";
        lastDt = getPreciousMetals_UpdateDate(GOLD);
        String goldPrices = downloadData("WGC/GOLD_DAILY_USD", lastDt);
        //Backup Source FRED/GOLDPMGBD228NLBM - Federal Reserve
        insertPreciousMetalsPricesIntoDB(GOLD, goldPrices);
        
        final String SILVER = "SILVER";
        lastDt = getPreciousMetals_UpdateDate(SILVER);
        String silverPrices = downloadData("LBMA/SILVER", lastDt);
        insertPreciousMetalsPricesIntoDB(SILVER, silverPrices);

        final String PLATINUM = "PLATINUM";
        lastDt = getPreciousMetals_UpdateDate(PLATINUM);
        String platinumPrices = downloadData("LPPM/PLAT", lastDt);
        insertPreciousMetalsPricesIntoDB(PLATINUM, platinumPrices);

        //Global Stock Indexes
        final String SP500 = "S&P500";
        lastDt = getStockIndex_UpdateDate(SP500);
        String spIndex = downloadData("YAHOO/INDEX_GSPC", lastDt);
        insertStockIndexDataIntoDB(SP500, spIndex);

        final String DAX = "DAX";
        lastDt = getStockIndex_UpdateDate(DAX);
        String daxIndex = downloadData("YAHOO/INDEX_GDAXI", lastDt);
        insertStockIndexDataIntoDB(DAX, daxIndex);

        final String HANGSENG = "HANGSENG";
        lastDt = getStockIndex_UpdateDate(HANGSENG);
        String hangSengIndex = downloadData("YAHOO/INDEX_HSI", lastDt);
        insertStockIndexDataIntoDB(HANGSENG, hangSengIndex);

        final String NIKEII = "NIKEII";
        lastDt = getStockIndex_UpdateDate(NIKEII);
        String nikeiiIndex = downloadData("NIKKEI/INDEX", lastDt);
        insertStockIndexDataIntoDB(NIKEII, nikeiiIndex);

        //Interest Rates
        lastDt = getInterestRates_UpdateDate();
        String primeRates = downloadData("FRED/DPRIME", lastDt);
        insertInterestRatesIntoDB(primeRates);

        //Unemployment Rates - OLD!!!!!!!
        lastDt = getUnemployment_UpdateDate();
        String unemploymentRates = downloadData("ILOSTAT/UNE_DEAP_RT_SEX_T_M_USA", lastDt);
        insertUnemploymentRatesIntoDB(unemploymentRates);

        //Stock Quotes
        List<StockTicker> listOfAllStocks = getAllStockTickers();
        for (StockTicker st : listOfAllStocks) {
            lastDt = getStockQuote_UpdateDate(st.getTicker());
            String stockValues = downloadData(st.getQuandlCode(), lastDt);
            insertStockPricesIntoDB(st.getTicker(), stockValues);
        }

        //Stock Fundamentals
        /*
        List<StockTicker> listOfAllStocks = getAllStockTickers();
        for (StockTicker st : listOfAllStocks) {
            getAllFundamentalsData(st.getTicker());
        }
        */ 
        
        //GDP
        Date lastDt = getGDP_UpdateDate();
        String jsonGDP = downloadBEAData(lastDt);
        insertGDPIntoDB(jsonGDP);
    }

    private String downloadBEAData(Date lastDt) throws Exception {
        
        StringBuilder sb = new StringBuilder();

        try {
            URL url = new URL("http://www.bea.gov/api/data/?&UserID=" + BEA_USER_ID + "&method=GetData&DataSetName=NIPA&TableID=1&Frequency=Q&Year=X&ResultFormat=JSON");
            URLConnection conxn = url.openConnection();

            System.out.println("Downloading: GDP data from BEA");
            
            //Pull back the data as CSV
            try (InputStream is = conxn.getInputStream()) {
                int c;
                for(;;) {
                    c = is.read();
                    if (c == -1)
                        break;

                    sb.append((char)c);
                }
            }
            
        } catch(Exception exc) {
            System.out.println(exc);
        }

        return sb.toString();
    }
    
    public void setModelValues(String ticker, String modelType, double[] weights, double lambda, double cost) throws Exception {
        BigDecimal theta;
        BigDecimal lambdaBD;
        BigDecimal costBD;
        java.sql.Date dt = new java.sql.Date(new Date().getTime());
        
        try (Connection conxn = getDBConnection();
             CallableStatement stmtWeights = conxn.prepareCall("{call sp_Insert_Weights (?, ?, ?, ?, ?)}");
             CallableStatement stmtModel = conxn.prepareCall("{call sp_Insert_Model_Runs (?, ?, ?, ?)}")) {

            //First insert theta values
            for (int i = 0; i < weights.length; i++) {
                theta = new BigDecimal(weights[i]);

                //Insert theta records into the DB
                stmtWeights.setString(1, ticker);
                stmtWeights.setDate(2, dt);
                stmtWeights.setString(3, modelType);
                stmtWeights.setInt(4, i);
                stmtWeights.setBigDecimal(5, theta);
                stmtWeights.executeUpdate();
            }
            
            //Now insert lambda
            lambdaBD = new BigDecimal(lambda);
            
            stmtWeights.setString(1, ticker);
            stmtWeights.setDate(2, dt);
            stmtWeights.setString(3, modelType);
            stmtWeights.setInt(4, -1);
            stmtWeights.setBigDecimal(5, lambdaBD);
            stmtWeights.executeUpdate();
            
            //Now insert the model cost
            costBD = new BigDecimal(cost);

            stmtModel.setString(1, ticker);
            stmtModel.setDate(2, dt);
            stmtModel.setString(3, modelType);
            stmtModel.setBigDecimal(4, costBD);
            
            stmtModel.executeUpdate();
            

        } catch(Exception exc) {
            System.out.println("Method: insertWeightsIntoDB, Description: " + exc);
            throw exc;
        }
    }
    
    
    private String downloadData(final String QUANDL_CODE, final Date fromDt) {

        //Move the date ONE day ahead
        final long DAY_IN_MILLIS = 86400000;
        fromDt.setTime(fromDt.getTime() + DAY_IN_MILLIS);
        
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String dtStr = sdf.format(fromDt);
        
        String quandlQuery = QUANDL_BASE_URL + QUANDL_CODE + ".csv?auth_token=" + QUANDL_AUTH_TOKEN + "&trim_start=" + dtStr + "&sort_order=asc";
        
        StringBuilder sb = new StringBuilder();

        try {
            URL url = new URL(quandlQuery);
            URLConnection conxn = url.openConnection();

            System.out.println("Downloading: " + quandlQuery);
            
            //Pull back the data as CSV
            try (InputStream is = conxn.getInputStream()) {
                int c;
                for(;;) {
                    c = is.read();
                    if (c == -1)
                        break;

                    sb.append((char)c);
                }
            }
            
        } catch(Exception exc) {
            System.out.println(exc);
        }

        return sb.toString();
    }
}
