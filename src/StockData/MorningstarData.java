/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package StockData;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 *
 * @author Matt Jones
 */
public class MorningstarData {
    public StockFundamentals getStockFundamentals(StockTicker ticker) throws Exception {
        
        String tenYrData = getDataFromMorningstar(ticker);

        StockFundamentals stockFundBasics = null;
        try {
            stockFundBasics = parse10YrData(ticker.getTicker(), tenYrData);
        } catch (Exception exc) {
            System.out.println("Method: getStockFundamentals, Ticker: " + ticker + ", Error Parsing Data!");
        }
        return stockFundBasics;
    }
    
    private StockFundamentals parse10YrData(String ticker, String input) throws Exception {

        StockFundamentals stockFund = new StockFundamentals(ticker);

        try(BufferedReader r = new BufferedReader(new StringReader(input))) {

            String line;
            List<String> rowList = new ArrayList<>();

            for (;;) {
                line = r.readLine();
                if (line == null)
                    break;
                
                rowList.add(line);
            }
            
            //Sanity Check
            if (rowList.size() <= 1)
                throw new Exception("No data returned from Morningstar!");

            //Convert to Array from List
            String[] rows = rowList.toArray(new String[rowList.size()]);
            
            //Dates
            String[] financialsDates = rows[2].split(",");
            Date[] financialsDatesDt = convertToDates(financialsDates);
            stockFund.setFinancials_Dates(financialsDatesDt);

            final int NUM_DATES = financialsDatesDt.length;
            
            //Revenue
            String[] financialsRevenue = processString(rows[3]).split(",");
            BigDecimal[] financialsRevenueBD = convertToBD(financialsRevenue, NUM_DATES);
            stockFund.setFinancials_Revenue(financialsRevenueBD);

            //Gross Margin
            String[] financialsGrossMargin = processString(rows[4]).split(",");
            BigDecimal[] financialsGrossMarginBD = convertToBD(financialsGrossMargin, NUM_DATES);
            stockFund.setFinancials_GrossMargin(financialsGrossMarginBD);
            
            //Operating Income
            String[] financialsOperIncome = processString(rows[5]).split(",");
            BigDecimal[] financialsOperIncomeBD = convertToBD(financialsOperIncome, NUM_DATES);
            stockFund.setFinancials_OperIncome(financialsOperIncomeBD);
            
            //Operating Margin
            String[] financialsOperMargin = processString(rows[6]).split(",");
            BigDecimal[] financialsOperMarginBD = convertToBD(financialsOperMargin, NUM_DATES);
            stockFund.setFinancials_OperMargin(financialsOperMarginBD);
            
            //Net Income
            String[] financialsNetIncome = processString(rows[7]).split(",");
            BigDecimal[] financialsNetIncomeBD = convertToBD(financialsNetIncome, NUM_DATES);
            stockFund.setFinancials_NetIncome(financialsNetIncomeBD);
            
            //EPS
            String[] financialsEPS = processString(rows[8]).split(",");
            BigDecimal[] financialsEPSBD = convertToBD(financialsEPS, NUM_DATES);
            stockFund.setFinancials_EPS(financialsEPSBD);
            
            //Dividends
            String[] financialsDividends = processString(rows[9]).split(",");
            BigDecimal[] financialsDividendsBD = convertToBD(financialsDividends, NUM_DATES);
            stockFund.setFinancials_Dividends(financialsDividendsBD);
            
            //Payout Ratio
            String[] financialsPayoutRatio = processString(rows[10]).split(",");
            BigDecimal[] financialsPayoutRatioBD = convertToBD(financialsPayoutRatio, NUM_DATES);
            stockFund.setFinancials_PayoutRatio(financialsPayoutRatioBD);
            
            //Num Shares
            String[] financialsSharesMil = processString(rows[11]).split(",");
            BigDecimal[] financialsSharesMilBD = convertToBD(financialsSharesMil, NUM_DATES);
            stockFund.setFinancials_SharesMil(financialsSharesMilBD);

            //Book Value Per Share
            String[] financialsBookValPerShare = processString(rows[12]).split(",");
            BigDecimal[] financialsBookValPerShareBD = convertToBD(financialsBookValPerShare, NUM_DATES);
            stockFund.setFinancials_BookValPerShare(financialsBookValPerShareBD);
            
            //Operating Cash Flow
            String[] financialsOperCashFlow = processString(rows[13]).split(",");
            BigDecimal[] financialsOperCashFlowBD = convertToBD(financialsOperCashFlow, NUM_DATES);
            stockFund.setFinancials_OperCashFlow(financialsOperCashFlowBD);
            
            //Cap Spending
            String[] financialsCapSpending = processString(rows[14]).split(",");
            BigDecimal[] financialsCapSpendingBD = convertToBD(financialsCapSpending, NUM_DATES);
            stockFund.setFinancials_CapSpending(financialsCapSpendingBD);
            
            //Free Cash Flow
            String[] financialsFreeCashFlow = processString(rows[15]).split(",");
            BigDecimal[] financialsFreeCashFlowBD = convertToBD(financialsFreeCashFlow, NUM_DATES);
            stockFund.setFinancials_FreeCashFlow(financialsFreeCashFlowBD);
            
            //Free Cash Flow Per Share
            String[] financialsFreeCashFlowPerShare = processString(rows[16]).split(",");
            BigDecimal[] financialsFreeCashFlowPerShareBD = convertToBD(financialsFreeCashFlowPerShare, NUM_DATES);
            stockFund.setFinancials_FreeCashFlowPerShare(financialsFreeCashFlowPerShareBD);
            
            //Working Capital
            String[] financialsWorkingCap = processString(rows[17]).split(",");
            BigDecimal[] financialsWorkingCapBD = convertToBD(financialsWorkingCap, NUM_DATES);
            stockFund.setFinancials_WorkingCap(financialsWorkingCapBD);
            
            //Return on Assets
            String[] financialsROA = processString(rows[35]).split(",");
            BigDecimal[] financialsROABD = convertToBD(financialsROA, NUM_DATES);
            stockFund.setFinancials_ReturnOnAssets(financialsROABD);
            
            //Return on Equity
            String[] financialsROE = processString(rows[37]).split(",");
            BigDecimal[] financialsROEBD = convertToBD(financialsROE, NUM_DATES);
            stockFund.setFinancials_ReturnOnEquity(financialsROEBD);
            
        } catch (Exception exc) {
            System.out.println("Method: parse10YrData, Desc: Ticker = " + ticker + ", " + input);
            throw exc;
        }
        
        return stockFund;
    }
    
    //Go through string and eliminate un-necessary commas
    private static String processString(String input) {
        char ch;
        boolean foundQuotes = false;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < input.length(); i++) {
            
            ch = input.charAt(i);

            //Found first double quote
            if (ch == '"' && !foundQuotes) {
                foundQuotes = true;
            }
            //Found last double quote
            else if (ch == '"' && foundQuotes) {
                foundQuotes = false;
            }
            //Found comma after double quote
            else if (ch == ',' && foundQuotes) {
                continue;
            }
            //Found different character
            else {
                sb.append(ch);
            }
        }
        
        return sb.toString();
    }
    
    private static BigDecimal[] convertToBD(String[] strArray, final int NUM_DATES) {
        
        //Check to ensure that empty strings still generate 0.0 values
        if (strArray.length < NUM_DATES + 1) {
            strArray = new String[NUM_DATES + 1];
            for (int i  = 0; i < strArray.length; i++) {
                strArray[i] = new String();
            }
        }
        
        //Skip first column as it has text values
        BigDecimal[] bdArray = new BigDecimal[strArray.length - 1];

        for (int i = 0; i < bdArray.length; i++) {
            if (!strArray[i + 1].isEmpty())
                bdArray[i] = new BigDecimal(strArray[i + 1]);
            else
                bdArray[i] = new BigDecimal("0.0");
        }
        
        return bdArray;
    }
    
    private static Date[] convertToDates(String[] strArray) {
        Date[] dtArray = new Date[strArray.length - 1];
        
        //Skip first column as its empty
        for (int i = 0; i < dtArray.length; i++) {
            String val = strArray[i + 1];
            int year, month;
            
            if (val.equals("TTM")) {
                Calendar c = Calendar.getInstance();
                year = c.get(Calendar.YEAR);
                month = c.get(Calendar.MONTH) + 1;
            } else {
                year = Integer.parseInt(val.substring(0, 4));
                month = Integer.parseInt(val.substring(5, 7));
            }

            Calendar cal = Calendar.getInstance();
            cal.set(year, month - 1, 1);
            dtArray[i] = cal.getTime();
        }
        
        return dtArray;
    }

    
    //Returns two elements: 0 = 10 Year Financial Data, 1 = 10 Quarter Income Statment Data
    private String getDataFromMorningstar(StockTicker ticker) throws Exception {

        URL urlTenYrData = null;
        //URL urlTenQtrData = null;

        switch(ticker.getExchange().toUpperCase()) {
            case "NYSE":
                urlTenYrData = new URL("http://financials.morningstar.com/ajax/exportKR2CSV.html?&callback=?&t=XNYS:" + ticker.getTicker() + "&region=usa&culture=en-US&cur=USD&order=ASC");
                //urlTenQtrData = new URL("http://financials.morningstar.com/ajax/ReportProcess4CSV.html?&t=XNYS:" + ticker.getTicker() + "&region=usa&culture=en-US&cur=USD&reportType=is&period=3&dataType=A&order=asc&columnYear=10&rounding=3&view=raw&denominatorView=raw&number=3");
                break;
            case "NASDAQ":
                urlTenYrData = new URL("http://financials.morningstar.com/ajax/exportKR2CSV.html?&callback=?&t=XNAS:" + ticker.getTicker() + "&region=usa&culture=en-US&cur=USD&order=ASC");
                //urlTenQtrData = new URL("http://financials.morningstar.com/ajax/ReportProcess4CSV.html?&t=XNAS:" + ticker.getTicker() + "&region=usa&culture=en-US&cur=USD&reportType=is&period=3&dataType=A&order=asc&columnYear=10&rounding=3&view=raw&denominatorView=raw&number=3");
                break;
            case "AMEX":
                urlTenYrData = new URL("http://financials.morningstar.com/ajax/exportKR2CSV.html?&callback=?&t=XASE:" + ticker.getTicker() + "&region=usa&culture=en-US&cur=USD&order=ASC");
                //urlTenQtrData = new URL("http://financials.morningstar.com/ajax/ReportProcess4CSV.html?&t=XASE:" + ticker.getTicker() + "&region=usa&culture=en-US&cur=USD&reportType=is&period=3&dataType=A&order=asc&columnYear=10&rounding=3&view=raw&denominatorView=raw&number=3");
                break;
            case "BATS":
                urlTenYrData = new URL("http://financials.morningstar.com/ajax/exportKR2CSV.html?&callback=?&t=BATS:" + ticker.getTicker() + "&region=usa&culture=en-US&cur=USD&order=ASC");
                //urlTenQtrData = new URL("http://financials.morningstar.com/ajax/ReportProcess4CSV.html?&t=BATS:" + ticker.getTicker() + "&region=usa&culture=en-US&cur=USD&reportType=is&period=3&dataType=A&order=asc&columnYear=10&rounding=3&view=raw&denominatorView=raw&number=3");
                break;
            default:
                throw new Exception("Method: getDataFromMorningstar, Desc: Exchange Not Found! " + ticker.getExchange());
        }
        System.out.println(urlTenYrData);
        //System.out.println(urlTenQtrData);
        
        //Pull back the Financial Data
        int c;
        StringBuilder sbBasic = new StringBuilder();
        URLConnection conxnBasic = urlTenYrData.openConnection();
        try(InputStream is = conxnBasic.getInputStream()) {
            for (;;) {
                c = is.read();
                if (c == -1)
                    break;

                sbBasic.append((char) c);
            }
        }

        //Compose the response
        return sbBasic.toString();
    }
}
