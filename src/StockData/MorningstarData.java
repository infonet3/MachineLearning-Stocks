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
import java.util.Calendar;
import java.util.Date;

/**
 *
 * @author Matt Jones
 */
public class MorningstarData {
    public StockFundamentals getStockFundamentals(StockTicker ticker) throws Exception {
        
        String[] responseArray = getDataFromMorningstar(ticker);
        String tenYrData = responseArray[0];
        String tenQtrData = responseArray[1];
        
        StockFundamentals stockFundBasics = parse10YrData(ticker.getTicker(), tenYrData);
        return stockFundBasics;
        
        
        
        
    }
    
    private StockFundamentals parse10YrData(String ticker, String input) {

        StockFundamentals stockFund = new StockFundamentals(ticker);

        try(BufferedReader r = new BufferedReader(new StringReader(input))) {

            final int MAX_ROWS = 18;
            String line;
            String[] rows = new String[MAX_ROWS];

            for (int i = 0; i < MAX_ROWS; i++) {
                line = r.readLine();
                if (line == null)
                    break;
                
                rows[i] = line;
            }

            //Dates
            String[] financialsDates = rows[2].split(",");
            Date[] financialsDatesDt = convertToDates(financialsDates);
            stockFund.setFinancials_Dates(financialsDatesDt);
            
            //Revenue
            String[] financialsRevenue = processString(rows[3]).split(",");
            BigDecimal[] financialsRevenueBD = convertToBD(financialsRevenue);
            stockFund.setFinancials_Revenue(financialsRevenueBD);

            //Gross Margin
            String[] financialsGrossMargin = processString(rows[4]).split(",");
            BigDecimal[] financialsGrossMarginBD = convertToBD(financialsGrossMargin);
            stockFund.setFinancials_GrossMargin(financialsGrossMarginBD);
            
            //Operating Income
            String[] financialsOperIncome = processString(rows[5]).split(",");
            BigDecimal[] financialsOperIncomeBD = convertToBD(financialsOperIncome);
            stockFund.setFinancials_OperIncome(financialsOperIncomeBD);
            
            //Operating Margin
            String[] financialsOperMargin = processString(rows[6]).split(",");
            BigDecimal[] financialsOperMarginBD = convertToBD(financialsOperMargin);
            stockFund.setFinancials_OperMargin(financialsOperMarginBD);
            
            //Net Income
            String[] financialsNetIncome = processString(rows[7]).split(",");
            BigDecimal[] financialsNetIncomeBD = convertToBD(financialsNetIncome);
            stockFund.setFinancials_NetIncome(financialsNetIncomeBD);
            
            //EPS
            String[] financialsEPS = processString(rows[8]).split(",");
            BigDecimal[] financialsEPSBD = convertToBD(financialsEPS);
            stockFund.setFinancials_EPS(financialsEPSBD);
            
            //Dividends
            String[] financialsDividends = processString(rows[9]).split(",");
            BigDecimal[] financialsDividendsBD = convertToBD(financialsDividends);
            stockFund.setFinancials_Dividends(financialsDividendsBD);
            
            //Payout Ratio
            String[] financialsPayoutRatio = processString(rows[10]).split(",");
            BigDecimal[] financialsPayoutRatioBD = convertToBD(financialsPayoutRatio);
            stockFund.setFinancials_PayoutRatio(financialsPayoutRatioBD);
            
            //Num Shares
            String[] financialsSharesMil = processString(rows[11]).split(",");
            BigDecimal[] financialsSharesMilBD = convertToBD(financialsSharesMil);
            stockFund.setFinancials_SharesMil(financialsSharesMilBD);

            //Book Value Per Share
            String[] financialsBookValPerShare = processString(rows[12]).split(",");
            BigDecimal[] financialsBookValPerShareBD = convertToBD(financialsBookValPerShare);
            stockFund.setFinancials_BookValPerShare(financialsBookValPerShareBD);
            
            //Operating Cash Flow
            String[] financialsOperCashFlow = processString(rows[13]).split(",");
            BigDecimal[] financialsOperCashFlowBD = convertToBD(financialsOperCashFlow);
            stockFund.setFinancials_OperCashFlow(financialsOperCashFlowBD);
            
            //Cap Spending
            String[] financialsCapSpending = processString(rows[14]).split(",");
            BigDecimal[] financialsCapSpendingBD = convertToBD(financialsCapSpending);
            stockFund.setFinancials_CapSpending(financialsCapSpendingBD);
            
            //Free Cash Flow
            String[] financialsFreeCashFlow = processString(rows[15]).split(",");
            BigDecimal[] financialsFreeCashFlowBD = convertToBD(financialsFreeCashFlow);
            stockFund.setFinancials_FreeCashFlow(financialsFreeCashFlowBD);
            
            //Free Cash Flow Per Share
            String[] financialsFreeCashFlowPerShare = processString(rows[16]).split(",");
            BigDecimal[] financialsFreeCashFlowPerShareBD = convertToBD(financialsFreeCashFlowPerShare);
            stockFund.setFinancials_FreeCashFlowPerShare(financialsFreeCashFlowPerShareBD);
            
            //Working Capital
            String[] financialsWorkingCap = processString(rows[17]).split(",");
            BigDecimal[] financialsWorkingCapBD = convertToBD(financialsWorkingCap);
            stockFund.setFinancials_WorkingCap(financialsWorkingCapBD);
            
        } catch (Exception exc) {
            System.out.println(exc);
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
    
    private static BigDecimal[] convertToBD(String[] strArray) {
        BigDecimal[] bdArray = new BigDecimal[strArray.length - 1];
        
        //Skip first column as it has text values
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
    private String[] getDataFromMorningstar(StockTicker ticker) throws Exception {

        //Slow down execution
        Thread.sleep((int)(30000.0 * Math.random()));
        
        URL urlTenYrData = null;
        URL urlTenQtrData = null;

        switch(ticker.getExchange().toUpperCase()) {
            case "NYSE":
                urlTenYrData = new URL("http://financials.morningstar.com/ajax/exportKR2CSV.html?&callback=?&t=XNYS:" + ticker.getTicker() + "&region=usa&culture=en-US&cur=USD&order=ASC");
                urlTenQtrData = new URL("http://financials.morningstar.com/ajax/ReportProcess4CSV.html?&t=XNYS:" + ticker.getTicker() + "&region=usa&culture=en-US&cur=USD&reportType=is&period=3&dataType=A&order=asc&columnYear=10&rounding=3&view=raw&denominatorView=raw&number=3");
                break;
            case "NASDAQ":
                urlTenYrData = new URL("http://financials.morningstar.com/ajax/exportKR2CSV.html?&callback=?&t=XNAS:" + ticker.getTicker() + "&region=usa&culture=en-US&cur=USD&order=ASC");
                urlTenQtrData = new URL("http://financials.morningstar.com/ajax/ReportProcess4CSV.html?&t=XNAS:" + ticker.getTicker() + "&region=usa&culture=en-US&cur=USD&reportType=is&period=3&dataType=A&order=asc&columnYear=10&rounding=3&view=raw&denominatorView=raw&number=3");
                break;
            case "AMEX":
                urlTenYrData = new URL("http://financials.morningstar.com/ajax/exportKR2CSV.html?&callback=?&t=XASE:" + ticker.getTicker() + "&region=usa&culture=en-US&cur=USD&order=ASC");
                urlTenQtrData = new URL("http://financials.morningstar.com/ajax/ReportProcess4CSV.html?&t=XASE:" + ticker.getTicker() + "&region=usa&culture=en-US&cur=USD&reportType=is&period=3&dataType=A&order=asc&columnYear=10&rounding=3&view=raw&denominatorView=raw&number=3");
                break;
            default:
                throw new Exception("Method: getDataFromMorningstar, Desc: Exchange Not Found!");
        }
        System.out.println(urlTenYrData);
        System.out.println(urlTenQtrData);
        
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

        //Pull back the Income Statement
        StringBuilder sbIncomeStmt = new StringBuilder();
        URLConnection conxnIncomeStmt = urlTenQtrData.openConnection();
        try(InputStream is = conxnIncomeStmt.getInputStream()) {
            for (;;) {
                c = is.read();
                if (c == -1)
                    break;

                sbIncomeStmt.append((char) c);
            }
        }
        System.out.println(sbIncomeStmt);

        //Compose the response
        String[] response = new String[2];
        response[0] =sbBasic.toString();
        response[1] = sbIncomeStmt.toString();
        return response;
    }
}
