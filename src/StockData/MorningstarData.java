/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package StockData;

import Utilities.Logger;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Matt Jones
 */
public class MorningstarData {

    static Logger logger = new Logger();
    
    public StockFundamentals_Quarter getStockFundamentals_Quarterly(StockTicker ticker) throws Exception {
        
        String tenQtrData = getDataFromMorningstar_Quarterly(ticker);
        //String tenQtrData = getDataFromMorningstar_Quarterly_FromFile(ticker);

        StockFundamentals_Quarter stockFundBasics = null;
        try {
            stockFundBasics = parse10QtrData(ticker.getTicker(), tenQtrData);
        } catch (Exception exc) {
            logger.Log("MorningstarData", "getStockFundamentals_Quarterly", "Exception", exc.toString(), true);
        }
        return stockFundBasics;
    }
    
    public StockFundamentals_Annual getStockFundamentals_Annual(StockTicker ticker) throws Exception {
        
        String tenYrData = getDataFromMorningstar_Annual(ticker);

        StockFundamentals_Annual stockFundBasics = null;
        try {
            stockFundBasics = parse10YrData(ticker.getTicker(), tenYrData);
        } catch (Exception exc) {
            logger.Log("MorningstarData", "getStockFundamentals_Annual", "Exception", exc.toString(), true);
        }
        return stockFundBasics;
    }
    
    private StockFundamentals_Annual parse10YrData(String ticker, String input) throws Exception {

        StockFundamentals_Annual stockFund = new StockFundamentals_Annual(ticker);

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
            logger.Log("MorningstarData", "parse10YrData", "Exception", ticker + ": " + exc.toString(), true);
            throw exc;
        }
        
        return stockFund;
    }

    private Map<IncomeStmt_Items, Integer> getRowIndexes(String[] rows) {
        
        Map<IncomeStmt_Items, Integer> map = new HashMap<>();
        String label;
        int index;
        boolean isEPS = false;
        boolean isWeightedShares = false;
        for (int i = 0; i < rows.length; i++) {
            index = rows[i].indexOf(",");
            if (index == -1)
                label = rows[i];
            else
                label = rows[i].substring(0, index).replaceAll("\"", "");
            
            switch (label) {
                case "Revenue":
                    map.put(IncomeStmt_Items.REVENUE, i);
                    break;
                case "Cost of revenue":
                    map.put(IncomeStmt_Items.COST_OF_REVENUE, i);
                    break;
                case "Gross profit":
                    map.put(IncomeStmt_Items.GROSS_PROFIT, i);
                    break;
                case "Research and development":
                    map.put(IncomeStmt_Items.R_AND_D, i);
                    break;
                case "Sales":
                    map.put(IncomeStmt_Items.SALES_GEN_ADMIN, i);
                    break;
                case "Total operating expenses":
                    map.put(IncomeStmt_Items.TOTAL_OP_EXP, i);
                    break;
                case "Operating income":
                    map.put(IncomeStmt_Items.OPER_INC, i);
                    break;
                case "Interest Expense":
                    map.put(IncomeStmt_Items.INT_EXP, i);
                    break;
                case "Other income (expense)":
                    map.put(IncomeStmt_Items.OTHER_INC, i);
                    break;
                case "Income before taxes":
                    map.put(IncomeStmt_Items.INC_BEF_TAX, i);
                    break;
                case "Provision for income taxes":
                    map.put(IncomeStmt_Items.PROV_INC_TAX, i);
                    break;
                case "Net income from continuing operations":
                    map.put(IncomeStmt_Items.NET_INC_CONT_OP, i);
                    break;
                case "Net income from discontinuing ops":
                    map.put(IncomeStmt_Items.NET_INC_DISCONT_OP, i);
                    break;
                case "Net income":
                    map.put(IncomeStmt_Items.NET_INCOME, i);
                    break;
                case "Net income available to common shareholders":
                    map.put(IncomeStmt_Items.NET_INCOME_CMN_SHR, i);
                    break;
                case "Earnings per share":
                    isEPS = true;
                    break;
                case "Weighted average shares outstanding":
                    isWeightedShares = true;
                    break;
                case "Basic":
                    if (isEPS && !isWeightedShares)
                        map.put(IncomeStmt_Items.EPS_BASIC, i);
                    else if (isEPS && isWeightedShares)
                        map.put(IncomeStmt_Items.SHR_OUT_BASIC, i);
                    break;
                case "Diluted":
                    if (isEPS && !isWeightedShares)
                        map.put(IncomeStmt_Items.EPS_DILUTED, i);
                    else if (isEPS && isWeightedShares)
                        map.put(IncomeStmt_Items.SHR_OUT_DILUTED, i);
                    break;
                case "EBITDA":
                    map.put(IncomeStmt_Items.EBITDA, i);
                    break;
            }
        }
        
        return map;
    }
    
    private StockFundamentals_Quarter parse10QtrData(String ticker, String input) throws Exception {

        StockFundamentals_Quarter stockFund = new StockFundamentals_Quarter(ticker);

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
            String[] financialsDates = rows[1].split(",");
            Date[] financialsDatesDt = convertToDates(financialsDates);
            stockFund.setFinancials_Dates(financialsDatesDt);

            final int NUM_DATES = financialsDatesDt.length;
            
            //Determine row indexes for fundamental items
            Map<IncomeStmt_Items, Integer> map = getRowIndexes(rows);
            
            //Revenue
            Integer index = map.get(IncomeStmt_Items.REVENUE);
            if (index != null) {
                String[] financialsRevenue = processString(rows[index]).split(",");
                BigDecimal[] financialsRevenueBD = convertToBD(financialsRevenue, NUM_DATES);
                stockFund.setFinancials_Revenue(financialsRevenueBD);
            }

            //Cost of Revenue
            index = map.get(IncomeStmt_Items.COST_OF_REVENUE);
            if (index != null) {
                String[] financialsCostRevenue = processString(rows[index]).split(",");
                BigDecimal[] financialsCostRevenueBD = convertToBD(financialsCostRevenue, NUM_DATES);
                stockFund.setFinancials_CostOfRev(financialsCostRevenueBD);
            }
            
            //Gross Profit
            index = map.get(IncomeStmt_Items.GROSS_PROFIT);
            if (index != null) {
                String[] financialsGrossProfit = processString(rows[index]).split(",");
                BigDecimal[] financialsGrossProfitBD = convertToBD(financialsGrossProfit, NUM_DATES);
                stockFund.setFinancials_GrossProfit(financialsGrossProfitBD);
            }
            
            //R&D
            index = map.get(IncomeStmt_Items.R_AND_D);
            if (index != null) {
                String[] financialsRandD = processString(rows[index]).split(",");
                BigDecimal[] financialsRandDBD = convertToBD(financialsRandD, NUM_DATES);
                stockFund.setFinancials_RandD(financialsRandDBD);
            }
            
            //Sales Gen and Admin
            index = map.get(IncomeStmt_Items.SALES_GEN_ADMIN);
            if (index != null) {
                String[] financialsSalesGenAdmin = processString(rows[index]).split(",");
                BigDecimal[] financialsSalesGenAdminBD = convertToBD(financialsSalesGenAdmin, NUM_DATES);
                stockFund.setFinancials_SalesGenAdmin(financialsSalesGenAdminBD);
            }
            
            //Total Operating Expenses
            index = map.get(IncomeStmt_Items.TOTAL_OP_EXP);
            if (index != null) {
                String[] financialsTotalOpExp = processString(rows[index]).split(",");
                BigDecimal[] financialsTotalOpExpBD = convertToBD(financialsTotalOpExp, NUM_DATES);
                stockFund.setFinancials_TotalOpExp(financialsTotalOpExpBD);
            }
            
            //Operating Income
            index = map.get(IncomeStmt_Items.OPER_INC);
            if (index != null) {
                String[] financialsOpInc = processString(rows[index]).split(",");
                BigDecimal[] financialsOpIncBD = convertToBD(financialsOpInc, NUM_DATES);
                stockFund.setFinancials_OperIncome(financialsOpIncBD);
            }
            
            //Interest Expense
            index = map.get(IncomeStmt_Items.INT_EXP);
            if (index != null) {
                String[] financialsIntExp = processString(rows[index]).split(",");
                BigDecimal[] financialsIntExpBD = convertToBD(financialsIntExp, NUM_DATES);
                stockFund.setFinancials_IntExp(financialsIntExpBD);
            }
            
            //Other Income
            index = map.get(IncomeStmt_Items.OTHER_INC);
            if (index != null) {
                String[] financialsOtherInc = processString(rows[index]).split(",");
                BigDecimal[] financialsOtherIncBD = convertToBD(financialsOtherInc, NUM_DATES);
                stockFund.setFinancials_OtherIncome(financialsOtherIncBD);
            }
            
            //Income Before Taxes
            index = map.get(IncomeStmt_Items.INC_BEF_TAX);
            if (index != null) {
                String[] financialsIncBefTaxes = processString(rows[index]).split(",");
                BigDecimal[] financialsIncBefTaxesBD = convertToBD(financialsIncBefTaxes, NUM_DATES);
                stockFund.setFinancials_IncomeBeforeTax(financialsIncBefTaxesBD);
            }
            
            //Provision for Income Taxes
            index = map.get(IncomeStmt_Items.PROV_INC_TAX);
            if (index != null) {
                String[] financialsProvIncTax = processString(rows[index]).split(",");
                BigDecimal[] financialsProvIncTaxBD = convertToBD(financialsProvIncTax, NUM_DATES);
                stockFund.setFinancials_ProvForIncTax(financialsProvIncTaxBD);
            }
            
            //Net Income Continuing Operations
            index = map.get(IncomeStmt_Items.NET_INC_CONT_OP);
            if (index != null) {
                String[] financialsNetIncContOp = processString(rows[index]).split(",");
                BigDecimal[] financialsNetIncContOpBD = convertToBD(financialsNetIncContOp, NUM_DATES);
                stockFund.setFinancials_NetIncomeContOp(financialsNetIncContOpBD);
            }
            
            //Net Income Discontinuing Operations
            index = map.get(IncomeStmt_Items.NET_INC_DISCONT_OP);
            if (index != null) {
                String[] financialsNetIncDiscontOp = processString(rows[index]).split(",");
                BigDecimal[] financialsNetIncDiscontOpBD = convertToBD(financialsNetIncDiscontOp, NUM_DATES);
                stockFund.setFinancials_NetIncomeDiscontOp(financialsNetIncDiscontOpBD);
            }
            
            //Net Income
            index = map.get(IncomeStmt_Items.NET_INCOME);
            if (index != null) {
                String[] financialsNetInc = processString(rows[index]).split(",");
                BigDecimal[] financialsNetIncBD = convertToBD(financialsNetInc, NUM_DATES);
                stockFund.setFinancials_NetIncome(financialsNetIncBD);
            }
            
            //Net Income Availble to Common Shareholders
            index = map.get(IncomeStmt_Items.NET_INCOME_CMN_SHR);
            if (index != null) {
                String[] financialsNetIncCmnShr = processString(rows[index]).split(",");
                BigDecimal[] financialsNetIncCmnShrBD = convertToBD(financialsNetIncCmnShr, NUM_DATES);
                stockFund.setFinancials_NetIncomeCommonShareholders(financialsNetIncCmnShrBD);
            }
            
            //EPS-Basic
            index = map.get(IncomeStmt_Items.EPS_BASIC);
            if (index != null) {
                String[] financialsEPSBasic = processString(rows[index]).split(",");
                BigDecimal[] financialsEPSBasicBD = convertToBD(financialsEPSBasic, NUM_DATES);
                stockFund.setFinancials_EPS_Basic(financialsEPSBasicBD);
            }
            
            //EPS-Diluted
            index = map.get(IncomeStmt_Items.EPS_DILUTED);
            if (index != null) {
                String[] financialsEPSDiluted = processString(rows[index]).split(",");
                BigDecimal[] financialsEPSDilutedBD = convertToBD(financialsEPSDiluted, NUM_DATES);
                stockFund.setFinancials_EPS_Diluted(financialsEPSDilutedBD);
            }

            //Average Shares Basic
            index = map.get(IncomeStmt_Items.SHR_OUT_BASIC);
            if (index != null) {
                String[] financialsAvgShrBasic = processString(rows[index]).split(",");
                BigDecimal[] financialsAvgShrBasicBD = convertToBD(financialsAvgShrBasic, NUM_DATES);
                stockFund.setFinancials_AvgSharesOutstanding_Basic(financialsAvgShrBasicBD);
            }
            
            //Average Shares Diluted
            index = map.get(IncomeStmt_Items.SHR_OUT_DILUTED);
            if (index != null) {
                String[] financialsAvgShrDiluted = processString(rows[index]).split(",");
                BigDecimal[] financialsAvgShrDilutedBD = convertToBD(financialsAvgShrDiluted, NUM_DATES);
                stockFund.setFinancials_AvgSharesOutstanding_Diluted(financialsAvgShrDilutedBD);
            }
            
            //EBITDA
            index = map.get(IncomeStmt_Items.EBITDA);
            if (index != null) {
                String[] financialsEBITDA = processString(rows[index]).split(",");
                BigDecimal[] financialsEBITDABD = convertToBD(financialsEBITDA, NUM_DATES);
                stockFund.setFinancials_EBITDA(financialsEBITDABD);
            }
            
        } catch (Exception exc) {
            logger.Log("MorningstarData", "parse10QtrData", "Exception", ticker + ": " + exc.toString(), true);
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

    private String getDataFromMorningstar_Quarterly_FromFile(StockTicker ticker) throws Exception {

        StringBuilder sbBasic = new StringBuilder();
        Path p = Paths.get("C:\\Java\\Quarterlys\\" + ticker.getTicker() + " Income Statement.csv");
        try (BufferedReader r = Files.newBufferedReader(p, Charset.defaultCharset())) {

            String line = null;
            for (;;) {
                line = r.readLine();
                if (line == null)
                    break;
                
                sbBasic.append(line);
                sbBasic.append("\n");
            }
        }
        
        //Compose the response
        return sbBasic.toString();
    }
    
    private String getDataFromMorningstar_Quarterly(StockTicker ticker) throws Exception {
        URL urlTenQtrData = null;

        switch(ticker.getExchange().toUpperCase()) {
            case "NYSE":
                urlTenQtrData = new URL("http://financials.morningstar.com/ajax/ReportProcess4CSV.html?&t=XNYS:" + ticker.getTicker() + "&region=usa&culture=en-US&cur=USD&reportType=is&period=3&dataType=A&order=asc&columnYear=10&rounding=3&view=raw&denominatorView=raw&number=3");
                break;
            case "NASDAQ":
                urlTenQtrData = new URL("http://financials.morningstar.com/ajax/ReportProcess4CSV.html?&t=XNAS:" + ticker.getTicker() + "&region=usa&culture=en-US&cur=USD&reportType=is&period=3&dataType=A&order=asc&columnYear=10&rounding=3&view=raw&denominatorView=raw&number=3");
                break;
            case "AMEX":
                urlTenQtrData = new URL("http://financials.morningstar.com/ajax/ReportProcess4CSV.html?&t=XASE:" + ticker.getTicker() + "&region=usa&culture=en-US&cur=USD&reportType=is&period=3&dataType=A&order=asc&columnYear=10&rounding=3&view=raw&denominatorView=raw&number=3");
                break;
            case "BATS":
                urlTenQtrData = new URL("http://financials.morningstar.com/ajax/ReportProcess4CSV.html?&t=BATS:" + ticker.getTicker() + "&region=usa&culture=en-US&cur=USD&reportType=is&period=3&dataType=A&order=asc&columnYear=10&rounding=3&view=raw&denominatorView=raw&number=3");
                break;
            default:
                throw new Exception("Method: getDataFromMorningstar, Desc: Exchange Not Found! " + ticker.getExchange());
        }
        logger.Log("MorningstarData", "getDataFromMorningstar_Quarterly", "URL", urlTenQtrData.toString(), false);
        
        //Pull back the Financial Data
        int c;
        StringBuilder sbBasic = new StringBuilder();
        URLConnection conxnBasic = urlTenQtrData.openConnection();
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
    
    //Returns two elements: 0 = 10 Year Financial Data, 1 = 10 Quarter Income Statment Data
    private String getDataFromMorningstar_Annual(StockTicker ticker) throws Exception {

        URL urlTenYrData = null;

        switch(ticker.getExchange().toUpperCase()) {
            case "NYSE":
                urlTenYrData = new URL("http://financials.morningstar.com/ajax/exportKR2CSV.html?&callback=?&t=XNYS:" + ticker.getTicker() + "&region=usa&culture=en-US&cur=USD&order=ASC");
                break;
            case "NASDAQ":
                urlTenYrData = new URL("http://financials.morningstar.com/ajax/exportKR2CSV.html?&callback=?&t=XNAS:" + ticker.getTicker() + "&region=usa&culture=en-US&cur=USD&order=ASC");
                break;
            case "AMEX":
                urlTenYrData = new URL("http://financials.morningstar.com/ajax/exportKR2CSV.html?&callback=?&t=XASE:" + ticker.getTicker() + "&region=usa&culture=en-US&cur=USD&order=ASC");
                break;
            case "BATS":
                urlTenYrData = new URL("http://financials.morningstar.com/ajax/exportKR2CSV.html?&callback=?&t=BATS:" + ticker.getTicker() + "&region=usa&culture=en-US&cur=USD&order=ASC");
                break;
            default:
                throw new Exception("Method: getDataFromMorningstar, Desc: Exchange Not Found! " + ticker.getExchange());
        }
        logger.Log("MorningstarData", "getDataFromMorningstar_Annual", "URL", urlTenYrData.toString(), false);
        
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
