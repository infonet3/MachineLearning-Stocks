package Trading;

import Modeling.Predictor;
import StockData.StockDataHandler;
import StockData.StockHolding;
import StockData.StockQuote;
import Trading.IB.*;
import Utilities.Logger;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public class TradeEngine implements EWrapper {

    //Fields
    volatile BigDecimal availableFunds = null;
    volatile BigDecimal stockQuote = null;
    volatile List<StockHolding> listHoldings = new ArrayList<>();
    int orderID = 1;
    Logger logger = new Logger();
    
    //Methods
    public TradeEngine() throws Exception {
    }
    
    @Override
    public void tickPrice(int tickerId, int field, double price, int canAutoExecute) {
        
        final int CLOSE_PRICE = 9;
        final int LAST_PRICE = 4;
        
        if (field == LAST_PRICE) //New change 11-23-14
            stockQuote = new BigDecimal(String.valueOf(price));

        String outputMsg = String.format("Field = %d, Price = %f", field, price);
        try {
            logger.Log("TradeEngine", "tickPrice", outputMsg, "", false);
        } catch(Exception exc) {
            System.out.println("Exception: tickPrice");
        }
    }

    @Override
    public void tickSize(int tickerId, int field, int size) {

        try {
            logger.Log("TradeEngine", "tickSize", "", "", false);
        } catch(Exception exc) {
            System.out.println("Exception: tickSize");
        }
    }

    @Override
    public void tickOptionComputation(int tickerId, int field, double impliedVol, double delta, double optPrice, double pvDividend, double gamma, double vega, double theta, double undPrice) {

        try {
            logger.Log("TradeEngine", "tickOptionComputation", "", "", false);
        } catch(Exception exc) {
            System.out.println("Exception: tickOptionComputation");
        }
    }

    @Override
    public void tickGeneric(int tickerId, int tickType, double value) {

        try {
            logger.Log("TradeEngine", "tickGeneric", "", "", false);
        } catch(Exception exc) {
            System.out.println("Exception: tickGeneric");
        }
    }

    @Override
    public void tickString(int tickerId, int tickType, String value) {

        try {
            logger.Log("TradeEngine", "tickString", "", "", false);
        } catch(Exception exc) {
            System.out.println("Exception: tickString");
        }
    }

    @Override
    public void tickEFP(int tickerId, int tickType, double basisPoints, String formattedBasisPoints, double impliedFuture, int holdDays, String futureExpiry, double dividendImpact, double dividendsToExpiry) {

        try {
            logger.Log("TradeEngine", "tickEFP", "", "", false);
        } catch(Exception exc) {
            System.out.println("Exception: tickEFP");
        }
    }

    @Override
    public void orderStatus(int orderId, String status, int filled, int remaining, double avgFillPrice, int permId, int parentId, double lastFillPrice, int clientId, String whyHeld) {

        try {
            logger.Log("TradeEngine", "orderStatus", "", "", false);
        } catch(Exception exc) {
            System.out.println("Exception: orderStatus");
        }
    }

    @Override
    public void openOrder(int orderId, Contract contract, Order order, OrderState orderState) {

        String outputMsg = String.format("Order: %d, Ticker: %s, Order: %d, State: %s", orderId, contract.m_symbol, order.m_totalQuantity, orderState.m_status);
        try {
            logger.Log("TradeEngine", "openOrder", outputMsg, "", false);
        } catch(Exception exc) {
            System.out.println("Exception: openOrder");
        }
    }

    @Override
    public void openOrderEnd() {

        try {
            logger.Log("TradeEngine", "openOrderEnd", "", "", false);
        } catch(Exception exc) {
            System.out.println("Exception: openOrderEnd");
        }
    }

    @Override
    public void updateAccountValue(String key, String value, String currency, String accountName) {

        try {
            logger.Log("TradeEngine", "updateAccountValue", "", "", false);
        } catch(Exception exc) {
            System.out.println("Exception: updateAccountValue");
        }
    }

    @Override
    public void updatePortfolio(Contract contract, int position, double marketPrice, double marketValue, double averageCost, double unrealizedPNL, double realizedPNL, String accountName) {

        try {
            logger.Log("TradeEngine", "updatePortfolio", "", "", false);
        } catch(Exception exc) {
            System.out.println("Exception: updatePortfolio");
        }
    }

    @Override
    public void updateAccountTime(String timeStamp) {

        try {
            logger.Log("TradeEngine", "updateAccountTime", "", "", false);
        } catch(Exception exc) {
            System.out.println("Exception: updateAccountTime");
        }
    }

    @Override
    public void accountDownloadEnd(String accountName) {

        try {
            logger.Log("TradeEngine", "accountDownloadEnd", "", "", false);
        } catch(Exception exc) {
            System.out.println("Exception: accountDownloadEnd");
        }
    }

    @Override
    public void nextValidId(int orderId) {

        try {
            logger.Log("TradeEngine", "nextValidId", "", "", false);
        } catch(Exception exc) {
            System.out.println("Exception: nextValidId");
        }
    }

    @Override
    public void contractDetails(int reqId, ContractDetails contractDetails) {

        try {
            logger.Log("TradeEngine", "contractDetails", "", "", false);
        } catch(Exception exc) {
            System.out.println("Exception: contractDetails");
        }
    }

    @Override
    public void bondContractDetails(int reqId, ContractDetails contractDetails) {

        try {
            logger.Log("TradeEngine", "bondContractDetails", "", "", false);
        } catch(Exception exc) {
            System.out.println("Exception: bondContractDetails");
        }
    }

    @Override
    public void contractDetailsEnd(int reqId) {

        try {
            logger.Log("TradeEngine", "contractDetailsEnd", "", "", false);
        } catch(Exception exc) {
            System.out.println("Exception: contractDetailsEnd");
        }
    }

    @Override
    public void execDetails(int reqId, Contract contract, Execution execution) {
        String ticker = contract.m_symbol;
        double totalPrice = execution.m_price;
        String outputMsg = String.format("Method: execDetails, reqId: %d, ticker: %s, price: %f", reqId, ticker, totalPrice);

        try {
            logger.Log("TradeEngine", "execDetails", outputMsg, "", false);
        } catch(Exception exc) {
            System.out.println("Exception: execDetails");
        }
    }

    @Override
    public void execDetailsEnd(int reqId) {

        try {
            logger.Log("TradeEngine", "execDetailsEnd", "", "", false);
        } catch(Exception exc) {
            System.out.println("Exception: execDetailsEnd");
        }
    }

    @Override
    public void updateMktDepth(int tickerId, int position, int operation, int side, double price, int size) {

        try {
            logger.Log("TradeEngine", "updateMktDepth", "", "", false);
        } catch(Exception exc) {
            System.out.println("Exception: updateMktDepth");
        }
    }

    @Override
    public void updateMktDepthL2(int tickerId, int position, String marketMaker, int operation, int side, double price, int size) {

        try {
            logger.Log("TradeEngine", "updateMktDepthL2", "", "", false);
        } catch(Exception exc) {
            System.out.println("Exception: updateMktDepthL2");
        }
    }

    @Override
    public void updateNewsBulletin(int msgId, int msgType, String message, String origExchange) {

        try {
            logger.Log("TradeEngine", "updateNewsBulletin", "", "", false);
        } catch(Exception exc) {
            System.out.println("Exception: updateNewsBulletin");
        }
    }

    @Override
    public void managedAccounts(String accountsList) {
    
        try {
            logger.Log("TradeEngine", "managedAccounts", "", "", false);
        } catch(Exception exc) {
            System.out.println("Exception: managedAccounts");
        }
    }

    @Override
    public void receiveFA(int faDataType, String xml) {

        try {
            logger.Log("TradeEngine", "receiveFA", "", "", false);
        } catch(Exception exc) {
            System.out.println("Exception: receiveFA");
        }
    }

    @Override
    public void historicalData(int reqId, String date, double open, double high, double low, double close, int volume, int count, double WAP, boolean hasGaps) {

        try {
            logger.Log("TradeEngine", "historicalData", "", "", false);
        } catch(Exception exc) {
            System.out.println("Exception: historicalData");
        }
    }

    @Override
    public void scannerParameters(String xml) {

        try {
            logger.Log("TradeEngine", "scannerParameters", "", "", false);
        } catch(Exception exc) {
            System.out.println("Exception: scannerParameters");
        }
    }

    @Override
    public void scannerData(int reqId, int rank, ContractDetails contractDetails, String distance, String benchmark, String projection, String legsStr) {

        try {
            logger.Log("TradeEngine", "scannerData", "", "", false);
        } catch(Exception exc) {
            System.out.println("Exception: scannerData");
        }
    }

    @Override
    public void scannerDataEnd(int reqId) {

        try {
            logger.Log("TradeEngine", "scannerDataEnd", "", "", false);
        } catch(Exception exc) {
            System.out.println("Exception: scannerDataEnd");
        }
    }

    @Override
    public void realtimeBar(int reqId, long time, double open, double high, double low, double close, long volume, double wap, int count) {

        try {
            logger.Log("TradeEngine", "realtimeBar", "", "", false);
        } catch(Exception exc) {
            System.out.println("Exception: realtimeBar");
        }
    }

    @Override
    public void currentTime(long time) {

        try {
            logger.Log("TradeEngine", "currentTime", "", "", false);
        } catch(Exception exc) {
            System.out.println("Exception: currentTime");
        }
    }

    @Override
    public void fundamentalData(int reqId, String data) {

        try {
            logger.Log("TradeEngine", "fundamentalData", "", "", false);
        } catch(Exception exc) {
            System.out.println("Exception: fundamentalData");
        }
    }

    @Override
    public void deltaNeutralValidation(int reqId, UnderComp underComp) {

        try {
            logger.Log("TradeEngine", "deltaNeutralValidation", "", "", false);
        } catch(Exception exc) {
            System.out.println("Exception: deltaNeutralValidation");
        }
    }

    @Override
    public void tickSnapshotEnd(int reqId) {

        try {
            logger.Log("TradeEngine", "tickSnapshotEnd", "", "", false);
        } catch(Exception exc) {
            System.out.println("Exception: tickSnapshotEnd");
        }
    }

    @Override
    public void marketDataType(int reqId, int marketDataType) {

        try {
            logger.Log("TradeEngine", "marketDataType", "", "", false);
        } catch(Exception exc) {
            System.out.println("Exception: marketDataType");
        }
    }

    @Override
    public void commissionReport(CommissionReport commissionReport) {

        try {
            logger.Log("TradeEngine", "commissionReport", "", "", false);
        } catch(Exception exc) {
            System.out.println("Exception: commissionReport");
        }
    }

    @Override
    public void position(String account, Contract contract, int pos, double avgCost) {

        if (pos > 0) {
            StockHolding holding = new StockHolding(contract.m_symbol, pos, null);
            listHoldings.add(holding);
        }
        
        String outputMsg = String.format("Account: %s, Ticker: %s, Pos: %d, Cost: %f, Expiry: %s", account, contract.m_symbol, pos, avgCost, contract.m_expiry);
        try {
            logger.Log("TradeEngine", "position", outputMsg, "", false);
        } catch(Exception exc) {
            System.out.println("Exception: position");
        }
    }

    @Override
    public void positionEnd() {

        try {
            logger.Log("TradeEngine", "positionEnd", "", "", false);
        } catch(Exception exc) {
            System.out.println("Exception: positionEnd");
        }
    }

    @Override
    public void accountSummary(int reqId, String account, String tag, String value, String currency) {
        
        final String AVAIL_FUNDS = "TotalCashValue";
        if (tag.equals(AVAIL_FUNDS)) {
            availableFunds = new BigDecimal(value);
        }
        
        String outputMsg = String.format("ReqID: %d, Acct: %s, Tag: %s, Value: %s, Currency: %s", reqId, account, tag, value, currency);
        try {
            logger.Log("TradeEngine", "accountSummary", outputMsg, "", false);
        } catch(Exception exc) {
            System.out.println("Exception: accountSummary");
        }
    }

    @Override
    public void accountSummaryEnd(int reqId) {

        try {
            logger.Log("TradeEngine", "accountSummaryEnd", "", "", false);
        } catch(Exception exc) {
            System.out.println("Exception: accountSummaryEnd");
        }
    }

    @Override
    public void verifyMessageAPI(String apiData) {

        try {
            logger.Log("TradeEngine", "verifyMessageAPI", "", "", false);
        } catch(Exception exc) {
            System.out.println("Exception: verifyMessageAPI");
        }
    }

    @Override
    public void verifyCompleted(boolean isSuccessful, String errorText) {

        try {
            logger.Log("TradeEngine", "verifyCompleted", "", "", false);
        } catch(Exception exc) {
            System.out.println("Exception: verifyCompleted");
        }
    }

    @Override
    public void displayGroupList(int reqId, String groups) {

        try {
            logger.Log("TradeEngine", "displayGroupList", "", "", false);
        } catch(Exception exc) {
            System.out.println("Exception: displayGroupList");
        }
    }

    @Override
    public void displayGroupUpdated(int reqId, String contractInfo) {

        try {
            logger.Log("TradeEngine", "displayGroupUpdates", "", "", false);
        } catch(Exception exc) {
            System.out.println("Exception: displayGroupUpdates");
        }
    }
    
    private void reqStockQuote(EClientSocket client, int tickerID, String quote) {
        Contract c = new Contract();
        c.m_conId = 0;
        c.m_symbol = quote;
        c.m_secType = "STK";
        c.m_expiry = "";
        c.m_strike = 0.0;
        c.m_right = "";
        c.m_multiplier = "";
        c.m_exchange = "SMART";
        c.m_primaryExch = "ISLAND";
        c.m_currency = "USD";
        c.m_localSymbol = "";
        c.m_tradingClass = "";
        //c.m_primaryExch = "";
        c.m_secIdType = "";
        c.m_secId = null;
    	client.reqMktData(tickerID, c, "", true, Collections.<TagValue>emptyList());
    }
    
    private void reqTrade(EClientSocket client, final TradeAction ACTION_CD, int orderID, String ticker, int numShares, double curPrice) throws Exception {

        String strOutput = String.format("Action: %s, Ticker: %s, Shares: %d, Price: %f", ACTION_CD.toString(), ticker, numShares, curPrice);
        logger.Log("TradeEngine", "reqTrade", strOutput, "", false);

        Contract contract = new Contract();
        contract.m_symbol = ticker.toUpperCase();
        contract.m_secType = "STK";
        contract.m_exchange = "SMART";
        contract.m_currency = "USD";
        
        Order order = new Order();
        order.m_action = ACTION_CD.toString();
        order.m_totalQuantity = numShares;
        //order.m_orderType = "MKT";
        order.m_orderType = "LMT";
        order.m_lmtPrice = curPrice;
        
        client.placeOrder(orderID, contract, order);
    }

    public void emailTodaysStockPicks(final int MAX_STOCK_COUNT, Date runDate) throws Exception {

        Predictor p = new Predictor();
        List<StockHolding> stkPicks = p.topStockPicks(MAX_STOCK_COUNT, runDate);
        
        StringBuilder sb = new StringBuilder("Today's top stock picks: ");
        for (StockHolding stk : stkPicks) {
            sb.append(stk.getTicker());
            sb.append("\t");
        }

        String subject = "Top Picks for: " + runDate.toString();
        Notifications.EmailActions.SendEmail(subject, sb.toString());
    }

    private boolean isPriceCloseToYesterdayClose(String ticker) throws Exception {

        //Ensure price is similar to yesterday's close, 10% max
        StockDataHandler sdh = new StockDataHandler();
        StockQuote quote = sdh.getStockQuote(ticker, Utilities.Dates.getYesterday());
        BigDecimal yesterdayClose = quote.getClose();
        
        double ratio = yesterdayClose.doubleValue() / stockQuote.doubleValue();
        
        //10% max deviation
        if (ratio < 0.9 || ratio > 1.1) 
            return false;
        else 
            return true;
    }
    
    private boolean isStockQuoteValid(String ticker) throws Exception {

        final double MIN_TRADE_PRICE = 5.0; //Min price for a stock, otherwise don't trade it
                                
        if (stockQuote == null || stockQuote.doubleValue() <= MIN_TRADE_PRICE) {
            logger.Log("TradeEngine", "isStockQuoteValid", ticker, "Price is null OR less than Min trade price!", true);
            return false;
        }

        boolean isPriceCloseToYesterday = isPriceCloseToYesterdayClose(ticker);
        if (!isPriceCloseToYesterday) {
            logger.Log("TradeEngine", "isStockQuoteValid", ticker, "Price is 10% or more different from yesterdays close!", true);
            return false;
        }
        
        return true;
    }
    
    /* Method: runTrading
     * Notes: This method will only buy at 9AM and will only sell at 3PM
     */
    public void runTrading(final int MAX_STOCK_COUNT, final int IB_GATE_PORT) throws Exception {

        final int WAIT_TIME = 3000;
        
        Calendar calNow = Calendar.getInstance();
        final int HOUR_OF_DAY = calNow.get(Calendar.HOUR_OF_DAY);
        
        String strOutput = String.format("Max Stock Count: %d", MAX_STOCK_COUNT);
        logger.Log("TradeEngine", "runTrading", strOutput, "", false);
        
        EClientSocket client = connect(IB_GATE_PORT);
        try {
            //Get current holdings
            client.reqPositions();
            Thread.sleep(WAIT_TIME);
            int holdingCount = listHoldings.size();

            //Find expiration from DB
            StockDataHandler sdh = new StockDataHandler();
            if (holdingCount > 0) {
                List<StockHolding> listCurStocks = sdh.getCurrentStockHoldings();
                logger.Log("TradeEngine", "runTrading", "Current Holdings = " + listCurStocks.size(), "", false);

                //Sanity check
                if (holdingCount != listCurStocks.size()) {
                    logger.Log("TradeEngine", "runTrading", "Assertion Failed", "Number of positions held at IB doesn't match positions held in DB", true);
                    System.exit(5);
                }

                //Sell everything if expired
                Date now = new Date();
                Date expDt = listCurStocks.get(0).getTargetDate(); //Get first stocks's expiration date, they should be the same
                if (now.compareTo(expDt) >= 0) { 

                    if (HOUR_OF_DAY != 15) //Ensure its 3PM
                        return;

                    String strExpOutput = String.format("Current Date: %s, Expiration Date: %s", now.toString(), expDt.toString());
                    logger.Log("TradeEngine", "runTrading", "Holdings Expired", strExpOutput, false);

                    for (int i = 0; i < listCurStocks.size(); i++) {
                        StockHolding stk = listCurStocks.get(i);

                        stockQuote = null;
                        reqStockQuote(client, i, stk.getTicker());
                        Thread.sleep(WAIT_TIME);
                        if (!isStockQuoteValid(stk.getTicker())) {
                            logger.Log("TradeEngine", "runTrading", stk.getTicker(), "Invalid Stock Quote!", true);
                            continue;
                        }

                        //Limit order for 99.5% of value
                        BigDecimal tmpLimitPrice = stockQuote.multiply(new BigDecimal("0.995"));
                        String strValue = String.format("%.2f", tmpLimitPrice.doubleValue());
                        double limitPrice = Double.parseDouble(strValue);

                        int orderID = sdh.getStockOrderID();
                        reqTrade(client, TradeAction.SELL, orderID, stk.getTicker(), stk.getSharesHeld(), limitPrice);
                        Thread.sleep(WAIT_TIME);

                        //Update DB
                        sdh.updateStockTrade(stk.getTicker());
                    }

                    return; //Sold all stocks, done

                } //End If expired holdings
                //Stocks aren't expired yet, done
                else {
                    logger.Log("TradeEngine", "runTrading", "Holdings Not Yet Expired", "", false);
                    return; 
                }
            } //End If holdings > 0
            //No current stock holdings
            else if (HOUR_OF_DAY == 9 || HOUR_OF_DAY == 10) { //9-10AM to buy stocks

                logger.Log("TradeEngine", "runTrading", "No current stock holdings", "", false);
                
                //Honor 3 wait waiting period
                Date lastSale = sdh.getLastStockSaleDate();
                if (lastSale != null) {
                    Calendar calSale = Calendar.getInstance();
                    calSale.setTime(lastSale);

                    final int WAITING_PERIOD = 3;
                    calSale = Utilities.Dates.getTargetDate(calSale, WAITING_PERIOD);

                    //Exit if 3 day waiting period isn't yet met
                    if (calNow.compareTo(calSale) < 0) {
                        logger.Log("TradeEngine", "runTrading", "Waiting for 3 days to trade again", "", false);
                        return;
                    }
                }

                //Get current balance
                reqAccountBalance(client);
                Thread.sleep(WAIT_TIME);
                if (availableFunds == null) {
                    logger.Log("TradeEngine", "runTrading", "Assertion Failed", "Account Balance wasn't retrieved from IB", true);
                    System.exit(6);
                }
                
                //Enter buy orders
                logger.Log("TradeEngine", "runTrading", "Buying Stocks", "", false);

                final BigDecimal CASH_RESERVE = new BigDecimal("100.00");
                BigDecimal divisor = new BigDecimal(String.valueOf(MAX_STOCK_COUNT));
                BigDecimal stkPartition = availableFunds.subtract(CASH_RESERVE).divide(divisor, RoundingMode.DOWN);

                Predictor p = new Predictor();
                Date yesterday = Utilities.Dates.getYesterday();
                List<StockHolding> stkPicks = p.topStockPicks(MAX_STOCK_COUNT, yesterday);
                if (stkPicks.size() != MAX_STOCK_COUNT) {
                    logger.Log("TradeEngine", "runTrading", "Stock Picks Number Mismatch", "Requested: " + MAX_STOCK_COUNT + ", Received: " + stkPicks.size(), false);
                    return;
                }

                for (int i = 0; i < MAX_STOCK_COUNT; i++) {
                    String ticker = stkPicks.get(i).getTicker();

                    stockQuote = null;
                    reqStockQuote(client, i, ticker);
                    Thread.sleep(WAIT_TIME);
                    if (!isStockQuoteValid(ticker)) {
                        logger.Log("TradeEngine", "runTrading", ticker, "Invalid Stock Quote!", true);
                        continue;
                    }

                    //Pay no more than 0.5% over Market
                    BigDecimal tmpLimitPrice = stockQuote.multiply(new BigDecimal("1.005"));
                    String strValue = String.format("%.2f", tmpLimitPrice.doubleValue());
                    double limitPrice = Double.parseDouble(strValue);

                    int numShares = stkPartition.divide(tmpLimitPrice, RoundingMode.DOWN).intValue(); 

                    int orderID = sdh.getStockOrderID();
                    reqTrade(client, TradeAction.BUY, orderID, ticker, numShares, limitPrice);
                    Thread.sleep(WAIT_TIME);

                    //Save to DB
                    sdh.insertStockTrades(ticker, numShares, new Date(), stkPicks.get(i).getTargetDate());
                }
            }
            
        } finally {
            client.eDisconnect();
        }
    }
    
    private void reqAccountBalance(EClientSocket client) throws Exception {
        
        logger.Log("TradeEngine", "reqAccountBalance", "", "", false);

        client.reqAccountSummary(101, "All", "TotalCashValue");
    }
    
    private EClientSocket connect(final int PORT) throws Exception {
        
        logger.Log("TradeEngine", "connect", "", "", false);
        
        EClientSocket client = new EClientSocket(this);
        client.eConnect("localhost", PORT, 101);

        if (!client.isConnected()) {
            logger.Log("TradeEngine", "connect", "Exception", "Cannot conect to IB API", true);
            System.exit(4);
        }
        
        return client;
    }

    
    @Override
    public void error(Exception e) {

        try {
            logger.Log("TradeEngine", "error", "Exception", e.toString(), true);
        } catch(Exception exc) {
            System.out.println("Exception: error");
        }
   }

    @Override
    public void error(String str) {

        try {
            logger.Log("TradeEngine", "error", "Exception", str, true);
        } catch(Exception exc) {
            System.out.println("Exception: error");
        }
    }

    @Override
    public void error(int id, int errorCode, String errorMsg) {

        String outputMsg = String.format("ID = %d, Error Code = %d, Msg = %s", id, errorCode, errorMsg);
        
        try {
            logger.Log("TradeEngine", "error", "Exception", errorMsg, true);
        } catch(Exception exc) {
            System.out.println("Exception: error");
        }
    }

    @Override
    public void connectionClosed() {

        try {
            logger.Log("TradeEngine", "connectionClosed", "", "", false);
        } catch(Exception exc) {
            System.out.println("Exception: connectionClosed");
        }
    }

}
