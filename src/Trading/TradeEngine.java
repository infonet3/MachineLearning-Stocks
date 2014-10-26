package Trading;

import Modeling.Predictor;
import StockData.StockDataHandler;
import StockData.StockHolding;
import Trading.IB.*;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public class TradeEngine implements EWrapper {

    //Fields
    BigDecimal availableFunds;
    BigDecimal stockQuote;
    int orderID = 1;
    List<StockHolding> listHoldings = new ArrayList<>();
    
    //Methods
    public TradeEngine() throws Exception {
    }
    
    @Override
    public void tickPrice(int tickerId, int field, double price, int canAutoExecute) {
        System.out.printf("tickPrice: field: %d, price: %f %n", field, price);
    }

    @Override
    public void tickSize(int tickerId, int field, int size) {
        System.out.println("tickSize");
    }

    @Override
    public void tickOptionComputation(int tickerId, int field, double impliedVol, double delta, double optPrice, double pvDividend, double gamma, double vega, double theta, double undPrice) {
        System.out.println("tickOptionComputation");
    }

    @Override
    public void tickGeneric(int tickerId, int tickType, double value) {
        System.out.printf("tickGeneric, type: %d, value: %f %n", tickType, value);
    }

    @Override
    public void tickString(int tickerId, int tickType, String value) {
        System.out.println("tickString");
    }

    @Override
    public void tickEFP(int tickerId, int tickType, double basisPoints, String formattedBasisPoints, double impliedFuture, int holdDays, String futureExpiry, double dividendImpact, double dividendsToExpiry) {
        System.out.println("tickEFP");
    }

    @Override
    public void orderStatus(int orderId, String status, int filled, int remaining, double avgFillPrice, int permId, int parentId, double lastFillPrice, int clientId, String whyHeld) {
        System.out.println("orderStatus");
    }

    @Override
    public void openOrder(int orderId, Contract contract, Order order, OrderState orderState) {
        String output = String.format("Order: %d, Ticker: %s, Order: %d, State: %s", orderId, contract.m_symbol, order.m_totalQuantity, orderState.m_status);
        System.out.println(output);
    }

    @Override
    public void openOrderEnd() {
        System.out.println("openOrderEnd");
    }

    @Override
    public void updateAccountValue(String key, String value, String currency, String accountName) {
        System.out.println("updateAccountValue");
    }

    @Override
    public void updatePortfolio(Contract contract, int position, double marketPrice, double marketValue, double averageCost, double unrealizedPNL, double realizedPNL, String accountName) {
        System.out.println("updatePortfolio");
    }

    @Override
    public void updateAccountTime(String timeStamp) {
        System.out.println("updateAccountTime");
    }

    @Override
    public void accountDownloadEnd(String accountName) {
        System.out.println("accountDownloadEnd");
    }

    @Override
    public void nextValidId(int orderId) {
        System.out.println("nextValidId");
    }

    @Override
    public void contractDetails(int reqId, ContractDetails contractDetails) {
        System.out.println("contractDetails");
    }

    @Override
    public void bondContractDetails(int reqId, ContractDetails contractDetails) {
        System.out.println("bondContractDetails");
    }

    @Override
    public void contractDetailsEnd(int reqId) {
        System.out.println("contractDetailsEnd");
    }

    @Override
    public void execDetails(int reqId, Contract contract, Execution execution) {
        System.out.println("execDetails");
    }

    @Override
    public void execDetailsEnd(int reqId) {
        System.out.println("execDetailsEnd");
    }

    @Override
    public void updateMktDepth(int tickerId, int position, int operation, int side, double price, int size) {
        System.out.println("updateMktDepth");
    }

    @Override
    public void updateMktDepthL2(int tickerId, int position, String marketMaker, int operation, int side, double price, int size) {
        System.out.println("updateMktDepthL2");
    }

    @Override
    public void updateNewsBulletin(int msgId, int msgType, String message, String origExchange) {
        System.out.println("updateNewsBulletin");
    }

    @Override
    public void managedAccounts(String accountsList) {
        System.out.println("managedAccounts");
    }

    @Override
    public void receiveFA(int faDataType, String xml) {
        System.out.println("receiveFA");
    }

    @Override
    public void historicalData(int reqId, String date, double open, double high, double low, double close, int volume, int count, double WAP, boolean hasGaps) {
        System.out.println("historicalData");
    }

    @Override
    public void scannerParameters(String xml) {
        System.out.println("scannerParameters");
    }

    @Override
    public void scannerData(int reqId, int rank, ContractDetails contractDetails, String distance, String benchmark, String projection, String legsStr) {
        System.out.println("scannerData");
    }

    @Override
    public void scannerDataEnd(int reqId) {
        System.out.println("scannerDataEnd");
    }

    @Override
    public void realtimeBar(int reqId, long time, double open, double high, double low, double close, long volume, double wap, int count) {
        System.out.println("realtimeBar");
    }

    @Override
    public void currentTime(long time) {
        System.out.println("currentTime");
    }

    @Override
    public void fundamentalData(int reqId, String data) {
        System.out.println("fundamentalData");
    }

    @Override
    public void deltaNeutralValidation(int reqId, UnderComp underComp) {
        System.out.println("deltaNeutralValidation");
    }

    @Override
    public void tickSnapshotEnd(int reqId) {
        System.out.println("tickSnapshotEnd");
    }

    @Override
    public void marketDataType(int reqId, int marketDataType) {
        System.out.println("marketDataType");
    }

    @Override
    public void commissionReport(CommissionReport commissionReport) {
        System.out.println("commissionReport");
    }

    @Override
    public void position(String account, Contract contract, int pos, double avgCost) {

        String ticker = contract.m_symbol;
        String output = String.format("Account: %s, Pos: %d, Cost: %f", account, pos, avgCost);
        System.out.println(output);
        
        StockHolding holding = new StockHolding(ticker, pos, null);
        listHoldings.add(holding);
    }

    @Override
    public void positionEnd() {
        System.out.println("positionEnd");
    }

    @Override
    public void accountSummary(int reqId, String account, String tag, String value, String currency) {
        
        final String AVAIL_FUNDS = "TotalCashValue";
        if (tag.equals(AVAIL_FUNDS)) {
            availableFunds = new BigDecimal(value);
        }
        
        String output = String.format("ReqID: %d, Acct: %s, Tag: %s, Value: %s, Currency: %s", reqId, account, tag, value, currency);
        System.out.println(output);
    }

    @Override
    public void accountSummaryEnd(int reqId) {
        System.out.println("accountSummaryEnd");
    }

    @Override
    public void verifyMessageAPI(String apiData) {
        System.out.println("verifyMessageAPI");
    }

    @Override
    public void verifyCompleted(boolean isSuccessful, String errorText) {
        System.out.println("verifyCompleted");
    }

    @Override
    public void displayGroupList(int reqId, String groups) {
        System.out.println("displayGroupList");
    }

    @Override
    public void displayGroupUpdated(int reqId, String contractInfo) {
        System.out.println("displayGroupUpdated");
    }
    
    private void reqStockQuote(EClientSocket client, String quote) {
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
        c.m_primaryExch = "";
        c.m_secIdType = "";
        c.m_secId = null;
    	client.reqMktData(1, c, "", true, Collections.<TagValue>emptyList());
    }
    
    private void reqBuyStock(EClientSocket client, String ticker, int numShares, double curPrice) {

        Contract contract = new Contract();
        contract.m_symbol = ticker.toUpperCase();
        contract.m_secType = "STK";
        contract.m_expiry = "";
        contract.m_strike = 0;
        contract.m_right = "None";
        contract.m_multiplier = String.valueOf(numShares);
        contract.m_exchange = "SMART";
        contract.m_primaryExch = "ISLAND";
        contract.m_currency = "USD";
        
        Order order = new Order();
        order.m_clientId = 1;
        order.m_orderId = 3;
        order.m_permId = 0;
        order.m_parentId = 0;
        order.m_account = "DU205493";
        order.m_action = "BUY";
        order.m_totalQuantity = numShares;
        order.m_orderType = "LMT";
        order.m_lmtPrice = curPrice * 1.05; //Max 5% above current price
        order.m_tif = "DAY";
        
        client.placeOrder(orderID++, contract, order);
    }

    private void reqSellStock(EClientSocket client, String ticker, int numShares, double curPrice) {

        Contract contract = new Contract();
        contract.m_symbol = ticker.toUpperCase();
        contract.m_secType = "STK";
        contract.m_expiry = "";
        contract.m_strike = 0;
        contract.m_right = "None";
        contract.m_multiplier = String.valueOf(numShares);
        contract.m_exchange = "SMART";
        contract.m_primaryExch = "ISLAND";
        contract.m_currency = "USD";
        
        Order order = new Order();
        order.m_clientId = 1;
        order.m_orderId = 3;
        order.m_permId = 0;
        order.m_parentId = 0;
        order.m_account = "DU205493";
        order.m_action = "SELL";
        order.m_totalQuantity = numShares;
        order.m_orderType = "LMT";
        order.m_lmtPrice = curPrice * .95; //Max 5% above current price
        order.m_tif = "DAY";
        
        client.placeOrder(orderID++, contract, order);
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
    
    public void runTrading(final int MAX_STOCK_COUNT) throws Exception {

        EClientSocket client = connect();

        //Get current holdings
        client.reqPositions();
        Thread.sleep(1000);
        int holdingCount = listHoldings.size();
        
        //Find expiration from DB
        

        
        reqAccountBalance(client);
        
        //First see what holdings we have
        StockDataHandler sdh = new StockDataHandler();
        List<StockHolding> listHoldings = sdh.getCurrentStockHoldings();
        int stockHoldingCount = listHoldings.size();
        
        //See if any currently held stocks are expired
        Calendar today = Calendar.getInstance();
        Calendar expDt = Calendar.getInstance();
        
        for (StockHolding stk : listHoldings) {
            expDt.setTime(stk.getTargetDate());
            double curPrice = 123.45;
            
            //Generate sell order if expired
            if (expDt.compareTo(today) >= 0) {
                reqStockQuote(client, stk.getTicker());
                reqSellStock(client, stk.getTicker(), stk.getSharesHeld(), curPrice);
            }
        }
        
        //Get positions held at IB
        client.reqPositions();
        Thread.sleep(2000);
        System.out.println("Stocks Held = " + stockHoldingCount);

        //Can we purchase stocks - must be in ALL cash
        if (stockHoldingCount == 0) {
            reqAccountBalance(client);
            Thread.sleep(2000);
            System.out.println("Funds = " + availableFunds);

            //Get List of Stocks forecasted to go up
            Date yesterday = Utilities.Dates.getYesterday();
            Predictor p = new Predictor();
            List<StockHolding> stkPicks = p.topStockPicks(MAX_STOCK_COUNT, yesterday);

            for (int i = 0; i < MAX_STOCK_COUNT; i++) {
                //BUY ORDERS
            }
        }

        
        
        //Open Orders
        client.reqAllOpenOrders();
        
        //If needed
        //client.reqGlobalCancel();
        
        //Stop Session
        client.eDisconnect();
    }
    
    private void reqAccountBalance(EClientSocket client) throws Exception {
        client.reqAccountSummary(101, "All", "TotalCashValue");
    }
    
    private EClientSocket connect() throws Exception {
        
        EClientSocket client = new EClientSocket(this);

        for (;;) {
            client.eConnect("localhost", 7496, 101);
            //client.eConnect("localhost", 4001, 101);

            if (client.isConnected())
                break;
            
            Thread.sleep(1000);
        }
        
        return client;
    }

    
    @Override
    public void error(Exception e) {
        System.out.println("error: desc: " + e);
    }

    @Override
    public void error(String str) {
        System.out.println("error: " + str);
    }

    @Override
    public void error(int id, int errorCode, String errorMsg) {
        System.out.println("error: " + errorMsg);
    }

    @Override
    public void connectionClosed() {
        System.out.println("connectionClosed");
    }

}
