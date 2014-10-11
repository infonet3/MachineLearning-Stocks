package Trading;

import java.io.FileReader;
import java.util.Properties;
import Trading.IB.*;
import java.util.Collections;

public class TradeEngine implements EWrapper {

    final String CONF_FILE = "settings.conf";
    final String MYSQL_SERVER_HOST;
    final String MYSQL_SERVER_PORT;
    final String MYSQL_SERVER_DB;
    final String MYSQL_SERVER_LOGIN;
    final String MYSQL_SERVER_PASSWORD;
    
    public TradeEngine() throws Exception {
        
        Properties p = new Properties();
        try (FileReader fis = new FileReader(CONF_FILE)) {
            p.load(fis);
            
            MYSQL_SERVER_HOST = p.getProperty("mysql_server_host");
            MYSQL_SERVER_PORT = p.getProperty("mysql_server_port");
            MYSQL_SERVER_DB = p.getProperty("mysql_server_db");
            MYSQL_SERVER_LOGIN = p.getProperty("mysql_server_login");
            MYSQL_SERVER_PASSWORD = p.getProperty("mysql_server_password");
        } 
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
        System.out.println("openOrder");
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
        System.out.println("position");
    }

    @Override
    public void positionEnd() {
        System.out.println("positionEnd");
    }

    @Override
    public void accountSummary(int reqId, String account, String tag, String value, String currency) {
        System.out.println("accountSummary");
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
    
    public void connect() {
        
        EClientSocket client = new EClientSocket(this);

        //client.eConnect("localhost", 7496, 101);
        client.eConnect("localhost", 4001, 101);

        Contract c = new Contract();
        c.m_conId = 0;
        c.m_symbol = "IBM";
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
        
        client.eDisconnect();
        
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
