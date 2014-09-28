package Trading;

import com.etrade.etws.account.Account;
import com.etrade.etws.account.AccountListResponse;
import com.etrade.etws.oauth.sdk.client.IOAuthClient;
import com.etrade.etws.oauth.sdk.client.OAuthClientImpl;
import com.etrade.etws.oauth.sdk.common.Token;
import com.etrade.etws.sdk.client.AccountsClient;
import com.etrade.etws.sdk.client.ClientRequest;
import com.etrade.etws.sdk.client.Environment;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import java.awt.Desktop;
import java.io.FileReader;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class TradeEngine {
    
    final String CONF_FILE = "settings.conf";
    final String OAUTH_CONS_KEY;
    final String OAUTH_CONS_SECRET;
    //final String OAUTH_VERIFY_CODE;
    final String MYSQL_SERVER_HOST;
    final String MYSQL_SERVER_PORT;
    final String MYSQL_SERVER_DB;
    final String MYSQL_SERVER_LOGIN;
    final String MYSQL_SERVER_PASSWORD;
    
    public TradeEngine() throws Exception {
        
        Properties p = new Properties();
        try (FileReader fis = new FileReader(CONF_FILE)) {
            p.load(fis);

            OAUTH_CONS_KEY = p.getProperty("etrade_oauth_consumer_key");
            OAUTH_CONS_SECRET = p.getProperty("etrade_consumer_secret");
            
            MYSQL_SERVER_HOST = p.getProperty("mysql_server_host");
            MYSQL_SERVER_PORT = p.getProperty("mysql_server_port");
            MYSQL_SERVER_DB = p.getProperty("mysql_server_db");
            MYSQL_SERVER_LOGIN = p.getProperty("mysql_server_login");
            MYSQL_SERVER_PASSWORD = p.getProperty("mysql_server_password");
        } 
    }

    private String getVerificationCode() throws Exception {
        Properties p = new Properties();
        String verificationCd = null;
        try (FileReader fis = new FileReader(CONF_FILE)) {
            p.load(fis);
            verificationCd = p.getProperty("etrade_verify_code");
        } 
        
        return verificationCd;
    }
    
    public void enterTrade(TradeAction action, int numShares) throws Exception {
        final String VENDOR = "eTrade";

        //Set Expiration 24 hours out for SANDBOX
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, 1);
        
        //See if a valid token is in the DB
        Token tok = getToken(VENDOR);
        if (tok == null) {
            tok = authenticate();
            saveToken(VENDOR, cal.getTime(), tok);
        }
        
        //Test Code--------------------------------------------------------------------------
        //Retrieve Accounts
        ClientRequest request = new ClientRequest(); // Instantiate ClientRequest
        request.setEnv(Environment.SANDBOX);
        request.setConsumerKey(OAUTH_CONS_KEY);
        request.setConsumerSecret(OAUTH_CONS_SECRET);
        request.setToken(tok.getToken());
        request.setTokenSecret(tok.getSecret());

        try
        {
          AccountsClient account_client = new AccountsClient(request);
          AccountListResponse response = account_client.getAccountList();
          List<Account> alist = response.getResponse();
          Iterator<Account> al = alist.iterator();
          while (al.hasNext()) {
            Account a = al.next();
            System.out.println("===================");
            System.out.println("Account: " + a.getAccountId());
            System.out.println("===================");
          }
        }
        catch (Exception e)
        {
            System.out.println(e);
        }
    }
    
    private Connection getDBConnection() throws Exception {
        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setServerName(MYSQL_SERVER_HOST);
        dataSource.setPort(Integer.parseInt(MYSQL_SERVER_PORT));
        dataSource.setDatabaseName(MYSQL_SERVER_DB);
        return dataSource.getConnection(MYSQL_SERVER_LOGIN, MYSQL_SERVER_PASSWORD);
    }
    
    private void saveToken(String vendor, Date expirationDt, Token tok) throws Exception {
        try (Connection conxn = getDBConnection();
             PreparedStatement stmt = conxn.prepareStatement("{call sp_Insert_Token (?, ?, ?)}")) {
            
            java.sql.Date sqlDt = new java.sql.Date(expirationDt.getTime());
            
            stmt.setString(1, vendor);
            stmt.setDate(2, sqlDt);
            stmt.setObject(3, tok);

            stmt.execute();
        } catch(Exception exc) {
            System.out.println(exc);
            throw exc;
        }
    }
    
    private Token getToken(final String vendor) throws Exception {

        Token tok = null;
        try (Connection conxn = getDBConnection();
             PreparedStatement stmt = conxn.prepareStatement("{call sp_Retrieve_Token (?)}")) {
            
            stmt.setString(1, vendor);
            ResultSet rs = stmt.executeQuery();
            
            if (rs.next()) {
                rs.getDate(1);
                tok = (Token)rs.getObject(2);
            }
        } catch(Exception exc) {
            System.out.println(exc);
            throw exc;
        }
        
        return tok;
    }
    
    private Token authenticate() throws Exception {

        //Obtain a Request Token
        IOAuthClient client = OAuthClientImpl.getInstance(); // Instantiate IOAUthClient
        ClientRequest request = new ClientRequest(); // Instantiate ClientRequest
        request.setEnv(Environment.SANDBOX); // Use sandbox environment
        request.setConsumerKey(OAUTH_CONS_KEY); 
        request.setConsumerSecret(OAUTH_CONS_SECRET); 
        Token token = client.getRequestToken(request); 
        
        String oauth_request_token = token.getToken(); 
        String oauth_request_token_secret = token.getSecret(); 
        request.setToken(oauth_request_token); 
        request.setTokenSecret(oauth_request_token_secret); 
        
        //Obtain Verification Code
        String authorizeURL = null;
        authorizeURL = client.getAuthorizeUrl(request); // E*TRADE authorization URL
        URI uri = new java.net.URI(authorizeURL);
        Desktop desktop = Desktop.getDesktop();
        desktop.browse(uri);
      
        //Obtain Access Token
        String oauth_verify_code = getVerificationCode(); // Should contain the Verification Code received from the authorization step

        request = new ClientRequest(); // Instantiate ClientRequest
        request.setEnv(Environment.SANDBOX); // Use sandbox environment
        request.setConsumerKey(OAUTH_CONS_KEY); 
        request.setConsumerSecret(OAUTH_CONS_SECRET); 
        request.setToken(oauth_request_token); // Set request token
        request.setTokenSecret(oauth_request_token_secret); // Set request-token secret
        request.setVerifierCode(oauth_verify_code); // Set verification code

        // Get access token
        token = client.getAccessToken(request); // Get access-token object
        String oauth_access_token = token.getToken(); // Access token string
        String oauth_access_token_secret = token.getSecret(); // Access token secret    
        
        //Save token to DB
        return token;
    }
}
