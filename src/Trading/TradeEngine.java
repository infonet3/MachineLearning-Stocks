package Trading;

import com.etrade.etws.account.Account;
import com.etrade.etws.account.AccountListResponse;
import com.etrade.etws.oauth.sdk.client.IOAuthClient;
import com.etrade.etws.oauth.sdk.client.OAuthClientImpl;
import com.etrade.etws.oauth.sdk.common.Token;
import com.etrade.etws.sdk.client.AccountsClient;
import com.etrade.etws.sdk.client.ClientRequest;
import com.etrade.etws.sdk.client.Environment;
import java.awt.Desktop;
import java.net.URI;
import java.util.Iterator;
import java.util.List;

public class TradeEngine {
    public void authenticate() throws Exception {

        String oauth_consumer_key = "ba89a69d661b1fd8900611512d3737dd";
        String oauth_consumer_secret = "c8cf2b679bc292a3c883e1bba6159bfb";

        //Obtain a Request Token
        IOAuthClient client = OAuthClientImpl.getInstance(); // Instantiate IOAUthClient
        ClientRequest request = new ClientRequest(); // Instantiate ClientRequest
        request.setEnv(Environment.SANDBOX); // Use sandbox environment
        request.setConsumerKey(oauth_consumer_key); 
        request.setConsumerSecret(oauth_consumer_secret); 
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
        String oauth_verify_code = "TUCV6"; // Should contain the Verification Code received from the authorization step

        request = new ClientRequest(); // Instantiate ClientRequest
        request.setEnv(Environment.SANDBOX); // Use sandbox environment
        request.setConsumerKey(oauth_consumer_key); 
        request.setConsumerSecret(oauth_consumer_secret); 
        request.setToken(oauth_request_token); // Set request token
        request.setTokenSecret(oauth_request_token_secret); // Set request-token secret
        request.setVerifierCode(oauth_verify_code); // Set verification code

        // Get access token
        token = client.getAccessToken(request); // Get access-token object
        String oauth_access_token = token.getToken(); // Access token string
        String oauth_access_token_secret = token.getSecret(); // Access token secret    
        
        //Retrieve Accounts
        request = new ClientRequest(); // Instantiate ClientRequest

        // Prepare request
        request.setEnv(Environment.SANDBOX);
        request.setConsumerKey(oauth_consumer_key);
        request.setConsumerSecret(oauth_consumer_secret);
        request.setToken(oauth_access_token);
        request.setTokenSecret(oauth_access_token_secret);

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
        }
    }
}
