package Trading;

import java.io.FileReader;
import java.util.Properties;


public class TradeEngine {
    
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

}
