/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Utilities;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import java.io.FileInputStream;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.util.Properties;

/**
 *
 * @author Matt Jones
 */
public class Logger {

    static final String CONF_FILE = "Resources/settings.conf";

    static String MYSQL_SERVER_HOST;
    static String MYSQL_SERVER_PORT;
    static String MYSQL_SERVER_DB;
    static String MYSQL_SERVER_LOGIN;
    static String MYSQL_SERVER_PASSWORD;
    
    static {
        //Load the file settings
        Properties p = new Properties();
        try (FileInputStream fis = new FileInputStream(CONF_FILE)) {
            p.load(fis);
            MYSQL_SERVER_HOST = p.getProperty("mysql_server_host");
            MYSQL_SERVER_PORT = p.getProperty("mysql_server_port");
            MYSQL_SERVER_DB = p.getProperty("mysql_server_db");
            MYSQL_SERVER_LOGIN = p.getProperty("mysql_server_login");
            MYSQL_SERVER_PASSWORD = p.getProperty("mysql_server_password");
        }
        catch (Exception exc) {
            System.out.println("Class Logger, Static Initializer, Desc:" + exc);
            
            try {
                Notifications.EmailActions.SendEmail("Can't open configuration file for Logger class", exc.toString());
            } catch(Exception exc2) {
                System.out.println(exc2.toString());
            }

            System.exit(2);
        }
    }
    
    public Logger() {
    }
    
    private Connection getDBConnection() throws Exception {
        MysqlDataSource dataSource = new MysqlDataSource();
        Connection conxn = null;
        
        try {
            dataSource.setServerName(MYSQL_SERVER_HOST);
            dataSource.setPort(Integer.parseInt(MYSQL_SERVER_PORT));
            dataSource.setDatabaseName(MYSQL_SERVER_DB);
            conxn  = dataSource.getConnection(MYSQL_SERVER_LOGIN, MYSQL_SERVER_PASSWORD);

        } catch (Exception exc) {
            Notifications.EmailActions.SendEmail("ML Notification - Database Error", "Cannot connect to DB.  Details: " + exc.toString());
            System.exit(3);
        }
        
        return conxn;
    }
    
    public void Log(String className, String method, String summary, String fullDescription, boolean isError) throws Exception {
        
        String strOutput = String.format("Class: %s, Method: %s, Summary: %s, Description: %s, Error: %s", className, method, summary, fullDescription, String.valueOf(isError));
        System.out.println(strOutput);

        if (isError)
            Notifications.EmailActions.SendEmail("Log Error", strOutput);
        
        try (Connection conxn = getDBConnection();
             CallableStatement stmt = conxn.prepareCall("{call sp_Insert_Log (?, ?, ?, ?, ?)}")) {
            
            stmt.setString(1, className);
            stmt.setString(2, method);
            stmt.setString(3, summary);
            stmt.setString(4, fullDescription);
            stmt.setBoolean(5, isError);

            stmt.executeUpdate();
            
        } catch (Exception exc) {
            System.out.println("Exception in Log");
            throw exc;
        }
        
    }
}
