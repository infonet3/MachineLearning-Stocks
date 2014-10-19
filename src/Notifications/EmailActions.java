/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Notifications;

import java.util.Date;
import java.util.Properties;
import javax.mail.Message;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

/**
 *
 * @author Matt Jones
 */
public class EmailActions {

    public static void SendEmail(String subject, String message) throws Exception {

        try {
            Properties props = System.getProperties();
            props.put("mail.smtp.host", "smtp.gmail.com");
            props.put("mail.smtp.ssl.enable", true);

            Session session = Session.getInstance(props, null);
            boolean debug = true;

            //DEBUG
            session.setDebug(true);

            Message msg = new MimeMessage(session);
            msg.setSentDate(new Date());
            msg.setFrom(new InternetAddress("auto.trader.phx.az@gmail.com"));
            msg.setRecipients(Message.RecipientType.TO, InternetAddress.parse("mrjones.cs@gmail.com", false));
            msg.setSubject(subject);
            msg.setText(message);

            String login = "auto.trader.phx.az";
            String pwd = "Apex2721";
            Transport.send(msg, login, pwd);
        }
        catch (Exception exc) {
            System.out.println(exc);
        }
    }
}
