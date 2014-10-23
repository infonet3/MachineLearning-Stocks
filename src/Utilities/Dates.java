/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Utilities;

import java.util.Calendar;
import java.util.Date;

/**
 *
 * @author Matt Jones
 */
public class Dates {
    public static Date getYesterday() {

        Calendar today = Calendar.getInstance();
        if (today.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY)
            today.add(Calendar.DATE, -2); //Move back to Friday
        else
            today.add(Calendar.DATE, -1); //Move to yesterday

        return today.getTime();
    }
}
