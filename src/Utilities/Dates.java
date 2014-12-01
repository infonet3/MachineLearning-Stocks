/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Utilities;

import StockData.StockDataHandler;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

/**
 *
 * @author Matt Jones
 */
public class Dates {
    
    Map<Date, String> mapHolidays;
    
    public Dates() throws Exception {
    
        StockDataHandler sdh = new StockDataHandler();
        mapHolidays = sdh.getAllHolidays();
    }
    
    public Date getYesterday() throws Exception {

        //Move one trading day behind
        Calendar today = Calendar.getInstance();
        if (today.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY) //Move back to Friday
            today.add(Calendar.DATE, -2);
        else if (today.get(Calendar.DAY_OF_WEEK) == Calendar.MONDAY) //Move back to Friday
            today.add(Calendar.DATE, -3); 
        else
            today.add(Calendar.DATE, -1); //Move to yesterday

        //Ensure previous trading day isn't a holiday
        String holidayCd = mapHolidays.get(today.getTime());
        if (holidayCd == null) 
            holidayCd = "";
        
        if (holidayCd.equals("Closed")) {
            if (today.get(Calendar.DAY_OF_WEEK) == Calendar.MONDAY)
                today.add(Calendar.DATE, -3); 
            else
                today.add(Calendar.DATE, -1); 
        }
        
        return today.getTime();
    }
    
    public Calendar getTargetDate(Calendar inputCal, final int DAYS_OUT) throws Exception {

        //Convert input date to cancel out hour, min, sec, and ms
        inputCal.set(Calendar.AM_PM, Calendar.AM);
        inputCal.set(Calendar.HOUR, 0);
        inputCal.set(Calendar.MINUTE, 0);
        inputCal.set(Calendar.SECOND, 0);
        inputCal.set(Calendar.MILLISECOND, 0);
        
        //Now derive the target sale date
        Calendar targetDate = Calendar.getInstance();
        targetDate.setTime(inputCal.getTime());
        int daysInAdvance = 0;

        for (;;) {
            
            //Weekend
            if (targetDate.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY || targetDate.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY)
                targetDate.add(Calendar.DATE, 1);
            //Business Days
            else {
                //Holidays
                Date dt = targetDate.getTime();
                String holidayCode = mapHolidays.get(dt);
                if (holidayCode != null && holidayCode.equals("Closed"))
                    targetDate.add(Calendar.DATE, 1);
                else {
                    targetDate.add(Calendar.DATE, 1);
                    daysInAdvance++;
                }
            }

            //Have we proceeded far enough into the future
            if (daysInAdvance == DAYS_OUT)
                break;
        }
        
        return targetDate;
    }

}
