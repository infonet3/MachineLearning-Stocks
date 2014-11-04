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
    public static Date getYesterday() throws Exception {

        //Move one trading day behind
        Calendar today = Calendar.getInstance();
        if (today.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY) //Move back to Friday
            today.add(Calendar.DATE, -2);
        else if (today.get(Calendar.DAY_OF_WEEK) == Calendar.MONDAY) //Move back to Friday
            today.add(Calendar.DATE, -3); 
        else
            today.add(Calendar.DATE, -1); //Move to yesterday

        //Ensure previous trading day isn't a holiday
        StockDataHandler sdh = new StockDataHandler();
        Map<Date, String> mapHolidays = sdh.getAllHolidays();
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
    
    public static Calendar getTargetDate(final Calendar INPUT_CAL, final int DAYS_OUT) throws Exception {

        Calendar targetDate = Calendar.getInstance();
        targetDate.setTime(INPUT_CAL.getTime());
        int daysInAdvance = 0;
        StockDataHandler sdh = new StockDataHandler();
        Map<Date, String> holidayMap = sdh.getAllHolidays();

        for (;;) {
            
            //Weekend
            if (targetDate.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY || targetDate.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY)
                targetDate.add(Calendar.DATE, 1);
            //Business Days
            else {
                //Holidays
                String holidayCode = holidayMap.get(targetDate.getTime());
                if (holidayCode == null)
                    holidayCode = "";
                
                if (holidayCode.equals("Closed"))
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
