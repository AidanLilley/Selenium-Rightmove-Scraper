package org.example;

import io.github.bonigarcia.wdm.WebDriverManager;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

public class RightmoveScraperRentals {
    static String sql = "";
    static boolean prevComplete = false;

    public static void main(String[] args) {
        ChromeOptions chromeOptions = new ChromeOptions();
        chromeOptions.addArguments("--headless"); // Enable headless mode
        chromeOptions.addArguments("--disable-gpu"); // Disable GPU acceleration
        chromeOptions.addArguments("--disable-extensions"); // Disable browser extensions (optional)
        chromeOptions.addArguments("--no-sandbox"); // Disable sandbox (useful in some environments)
        chromeOptions.addArguments("--disable-dev-shm-usage"); // Disable /dev/shm usage (useful in some environments)
        chromeOptions.addArguments("--disable-setuid-sandbox"); // Disable setuid sandbox (useful in some environments)
        chromeOptions.addArguments("--disable-popup-blocking"); // Disable popup blocking (optional)
        chromeOptions.addArguments("--disable-infobars"); // Disable infobars (optional)
        chromeOptions.addArguments("--disable-notifications"); // Disable notifications (optional)
        chromeOptions.addArguments("--disable-logging"); // Disable logging (optional)
        chromeOptions.addArguments("--mute-audio"); // Mute audio (optional)
        chromeOptions.addArguments("--disable-features=NetworkService");
        WebDriverManager.chromedriver().setup();
        WebDriver threadDriver = new ChromeDriver(chromeOptions);
        try {
            Connection con = DriverManager.getConnection("jdbc:postgresql://localhost:5433/scraperdb","postgres","Scraper123");
            RowSetFactory aFactory = RowSetProvider.newFactory();
            CachedRowSet crsOld = aFactory.createCachedRowSet();
            try{
                sql = "SELECT * FROM rent_data_new WHERE instances >= 1;";
                PreparedStatement selectOldData = con.prepareStatement(sql);
                selectOldData.executeQuery();
                ResultSet rs = selectOldData.getResultSet();
                crsOld.populate(rs);

                sql = "SELECT instances FROM rent_data_new WHERE address = 'Scrape Complete' AND postcode = 'End' AND price = 0 AND housetype = 'End' AND numofbeds = 0 AND numofbaths = 0;";
                PreparedStatement selectIfComplete = con.prepareStatement(sql);
                selectIfComplete.executeQuery();
                ResultSet rsIfComplete = selectIfComplete.getResultSet();
                if (rsIfComplete.next()) {
                    prevComplete = true;
                }
            }catch (Exception e){
                System.out.println("Error: "+ e);
            }
            sql = "DELETE FROM rent_data_new;";
            PreparedStatement clearOldData = con.prepareStatement(sql);
            clearOldData.execute();
            sql = "INSERT INTO rent_data_new VALUES (?,?,?,?,?,?,?);";
            PreparedStatement insertNewData = con.prepareStatement(sql);
            for(int i=1; i<=2979;i++) {
                try {
                    System.out.println("Scraping page: " + i);
                    String url = "https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=OUTCODE%5E"+i+"&numberOfPropertiesPerPage=500&propertyTypes=&mustHave=&dontShow=&furnishTypes=&keywords=";
                    threadDriver.get(url);
                    Document doc = Jsoup.parse(threadDriver.getPageSource());
                    doc.select(".propertyCard--featured").remove();
                    Elements propertyListings = doc.select(".propertyCard");

                    String postCodeValue = "Null";
                    if(!(doc.select(".input").isEmpty())) {
                        postCodeValue = threadDriver.findElement(By.cssSelector(".input")).getAttribute("value");

                    }
                    if(propertyListings.size()>499) {
                        System.out.println("Scraping page: " + i + " page 2");
                        url = "https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=OUTCODE%5E"+i+"&numberOfPropertiesPerPage=500&index=500&propertyTypes=&mustHave=&dontShow=&furnishTypes=&keywords=";
                        threadDriver.get(url);
                        doc = Jsoup.parse(threadDriver.getPageSource());
                        doc.select(".propertyCard--featured").remove();
                        propertyListings.addAll(doc.select(".propertyCard"));
                    }
                    Set<List<String>> uniqueStrings = new HashSet<>();
                    for (Element listing : propertyListings) {
                        try{
                            String houseType = "Null";
                            int numOfBeds = 0;
                            int numOfBaths = 0;
                            int instances = 1;
                            String price = listing.select(".propertyCard-priceValue").text();
                            String cleanedCurrency = price.replaceAll("[^\\d.]", "");
                            double doubleValue = Double.parseDouble(cleanedCurrency);
                            int intValue = (int) doubleValue;
                            String location = listing.select(".propertyCard-address").text();
                            String cleanedLocation = location.replaceAll(",", "");
                            Elements houseInfo = listing.select(".property-information").select("span");
                            for(int k=0; k<houseInfo.size();k++) {
                                if((k==0) && (houseInfo.get(k).hasClass("text"))) {
                                    houseType = houseInfo.get(k).text();
                                } else if ((houseInfo.get(k).hasClass("bed-icon"))&&(k+1<houseInfo.size())) {
                                    numOfBeds = Integer.parseInt(houseInfo.get(k+1).text());
                                } else if ((houseInfo.get(k).hasClass("bathroom-icon"))&&(k+1<houseInfo.size())) {
                                    numOfBaths = Integer.parseInt(houseInfo.get(k+1).text());
                                }
                            }
                            if (!uniqueStrings.add(Arrays.asList(cleanedLocation, postCodeValue, String.valueOf(intValue), houseType, String.valueOf(numOfBeds), String.valueOf(numOfBaths)))) {
                                PreparedStatement selectData = con.prepareStatement("SELECT instances FROM rent_data_new WHERE address = ? AND postcode = ? AND price = ? AND housetype = ? AND numofbeds = ? AND numofbaths = ?;");
                                selectData.clearParameters();
                                selectData.setString(1,cleanedLocation);
                                selectData.setString(2,postCodeValue);
                                selectData.setInt(3,intValue);
                                selectData.setString(4,houseType);
                                selectData.setInt(5,numOfBeds);
                                selectData.setInt(6,numOfBaths);
                                selectData.executeQuery();
                                ResultSet rs = selectData.getResultSet();
                                while (rs.next()) {
                                    instances = rs.getInt("instances");
                                }
                                instances++;
                                PreparedStatement updateData = con.prepareStatement("UPDATE rent_data_new SET instances = ? WHERE address = ? AND postcode = ? AND price = ? AND housetype = ? AND numofbeds = ? AND numofbaths = ?;");
                                updateData.clearParameters();
                                updateData.setInt(1,instances);
                                updateData.setString(2,cleanedLocation);
                                updateData.setString(3,postCodeValue);
                                updateData.setInt(4,intValue);
                                updateData.setString(5,houseType);
                                updateData.setInt(6,numOfBeds);
                                updateData.setInt(7,numOfBaths);
                                updateData.execute();
                            } else {
                                insertNewData.clearParameters();
                                insertNewData.setString(1,cleanedLocation);
                                insertNewData.setString(2,postCodeValue);
                                insertNewData.setInt(3,intValue);
                                insertNewData.setString(4,houseType);
                                insertNewData.setInt(5,numOfBeds);
                                insertNewData.setInt(6,numOfBaths);
                                insertNewData.setInt(7,instances);
                                insertNewData.execute();
                            }
                        }catch(Exception e){
                            System.out.println("Element not found in second for loop: " + e.getMessage());
                        }
                    }
                } catch (Exception e) {
                    System.out.println("Element not found in first for loop: " + e.getMessage());
                }

            }
            insertNewData.clearParameters();
            insertNewData.setString(1,"Scrape Complete");
            insertNewData.setString(2,"End");
            insertNewData.setInt(3,0);
            insertNewData.setString(4,"End");
            insertNewData.setInt(5,0);
            insertNewData.setInt(6,0);
            insertNewData.setInt(7,0);
            insertNewData.execute();
            if(prevComplete) {
                CachedRowSet crsNew = aFactory.createCachedRowSet();
                Set<List> newDataSet = new HashSet<>();
                try{
                    sql = "SELECT * FROM rent_data_new WHERE instances >= 1;";
                    PreparedStatement selectNewData = con.prepareStatement(sql);
                    selectNewData.executeQuery();
                    ResultSet rsNew = selectNewData.getResultSet();
                    crsNew.populate(rsNew);
                    while (crsNew.next()) {
                        newDataSet.add(Arrays.asList(crsNew.getString("address"),crsNew.getString("postcode"),crsNew.getInt("price"),crsNew.getString("housetype"),crsNew.getInt("numofbeds"),crsNew.getInt("numofbaths"),crsNew.getInt("instances")));
                    }
                    while (crsOld.next()) {
                        if(newDataSet.add(Arrays.asList(crsOld.getString("address"),crsOld.getString("postcode"),crsOld.getInt("price"),crsOld.getString("housetype"),crsOld.getInt("numofbeds"),crsOld.getInt("numofbaths"),crsOld.getInt("instances")))) {
                            int instanceInNew = 0;
                            int numOfSold;
                            PreparedStatement checkInstData = con.prepareStatement("SELECT instances FROM rent_data_new WHERE address = ? AND postcode = ? AND price = ? AND housetype = ? AND numofbeds = ? AND numofbaths = ?;");
                            checkInstData.clearParameters();
                            checkInstData.setString(1,crsOld.getString("address"));
                            checkInstData.setString(2,crsOld.getString("postcode"));
                            checkInstData.setInt(3, crsOld.getInt("price"));
                            checkInstData.setString(4,crsOld.getString("housetype"));
                            checkInstData.setInt(5,crsOld.getInt("numofbeds"));
                            checkInstData.setInt(6,crsOld.getInt("numofbaths"));
                            checkInstData.executeQuery();
                            ResultSet rsInst = checkInstData.getResultSet();
                            if(rsInst.next()) {
                                instanceInNew = rsInst.getInt("instances");
                            }
                            if(instanceInNew < crsOld.getInt("instances")) {
                                numOfSold = crsOld.getInt("instances") - instanceInNew;
                                PreparedStatement checkHistoryData = con.prepareStatement("SELECT instances FROM rent_data_historical WHERE address = ? AND postcode = ? AND price = ? AND housetype = ? AND numofbeds = ? AND numofbaths = ?;");
                                checkHistoryData.clearParameters();
                                checkHistoryData.setString(1,crsOld.getString("address"));
                                checkHistoryData.setString(2,crsOld.getString("postcode"));
                                checkHistoryData.setInt(3, crsOld.getInt("price"));
                                checkHistoryData.setString(4,crsOld.getString("housetype"));
                                checkHistoryData.setInt(5,crsOld.getInt("numofbeds"));
                                checkHistoryData.setInt(6,crsOld.getInt("numofbaths"));
                                checkHistoryData.executeQuery();
                                ResultSet rsHistory = checkHistoryData.getResultSet();
                                if (rsHistory.next()) {
                                    int instancesHistory = rsHistory.getInt("instances");
                                    instancesHistory = instancesHistory + numOfSold;
                                    PreparedStatement updateHistoryData = con.prepareStatement("UPDATE rent_data_historical SET instances = ? WHERE address = ? AND postcode = ? AND price = ? AND housetype = ? AND numofbeds = ? AND numofbaths = ?;");
                                    updateHistoryData.clearParameters();
                                    updateHistoryData.setInt(1,instancesHistory);
                                    updateHistoryData.setString(2,crsOld.getString("address"));
                                    updateHistoryData.setString(3,crsOld.getString("postcode"));
                                    updateHistoryData.setInt(4,crsOld.getInt("price"));
                                    updateHistoryData.setString(5,crsOld.getString("housetype"));
                                    updateHistoryData.setInt(6,crsOld.getInt("numofbeds"));
                                    updateHistoryData.setInt(7,crsOld.getInt("numofbaths"));
                                    updateHistoryData.execute();
                                } else {
                                    PreparedStatement insertHistoryData = con.prepareStatement("INSERT INTO rent_data_historical VALUES (?,?,?,?,?,?,?)");
                                    insertHistoryData.clearParameters();
                                    insertHistoryData.setString(1,crsOld.getString("address"));
                                    insertHistoryData.setString(2,crsOld.getString("postcode"));
                                    insertHistoryData.setInt(3,crsOld.getInt("price"));
                                    insertHistoryData.setString(4,crsOld.getString("housetype"));
                                    insertHistoryData.setInt(5,crsOld.getInt("numofbeds"));
                                    insertHistoryData.setInt(6,crsOld.getInt("numofbaths"));
                                    insertHistoryData.setInt(7,numOfSold);
                                    insertHistoryData.execute();
                                }
                            }
                        }
                    }
                }catch (Exception e){
                    System.out.println("Error: "+ e);
                }
            }
        } catch (Exception e) {
            System.out.println("Element not found: " + e.getMessage());
        } finally {
            threadDriver.quit();
        }
    }
}