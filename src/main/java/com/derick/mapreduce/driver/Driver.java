package com.derick.mapreduce.driver;

public class Driver {
    public String hack_license;
    public int passenger_count;
    public Driver(){};

    public Driver(String hack_license, int passenger_count){
        this.hack_license = hack_license;
        this.passenger_count = passenger_count;
    }

    public synchronized void getDriverInfo(){
        System.out.println("Hack License: " + hack_license + ", Passenger Count: " + passenger_count);
    }




}
