package com.derick.mapreduce.driver;

public class Driver implements Comparable<Driver> {
    private String hack_license;
    private int trip_time_in_secs;
    private double trip_distance;
    public Driver(){};

    public Driver(String hack_license){
        this.hack_license = hack_license;
    }

    public Driver(String hack_license, int trip_time_in_secs, double trip_distance) {
        this.hack_license = hack_license;
        this.trip_time_in_secs = trip_time_in_secs;
        this.trip_distance = trip_distance;
    }

    public synchronized void getDriverInfo(){
        System.out.println("Hack License: " + hack_license + ". Trip time in seconds: " + this.trip_time_in_secs + ". Distance: " + this.trip_distance);
    }

    public void setId(String id){
        this.hack_license = id;
    }

    public String getId(){
        return this.hack_license;
    }

    public int getTripTime(){
        return this.trip_time_in_secs;
    }

    public double getTripDistance(){
        return this.trip_distance;
    }

    public synchronized void reportTrip(int tts, double td){
        this.trip_time_in_secs += tts;
        this.trip_distance += td;

    }

    @Override
    public int compareTo(Driver other) {
        if (other == null) return 1;
        return Double.compare(other.trip_distance, this.trip_distance);
    }




}
