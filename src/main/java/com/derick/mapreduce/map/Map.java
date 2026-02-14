package com.derick.mapreduce.map;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import com.derick.mapreduce.driver.Driver;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;


public class Map {
    
    private static BlockingQueue<String> messageQueue;
    private static ConcurrentHashMap<String, Driver> driverMappings;
    private static ArrayList<Driver> drivers;

    public static void main(String[] args) throws InterruptedException, FileNotFoundException, IOException{
        System.out.println("Map class in MapReduce package");
        try{
            Socket socket = new Socket("localhost", 8000);
            
            messageQueue = new LinkedBlockingQueue<String>();
            driverMappings = new ConcurrentHashMap<String, Driver>();
            readDataSet("/Users/derickpaulalavazotolentino/Downloads/trip_data/trip_data_1.csv");
            updateDriverMappings();
            drivers = new ArrayList<>(driverMappings.values());
            sendToReduce(socket);
            
        }
        catch(Exception e){
            e.printStackTrace();
        }


    }


    private static void readDataSet(String csvPath) throws FileNotFoundException, IOException, InterruptedException{
        String line;
        try(BufferedReader br = new BufferedReader(new FileReader(csvPath))){
            br.readLine();
            while((line = br.readLine()) != null){
                messageQueue.add(line);
                
            }
            messageQueue.put("EOF");
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    };

    private static void updateDriverMappings() throws InterruptedException{
        while(true){
            String line = messageQueue.take();
            if("EOF".equals(line)) break;
            try{
                String[] tripRow = line.split(",");
                String driverId = tripRow[1];
                int tripTimeInSeconds = Integer.parseInt(tripRow[8]);
                double tripDistance = Double.parseDouble(tripRow[9]);
                driverMappings.compute(driverId, (id, driver) -> {
                    if(driver == null){
                        driver = new Driver(id);
                    }
                    driver.reportTrip(tripTimeInSeconds, tripDistance);
                    return driver;
                });
            }
            catch(IllegalArgumentException e){}
            
            
        }
        return;

    }



    private static void sendToReduce(Socket socket) throws IOException {
        System.out.println("Sending entries to Reduce phase:");

        Output outputStream =new Output(socket.getOutputStream());
        outputStream.writeInt(drivers.size());
        Kryo kryo = new Kryo();
        kryo.register(Driver.class);
        for(Driver driver: drivers){
            kryo.writeObject(outputStream, driver);
        }
        outputStream.flush();
        socket.close();
    };



}
