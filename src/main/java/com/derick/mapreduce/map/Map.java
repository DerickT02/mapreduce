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
import java.util.concurrent.LinkedBlockingQueue;

import com.derick.mapreduce.trip.*;
import com.esotericsoftware.kryo.Kryo;


public class Map {
    
    private static BlockingQueue<String> messageQueue;

    public static void main(String[] args) throws InterruptedException, FileNotFoundException, IOException{
        System.out.println("Map class in MapReduce package");
        try{
            
            messageQueue = new LinkedBlockingQueue<String>();
            readDataSet("/Users/derickpaulalavazotolentino/Downloads/trip_data/trip_data_1.csv");
            
        }
        catch(Exception e){
            e.printStackTrace();
        }


    }

    private static AbstractMap.SimpleEntry<String, String> Mapper(String key, String value) {
        return new AbstractMap.SimpleEntry<>(key, value);
    }

    private static void readDataSet(String csvPath) throws FileNotFoundException, IOException, InterruptedException{
        String line;
        String splitBy = ",";
        ArrayList<AbstractMap.SimpleEntry<String, String>> entries = new ArrayList<>();
        try(BufferedReader br = new BufferedReader(new FileReader(csvPath))){
            while((line = br.readLine()) != null){
                System.out.println(line);
                messageQueue.put(line);
            }
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    };

    private static void sendToReduce(Socket socket) throws IOException {
        System.out.println("Sending entries to Reduce phase:");
        OutputStream outputStream = socket.getOutputStream();
        Kryo kryo = new Kryo();
        kryo.register(Trip.class);
        for(String line: messageQueue){
            //kryo.writeObject(outputStream, kryo);
        }
        outputStream.flush();
        socket.close();
    };



}
