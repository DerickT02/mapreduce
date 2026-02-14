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

import com.derick.mapreduce.driver.Driver;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;


public class Map {
    
    private static BlockingQueue<String> messageQueue;

    public static void main(String[] args) throws InterruptedException, FileNotFoundException, IOException{
        System.out.println("Map class in MapReduce package");
        try{
            Socket socket = new Socket("localhost", 8000);
            
            messageQueue = new LinkedBlockingQueue<String>();
            readDataSet("/Users/derickpaulalavazotolentino/Downloads/trip_data/trip_data_1.csv");
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
                messageQueue.put(line);
            }
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    };

    private static void sendToReduce(Socket socket) throws IOException {
        System.out.println("Sending entries to Reduce phase:");

        Output outputStream =new Output(socket.getOutputStream());
        Kryo kryo = new Kryo();
        kryo.register(Driver.class);
        for(String line: messageQueue){
            //kryo.writeObject(outputStream, kryo);
            String[] tripRow = line.split(",");
            Driver driver = new Driver(tripRow[1], Integer.parseInt(tripRow[7]));
            kryo.writeObject(outputStream, driver);
        }
        outputStream.flush();
        socket.close();
    };



}
