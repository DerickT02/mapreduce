package com.derick.mapreduce.map;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.net.Socket;
import com.esotericsoftware.kryo.Kryo;

public class Map {

    public static void main(String[] args) throws InterruptedException, FileNotFoundException, IOException{
        System.out.println("Map class in MapReduce package");
        try{
            Socket socket = new Socket("localhost", 8080);
            

            ArrayList<AbstractMap.SimpleEntry<String, String>> entries = readDataSet("/Users/derickpaulalavazotolentino/Downloads/test.csv");
            sendToReduce(entries, socket);
            
        }
        catch(Exception e){
            e.printStackTrace();
        }


    }

    private static AbstractMap.SimpleEntry<String, String> Mapper(String key, String value) {
        return new AbstractMap.SimpleEntry<>(key, value);
    }

    private static ArrayList<AbstractMap.SimpleEntry<String, String>> readDataSet(String csvPath){
        String line;
        String splitBy = ",";
        ArrayList<AbstractMap.SimpleEntry<String, String>> entries = new ArrayList<>();
        try(BufferedReader br = new BufferedReader(new FileReader(csvPath))){
            while((line = br.readLine()) != null){
                String[] columns = line.split(splitBy);
                System.out.printf("%s %s\n",columns[0], columns[1]);
                AbstractMap.SimpleEntry<String, String> entry = Mapper(columns[0], columns[1]);  
                entries.add(entry);     
            }
            
        } catch (IOException e) {
            e.printStackTrace();
        }
        return entries;
    };

    private static void sendToReduce(ArrayList<AbstractMap.SimpleEntry<String, String>> entries, Socket socket) throws IOException {
        System.out.println("Sending entries to Reduce phase:");
        OutputStream outputStream = socket.getOutputStream();
        Kryo kryo = new Kryo();
        for(AbstractMap.SimpleEntry<String, String> entry : entries){
            System.out.printf("Key: %s, Value: %s\n", entry.getKey(), entry.getValue());
            
        }
        outputStream.flush();
        socket.close();
    };



}
