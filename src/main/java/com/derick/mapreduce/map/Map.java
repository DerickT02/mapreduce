package com.derick.mapreduce.map;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.net.Socket;

public class Map {
    public static void main(String[] args) throws InterruptedException, FileNotFoundException, IOException{
        System.out.println("Map class in MapReduce package");
        readDataSet("/Users/derickpaulalavazotolentino/Downloads/test.csv");


    }

    private static AbstractMap.SimpleEntry<String, String> Mapper(String key, String value) {
        return new AbstractMap.SimpleEntry<>(key, value);
    }

    private static void readDataSet(String csvPath){
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
            sendToReduce(entries);
        } catch (IOException e) {
            e.printStackTrace();
        }
    };

    private static void sendToReduce(ArrayList<AbstractMap.SimpleEntry<String, String>> entries){
        System.out.println("Sending entries to Reduce phase:");
        for(AbstractMap.SimpleEntry<String, String> entry : entries){
            
        }
    };



}
