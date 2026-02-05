package com.derick.mapreduce.map;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;

public class Map {
    public static void main(String[] args) {
        System.out.println("Map class in MapReduce package");
        readDataSet("/Users/derickpaulalavazotolentino/Downloads/test.csv");

    }

    private ArrayList<AbstractMap.SimpleEntry<String, String>> Mapper() {
        return new ArrayList<>();
    }

    private static void readDataSet(String csvPath){
        String line;
        String splitBy = ",";
        try(BufferedReader br = new BufferedReader(new FileReader(csvPath))){
            while((line = br.readLine()) != null){
                String[] columns = line.split(splitBy);
                System.out.printf("%s %s\n",columns[0], columns[1]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    };

    private void sendToReduce(){};



}
