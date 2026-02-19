package com.derick.mapreduce.reduce;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.derick.mapreduce.driver.Driver;


public class Reduce {
   static public int port = 8000;
   static public ConcurrentHashMap<String, Integer> driverTripMap;
   static public ConcurrentHashMap<String, Double> driverDistanceMap;
   public static void main(String[] args) throws IOException, InterruptedException{
      try{
         if(args.length > 0){
            port = Integer.parseInt(args[0]);
         }
          driverTripMap = new ConcurrentHashMap<String, Integer>();
          driverDistanceMap = new ConcurrentHashMap<String, Double>();
          ServerSocket serverSocket = new ServerSocket(port);
          System.out.println("Reduce class in MapReduce package");  
          Socket socket = serverSocket.accept();
          Thread clientThread = allocateThreadForSocket(socket); 
          clientThread.start();
          clientThread.join();

          serverSocket.close();
      }
      catch(IOException e){
          System.out.println(e);
      }
   }

   private static void handleClient(Socket socket) throws IOException{
      Kryo kryo = new Kryo();
      kryo.register(Driver.class);
      Input input = new Input(socket.getInputStream());

      while (input.readBoolean()) {                // true -> read next object
         Driver driver = kryo.readObject(input, Driver.class);
         driver.getDriverInfo();
      }

      input.close();
      socket.close();
      }

   private static Thread allocateThreadForSocket(Socket socket){
   
         return new Thread(() -> {
            try{
               handleClient(socket);
            }

            catch(IOException e){
               System.out.println(e);
            }});
   }
}

