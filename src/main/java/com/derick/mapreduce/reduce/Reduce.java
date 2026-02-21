package com.derick.mapreduce.reduce;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.derick.mapreduce.driver.Driver;
import java.util.PriorityQueue;


public class Reduce {
   static public int port = 8000;
   static public PriorityQueue<Driver> topDriversByTrips;
   public static void main(String[] args) throws IOException, InterruptedException{
      try{
         if(args.length > 0){
            port = Integer.parseInt(args[0]);
         }
         topDriversByTrips = new PriorityQueue<>();
          ServerSocket serverSocket = new ServerSocket(port);
          System.out.println("Reduce class in MapReduce package");  
          Socket clientSocket1 = serverSocket.accept();
          Thread clientThread1 = allocateThreadForSocket(clientSocket1); 

          Socket clientSocket2 = serverSocket.accept();
          Thread clientThread2 = allocateThreadForSocket(clientSocket2);

          clientThread1.start();
          clientThread2.start();
          clientThread1.join();
          clientThread2.join();

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

         synchronized (topDriversByTrips) {
            topDriversByTrips.add(driver);
         }

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

