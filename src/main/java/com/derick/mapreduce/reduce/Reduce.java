package com.derick.mapreduce.reduce;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.derick.mapreduce.driver.Driver;


public class Reduce {
   static public int port = 8000;
   public static void main(String[] args){
      try{
          ServerSocket serverSocket = new ServerSocket(port);
          System.out.println("Reduce class in MapReduce package");  
          Socket socket = serverSocket.accept();
          handleClient(socket);

         
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
      Driver driver = kryo.readObject(input, Driver.class);
      while(true){
         if(driver == null) break;
         driver.getDriverInfo();
         driver = kryo.readObject(input, Driver.class);
      }
       
      
      Output output = new Output(socket.getOutputStream());
   }
}

