package com.derick.mapreduce.reduce;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
public class Reduce {
   static public int port = 8000;
   public static void main(String[] args){
      try{
          ServerSocket serverSocket = new ServerSocket(port);
          System.out.println("Reduce class in MapReduce package");  
          serverSocket.accept();


          serverSocket.close();
      }
      catch(IOException e){
          System.out.println(e);
      }
   }

   private static void handleClient(Socket socket) throws IOException{
      Input input = new Input(socket.getInputStream());
      Output output = new Output(socket.getOutputStream());
   }
}

