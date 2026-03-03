package com.derick.mapreduce.merger;

import java.io.IOException;
import java.net.Socket;
import java.net.ServerSocket;
import com.esotericsoftware.kryo.Kryo;

public class Merger {
    
    public static void main(String[] args) throws InterruptedException, IOException{
        ServerSocket serverSocket = new ServerSocket(9000);
        Socket clientSocket1 = serverSocket.accept();
        Socket clientSocket2 = serverSocket.accept();
        Thread clientThread1 = allocateThreadForSocket(clientSocket1);
        Thread clientThread2 = allocateThreadForSocket(clientSocket2);
        clientThread1.start();
        clientThread2.start();
        clientThread1.join();
        clientThread2.join();
        serverSocket.close();
        
    }
    
    private static Thread allocateThreadForSocket(Socket socket){
        return new Thread(() -> {
            try {
                System.out.println("Merger class in MapReduce package");
            } catch (Exception e) {
                System.out.println(e);
            }
        });
    } 
}
