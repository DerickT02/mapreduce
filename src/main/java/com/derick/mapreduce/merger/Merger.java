package com.derick.mapreduce.merger;

import java.io.IOException;
import java.net.Socket;
import java.net.ServerSocket;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.derick.mapreduce.driver.Driver;
import java.util.concurrent.PriorityBlockingQueue;
public class Merger {
    static public PriorityBlockingQueue<Driver> topDriversByTrips;
    public static void main(String[] args) throws InterruptedException, IOException{
        try{
            topDriversByTrips = new PriorityBlockingQueue<>();
            ServerSocket serverSocket = new ServerSocket(9000);
            Socket clientSocket1 = serverSocket.accept();
            Thread clientThread1 = allocateThreadForSocket(clientSocket1);
            Socket clientSocket2 = serverSocket.accept();
            Thread clientThread2 = allocateThreadForSocket(clientSocket2);
            
            clientThread1.start();
            clientThread2.start();
            clientThread1.join();
            clientThread2.join();
            topDriversByTrips.peek().getDriverInfo();
            serverSocket.close();
        }
        catch(Exception e){
            e.printStackTrace();
        }
        
    }
    
    private static Thread allocateThreadForSocket(Socket socket){
        return new Thread(() -> {
            try {
                System.out.println("Merger class in MapReduce package");
                handleClient(socket);
            } catch (Exception e) {
                System.out.println(e);
            }
        });
    }
    
    private static void handleClient(Socket socket) throws IOException{
        Kryo kryo = new Kryo();
        kryo.register(Driver.class);
        Input input = new Input(socket.getInputStream());
        while (true) {

            try {
                Driver driver = kryo.readObject(input, Driver.class);
                synchronized (topDriversByTrips) {
                    topDriversByTrips.add(driver);                 
                }
            }

            // on an exception, we're done reading.
            catch (Exception e) {
                break;
            }
        }
        input.close();
        socket.close();      
    }
}
