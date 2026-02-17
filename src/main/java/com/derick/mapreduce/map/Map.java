package com.derick.mapreduce.map;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import com.derick.mapreduce.driver.Driver;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;


public class Map {
    
    private static BlockingQueue<String> messageQueue;
    private static ConcurrentHashMap<String, Driver> driverMappings;
    private static ArrayList<Driver> drivers;
    private static int WORKERS_AMT = Math.max(1, Runtime.getRuntime().availableProcessors() - 1);
    

    public static void main(String[] args) throws InterruptedException, FileNotFoundException, IOException{
        System.out.println("Map class in MapReduce package");
        try{
            
            messageQueue = new LinkedBlockingQueue<String>();
            driverMappings = new ConcurrentHashMap<String, Driver>();
            ArrayList<Thread> workerThreads = createWorkers();

            readDataSet("/Users/derickpaulalavazotolentino/Downloads/trip_data/trip_data_1.csv");
            waitOnWorkers(workerThreads);

            drivers = new ArrayList<>(driverMappings.values());

            Socket socket = new Socket("localhost", 8000);
            Thread sendThread = allocateThreadForSocket(socket);
            sendThread.start(); 
            sendThread.join();  
            System.out.println("Map phase completed. Data sent to Reduce phase.");
        }
        catch(Exception e){
            e.printStackTrace();
        }


    }

    private static ArrayList<Thread> createWorkers(){
        ArrayList<Thread> workers = new ArrayList<>();
        for(int i = 0; i < WORKERS_AMT; i++){
            Thread thread = new Thread(() -> {
                try{
                    updateDriverMappings();
                }
                catch(InterruptedException e){
                    System.out.println(e);
                }
            });
            thread.start();
            workers.add(thread);
        }
    
        return workers;
    }

    public static void waitOnWorkers(ArrayList<Thread> workers){
        for(Thread worker: workers){
            try{
                worker.join();
            }
            catch(InterruptedException e){
                System.out.println(e);
            }
        }
    }


    private static void readDataSet(String csvPath) throws FileNotFoundException, IOException, InterruptedException{
        String line;
        try(BufferedReader br = new BufferedReader(new FileReader(csvPath))){
            br.readLine();
            while((line = br.readLine()) != null){
                messageQueue.add(line);
                
            }
            for (int i = 0; i < WORKERS_AMT; i++)
            messageQueue.put("EOF");
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    };

    

    private static void updateDriverMappings() throws InterruptedException{
        while(true){
            String line = messageQueue.take();
            if("EOF".equals(line)) break;
            try{
                String[] tripRow = line.split(",");
                String driverId = tripRow[1];
                int tripTimeInSeconds = Integer.parseInt(tripRow[8]);
                double tripDistance = Double.parseDouble(tripRow[9]);
                driverMappings.compute(driverId, (id, driver) -> {
                    if(driver == null){
                        driver = new Driver(id);
                    }
                    driver.reportTrip(tripTimeInSeconds, tripDistance);
                    return driver;
                });
            }
            catch(IllegalArgumentException e){}
            
            
        }
        return;

    }



    private static void sendToReduce(Socket socket) throws IOException {
        Output outputStream = new Output(socket.getOutputStream());
        Kryo kryo = new Kryo();
        kryo.register(Driver.class);

        for (Driver driver : drivers) {
            outputStream.writeBoolean(true);      // sentinel = more objects
            kryo.writeObject(outputStream, driver);
        }
        outputStream.writeBoolean(false);         // sentinel = end
        outputStream.flush();
        outputStream.close();
        socket.close();
    };

    
    /* */
    private static Thread allocateThreadForSocket(Socket socket){
        // Implement thread allocation logic here
        return new Thread(() -> {
            try {
                sendToReduce(socket);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }
        



}
