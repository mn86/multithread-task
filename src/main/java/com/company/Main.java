package com.company;

import com.mongodb.Block;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.Date;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {

    public static void main(String[] args) {
        Logger.getLogger("org.mongodb.driver").setLevel(Level.WARNING); // to disable Level.INFO spam produced by mongo driver

        MongoClient mongoClient = MongoClients.create(); // mongo driver will reconnect automatically
        MongoDatabase database = mongoClient.getDatabase("mongo-test");
        MongoCollection<Document> mongoCollection = database.getCollection("timestamps");

        if (args.length > 0) {
            if (args[0].equals("-p")) {
                System.out.println("Started in print mode:");
                mongoCollection.find().forEach((Block<Document>) document -> System.out.println(document.get("timestamp")));
            } else {
                System.out.println("Unknown first parameter");
            }
            return;
        }

        ConcurrentLinkedQueue<Date> queue = new ConcurrentLinkedQueue<>();

        Thread consumerThread = new Thread(new Consumer(mongoCollection, queue));
        consumerThread.start();

        while(true) {
            try {
                Date timestamp = new Date();
                queue.add(timestamp);
                System.out.println("Produced value: '" + timestamp + "' added to queue. Current queue size: " + queue.size());
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
