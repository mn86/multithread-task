package com.company;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.util.concurrent.ConcurrentLinkedQueue;

public class Consumer implements Runnable {

    private final MongoCollection mongoCollection;
    private final ConcurrentLinkedQueue queue;

    Consumer(MongoCollection mongoCollection, ConcurrentLinkedQueue queue) {
        this.mongoCollection = mongoCollection;
        this.queue = queue;
    }

    @Override
    public void run() {
        while(true) {
                Object newData = queue.peek(); // read head of queue but don't delete it until we write it into DB
                if (newData == null) {
                    try {
                        Thread.sleep(1000L);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    continue;
                }
                try {
                    mongoCollection.insertOne(new Document("timestamp", newData));
                    queue.remove(); // remove queue head element as it was successfully written to DB
                    System.out.println("'" + newData + "' was consumed and written to DB");
                } catch (MongoException e) {
                    System.out.println("Mongo DB connectivity issue (Exception message: '" + e.getMessage() + "').");
                }
        }
    }
}
