import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Producer {
	
	public static void  main (String[] args) throws IOException {
	Properties config = new Properties();
	//config.
	config.setProperty("client.id", InetAddress.getLocalHost().getHostName());
	config.setProperty("bootstrap.servers","cp-kafka1:9092");
	config.put("acks", "all");
	config.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
	config.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
	 KafkaProducer<String, String> producer = new KafkaProducer(config);
	
	 Random random = new Random();
     String[] sentences = new String[] {
             "the cow jumped over the moon",
             "an apple a day keeps the doctor away",
             "four score and seven years ago",
             "snow white and the seven dwarfs",
             "i am at two with nature"
     };
     
     for(int i = 0; i < 100; i++) {
         // Pick a sentence at random
         String sentence = sentences[random.nextInt(sentences.length)];
         // Send the sentence to the test topic
         try
         {
             producer.send(new ProducerRecord<String, String>("pageviews", sentence)).get();
         }
         catch (Exception ex)
         {
             System.out.print(ex.getMessage());
             throw new IOException(ex.toString());
         }
	}

}
}
