package br.com.alura;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class NewOrderMain {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		KafkaProducer<String, String> producer = new KafkaProducer<>(properties());
		String value = "2, Junior, 05435";
		String email = "Sua ordem esta sendo processada.";

		ProducerRecord<String,String> orderRecord = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", UUID.randomUUID().toString(), value);
		ProducerRecord<String,String> emailRecord = new ProducerRecord<>("ECOMMERCE_SEND_EMAIL", UUID.randomUUID().toString(), email);
		
		Callback callback = (data, ex)  -> {
			if(ex != null) {
				ex.printStackTrace();
				return;
			}
			
			System.out.println("Success: Send in " + data.topic() + "::: Partition: " + data.partition() + " /Offset:" + data.offset() + " /TimeStamp:" + data.timestamp());
		};
		
		producer.send(orderRecord, callback).get();
		producer.send(emailRecord, callback).get();
	}

	private static Properties properties() {
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		return properties;
	}
}
