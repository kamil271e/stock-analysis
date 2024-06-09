import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.io.File;
import java.util.Arrays;

public class KafkaCSVProducer {


    public static void main(String[] args) {

        String inputDir = args[0];
        String topic = args[1];
        String header = args[2];

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        final File folder = new File(inputDir);
        File[] listOfFiles = folder.listFiles((dir, name) -> name.toLowerCase().endsWith(".csv"));
        assert listOfFiles != null;
        String[] listOfPaths = Arrays.stream(listOfFiles)
                .map(File::getAbsolutePath)
                .toArray(String[]::new);
        Arrays.sort(listOfPaths);

        for (String csvFile : listOfPaths) {
            try (BufferedReader reader = new BufferedReader(new FileReader(csvFile))) {
                String line;
                for(int i = 0; i <= Integer.parseInt(header); i++) reader.readLine();
                while ((line = reader.readLine()) != null) {
                    producer.send(new ProducerRecord<>(topic, line));
                    System.out.println(line);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        producer.close();
    }
}