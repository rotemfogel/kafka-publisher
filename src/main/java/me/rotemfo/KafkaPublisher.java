package me.rotemfo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

/**
 * project: kafka-publisher
 * package: me.rotemfo
 * file:    KafkaPublisher
 * created: 2019-04-12
 * author:  rotem
 */
public class KafkaPublisher {
    private static final Logger logger = LoggerFactory.getLogger(KafkaPublisher.class);
    private static final String topic = "rawmessages";
    private static Map<String, String> paths = new HashMap<>();

    private static void fixPath(String path) {
        String sortPath = path
                .replace("hourid=1/", "hourid=01/")
                .replace("hourid=2/", "hourid=02/")
                .replace("hourid=3/", "hourid=03/")
                .replace("hourid=4/", "hourid=04/")
                .replace("hourid=5/", "hourid=05/")
                .replace("hourid=6/", "hourid=06/")
                .replace("hourid=7/", "hourid=07/")
                .replace("hourid=8/", "hourid=08/")
                .replace("hourid=9/", "hourid=09/");
        paths.put(sortPath, path);
    }

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, "60");
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 2000);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 2000);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        String token = args[0];
        String path = args[1];

        Path pathStart = Paths.get(path);
        try (Stream<Path> stream = Files.walk(pathStart, Integer.MAX_VALUE)) {
            List<String> collect = stream
                    .map(String::valueOf)
                    .filter(f -> f.endsWith(".gz"))
                    .sorted()
                    .collect(Collectors.toList());

            collect.forEach(KafkaPublisher::fixPath);
        }

        for (String sortPath : paths.keySet().stream().sorted().collect(Collectors.toList())) {
            final long start = System.currentTimeMillis();
            org.apache.kafka.clients.producer.Producer<String, String> producer = new KafkaProducer<>(props);
            AtomicInteger lineInFile = new AtomicInteger();
            AtomicInteger fail = new AtomicInteger();
            AtomicInteger ack = new AtomicInteger();

            String file = paths.get(sortPath);
            logger.info(file);

            BufferedReader in = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))));

            String l;
            while ((l = in.readLine()) != null) {
                lineInFile.incrementAndGet();
                String[] keyValue = l.split("#");
                String key = String.format("%s###,%s", token, keyValue[0]);
                String value_ = keyValue[1].replace("\\\"", "\"");
                String value = value_.substring(1, value_.length() - 1);
                ProducerRecord<String, String> data = new ProducerRecord<>(topic, key, value);
//                Thread.sleep(rate);
                producer.send(data, (metadata, e) -> {
                    if (e != null) {
                        fail.incrementAndGet();
                    } else {
                        ack.incrementAndGet();
                    }
                });
            }
            producer.flush();
            producer.close();
            logger.info("[{}] - Lines In File: {}, Fails: {}, Acks: {}, Time: {} ms", sortPath, lineInFile.get(), fail.get(), ack.get(), System.currentTimeMillis() - start);
        }
    }
}