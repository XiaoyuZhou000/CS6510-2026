package support;

import messaging.KafkaClients;
import messaging.KafkaRecordSender;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Publishes arbitrary bytes with a key to a topic, bypassing every outbox, for poison-record scenarios such as
 * quickstart scenario G.
 *
 * <pre>
 * java -cp "out/test;out/common.jar;lib/messaging/*" support.PublishRawRecord &lt;topic&gt; &lt;key&gt; &lt;value&gt;
 * </pre>
 *
 * <p>{@code <value>} is {@code base64:<data>}, {@code file:<path>}, or literal text. The broker is
 * {@code KAFKA_BOOTSTRAP} (default {@code 127.0.0.1:19092}).</p>
 */
public final class PublishRawRecord {
    private PublishRawRecord() { }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: PublishRawRecord <topic> <key> <base64:...|file:...|text>");
            System.exit(2);
        }
        publish(System.getenv(), args[0], args[1], value(args[2]));
        System.out.println("Published " + args[0] + "/" + args[1]);
    }

    /** Sends one record and waits for the broker's {@code acks=all} confirmation. */
    public static void publish(Map<String, String> environment, String topic, String key, byte[] value) throws Exception {
        try (KafkaRecordSender sender = new KafkaRecordSender(KafkaClients.producer(environment, "publish-raw-record",
                KafkaClients.IMMEDIATE_LINGER_MS))) {
            sender.send(topic, key, value).get(30, TimeUnit.SECONDS);
        }
    }

    private static byte[] value(String argument) throws Exception {
        if (argument.startsWith("base64:")) return Base64.getDecoder().decode(argument.substring(7));
        if (argument.startsWith("file:")) return Files.readAllBytes(Path.of(argument.substring(5)));
        return argument.getBytes(StandardCharsets.UTF_8);
    }
}
