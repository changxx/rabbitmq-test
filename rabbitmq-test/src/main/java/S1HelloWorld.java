import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import org.junit.Test;

import java.io.IOException;

/**
 * S1HelloWorld
 *
 * @author changxiangxiang
 * @date 16/5/8
 */
public class S1HelloWorld {

    private final static String QUEUE_NAME = "hello";

    @Test
    public void testSend() throws IOException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        for (int i = 0; i < 10; i++) {
            String message = "hello world!";
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
            System.out.println("[x]Sent:" + message);
        }
        channel.close();
        connection.close();
    }

    @Test
    public void testReceive() throws IOException, InterruptedException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        // 在这里我们提供一个回调对象用来缓存消息，直到我们准备好再使用它们。这就是QueueingConsumer所做的事。
        QueueingConsumer consumer = new QueueingConsumer(channel);
        channel.basicConsume(QUEUE_NAME, true, consumer);

        while (true) {
            // QueueingConsumer.nextDelivery()在另一个来自服务器的消息到来之前它会一直阻塞着。
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            String message = new String(delivery.getBody());
            System.out.println("received message: " + message);
        }
    }

}
