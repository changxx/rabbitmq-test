import com.rabbitmq.client.*;
import org.junit.Test;

import java.io.IOException;

/**
 * S3Exchange
 *
 * @author changxiangxiang
 * @date 16/5/8
 */
public class S4Routing {

    private static final String EXCHANGE_NAME = "direct_routing";

    private static final String ROUTING_NAME = "info";

    @Test
    public void testSend() throws IOException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, "direct");

        String severity = ROUTING_NAME;
        String message = "Hello World!";

        channel.basicPublish(EXCHANGE_NAME, severity, null, message.getBytes("UTF-8"));
        System.out.println(" [x] Sent '" + severity + "':'" + message + "'");

        channel.close();
        connection.close();
    }

    /**
     * 如果队列还没有绑定到交换机上，消息将会丢失，但是这个对我们来说是ok的；如果没有消费者正在监听，我们可以安全的丢弃消息。
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testReceivePersist() throws IOException, InterruptedException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, "direct");
        String queueName = channel.queueDeclare().getQueue();


        channel.queueBind(queueName, EXCHANGE_NAME, ROUTING_NAME);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println(" [x] Received '" + envelope.getRoutingKey() + "':'" + message + "'");
            }
        };
        channel.basicConsume(queueName, true, consumer);

        Thread.sleep(100000);
    }

}
