import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import org.junit.Test;

import java.io.IOException;

/**
 * S3Exchange
 *
 * @author changxiangxiang
 * @date 16/5/8
 */
public class S3Exchange {

    private static final String EXCHANGE_NAME = "logs";

    @Test
    public void testSend() throws java.io.IOException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");

        String message = "message";

        for (int i = 0; i < 10; i++) {
            channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes());
            System.out.println(" [x] Sent '" + message + i + "'");
        }

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

        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");

        // 在Java客户端里，当我们使用无参数调用queueDeclare()方法，我们创建一个自动产生的名字，不持久化，独占的，自动删除的队列。
        String queueName = channel.queueDeclare().getQueue();

        // 我们已经创建了一个fanout交换机和队列。现在我们需要告诉交换机发送消息给我们的队列上。这交换机和队列之间的关系称之为一个绑定。
        channel.queueBind(queueName, EXCHANGE_NAME, "");

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        QueueingConsumer consumer = new QueueingConsumer(channel);
        channel.basicConsume(queueName, true, consumer);

        while (true) {
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            String message = new String(delivery.getBody());

            System.out.println(" [x] Received '" + message + "'");
        }
    }

}
