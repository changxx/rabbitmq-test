import com.rabbitmq.client.*;
import org.junit.Test;

import java.io.IOException;

/**
 * S2WorkQueues
 *
 * @author changxiangxiang
 * @date 16/5/8
 */
public class S2WorkQueues {

    private final static String QUEUE_NAME = "hello";

    private final static String QUEUE_NAME_PERSIST = "queue_persist";

    @Test
    public void testSend() throws IOException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        for (int i = 0; i < 10; i++) {
            String message = "hello world!" + i;
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
            // 启动多个testReceive
            // 我们将任务封装成消息，发送到队列中。一个工作者进程在后台运行，获取任务并最终执行任务。当你运行多个工作者，所有的任务将会被他们所共享。
            // QueueingConsumer.nextDelivery()在另一个来自服务器的消息到来之前它会一直阻塞着。
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            Thread.sleep(1000);
            String message = new String(delivery.getBody());
            System.out.println("received message: " + message);
        }
    }

    @Test
    public void testReceive2() throws IOException, InterruptedException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        // 在这里我们提供一个回调对象用来缓存消息，直到我们准备好再使用它们。这就是QueueingConsumer所做的事。
        QueueingConsumer consumer = new QueueingConsumer(channel);
        // 消息确认机制默认情况下是开着的。在先前的例子中我们是明确的将这个功能关闭no_ack=True
        boolean autoAck = false;
        channel.basicConsume(QUEUE_NAME, autoAck, consumer);

        int i = 0;

        while (true) {
            // 启动多个testReceive
            // 我们将任务封装成消息，发送到队列中。一个工作者进程在后台运行，获取任务并最终执行任务。当你运行多个工作者，所有的任务将会被他们所共享。
            // QueueingConsumer.nextDelivery()在另一个来自服务器的消息到来之前它会一直阻塞着。
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            Thread.sleep(1000);
            String message = new String(delivery.getBody());
            System.out.println("received message: " + message);
            if (i++ > 3) {
                System.out.println("i > 3 throw exception");
                throw new RuntimeException("i:" + i);
            }
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        }
    }

    @Test
    public void testSendPersist() throws IOException {
        // 当RabbitMQ退出或者崩溃，它将会忘记这队列和消息，除非你告诉它不要这样做。两个事情需要做来保证消息不会丢失：我们标记队列和消息持久化。
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        // 首先，我们需要确保RabbitMQ不会丢失我们的队列，为了这样做，我们需要将它声明为持久化：
        boolean durable = true;
        channel.queueDeclare(QUEUE_NAME_PERSIST, durable, false, false, null);
        for (int i = 0; i < 10; i++) {
            String message = "hello world!" + i;
            // 现在我们需要标记消息持久化 - 通过设置MessageProperties(实现了BasicProperties)的值为PERSISTENT_TEXT_PLAIN。
            channel.basicPublish("", QUEUE_NAME_PERSIST, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
            System.out.println("[x]Sent:" + message);
        }
        channel.close();
        connection.close();
    }

    @Test
    public void testReceivePersist() throws IOException, InterruptedException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        // 首先，我们需要确保RabbitMQ不会丢失我们的队列，为了这样做，我们需要将它声明为持久化：
        boolean durable = true;
        channel.queueDeclare(QUEUE_NAME_PERSIST, durable, false, false, null);

        // 在这里我们提供一个回调对象用来缓存消息，直到我们准备好再使用它们。这就是QueueingConsumer所做的事。
        QueueingConsumer consumer = new QueueingConsumer(channel);
        // 消息确认机制默认情况下是开着的。在先前的例子中我们是明确的将这个功能关闭no_ack=True
        boolean autoAck = false;
        channel.basicConsume(QUEUE_NAME_PERSIST, autoAck, consumer);

        while (true) {
            // 启动多个testReceive
            // 我们将任务封装成消息，发送到队列中。一个工作者进程在后台运行，获取任务并最终执行任务。当你运行多个工作者，所有的任务将会被他们所共享。
            // QueueingConsumer.nextDelivery()在另一个来自服务器的消息到来之前它会一直阻塞着。
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            Thread.sleep(1000);
            String message = new String(delivery.getBody());
            System.out.println("received message: " + message);
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        }
    }


}
