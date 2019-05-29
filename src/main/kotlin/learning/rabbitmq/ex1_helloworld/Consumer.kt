package learning.rabbitmq.ex1_helloworld

import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.Delivery
import org.slf4j.LoggerFactory


fun main(args: Array<String>) {
    val log = LoggerFactory.getLogger("CONSUMER")
    log.debug("Hello")

    val connFactory = ConnectionFactory()
    connFactory.host = "localhost"
    val conn: Connection = connFactory.newConnection()
    val channel: Channel = conn.createChannel()
    channel.queueDeclare(QUEUE_NAME, false, false, false, mapOf()) // idempotent

    val deliverCallback = { consumerTag: String, delivery: Delivery ->
        val message = String(delivery.getBody(), Charsets.UTF_8)
        log.info(" [x] Received '$message'")

        // Explicit ack (use when basicConsume(..., autoAck=false))
        // Here we chose to send an ack only to msgs containing an even number: those will be removed by the broker; the other will remain
        if(message.split(" ").last().toInt()%2==0) channel.basicAck(delivery.envelope.deliveryTag, false)
    }
    channel.basicConsume(QUEUE_NAME, false, deliverCallback, { consumerTag -> log.debug("Cancelled " + consumerTag)})

    readLine()

    channel.close();
    conn.close();

}
