package learning.rabbitmq.ex1_helloworld

import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import org.slf4j.LoggerFactory

val QUEUE_NAME = "hello";

fun main(args: Array<String>) {
    val log = LoggerFactory.getLogger("PRODUCER")
    log.debug("Hello")

    val connFactory = ConnectionFactory()
    connFactory.host = "localhost"
    val conn: Connection = connFactory.newConnection()
    val channel: Channel = conn.createChannel()
    channel.queueDeclare(QUEUE_NAME, false, false, false, mapOf()) // idempotent

    val message = "Hello World! "
    var k = 0

    // Note: avoid closing immediately the channel or the message may not be published in time
    while(readLine()!=null) {
        channel.basicPublish("", QUEUE_NAME, null, (message + k).toByteArray());
        k++
    }

    channel.close();
    conn.close();
}
