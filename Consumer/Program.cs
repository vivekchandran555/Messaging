using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

Console.WriteLine("Consumer!");

var factory = new ConnectionFactory { HostName = "localhost", UserName = "guest", Password = "guest" };
using var connection = await factory.CreateConnectionAsync();
using var channel = await connection.CreateChannelAsync();

string mainQueue = "message-queue";
string retryQueue = mainQueue + "_retry";
string deadLetterQueue = mainQueue + "_dead";

// main queue declaration
await channel.QueueDeclareAsync(
    queue: mainQueue,
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: new Dictionary<string, object?>
    {
        { "x-dead-letter-exchange", string.Empty },
        { "x-dead-letter-routing-key", deadLetterQueue }
    });

// retry queue declaration
await channel.QueueDeclareAsync(
    queue: retryQueue,
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: new Dictionary<string, object?>
    {
        { "x-message-ttl", 10000 }, // 10 seconds
        { "x-dead-letter-exchange", string.Empty },
        { "x-dead-letter-routing-key", mainQueue }
    });

// dead letter queue declaration
await channel.QueueDeclareAsync(
    queue: deadLetterQueue,
    durable: true,
    exclusive: false,
    autoDelete: false);

// consumer setup
var consumer = new AsyncEventingBasicConsumer(channel);
consumer.ReceivedAsync += async (model, args) =>
{
    var body = args.Body.ToArray();
    var message = Encoding.UTF8.GetString(body);
    Console.WriteLine($"Received message: {message}");
    try
    {
        // Simulate processing
        if (message.Contains("fail"))
        {
            throw new Exception("Simulated processing failure");
        }
        Console.WriteLine("Message processed successfully.");
        await channel.BasicAckAsync(args.DeliveryTag, multiple: false);
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error processing message: {ex.Message}");

        int retryCount = 0;
        int maxRetries = 3;
        var headers = args.BasicProperties.Headers;
        if (headers is not null
            && headers.TryGetValue("x-retry-count", out var value)
            && value is int count)
        {
            retryCount = count;
        }

        if (retryCount < maxRetries)
        {
            var props = new BasicProperties
            {
                Headers = new Dictionary<string, object?>
                {
                    { "x-retry-count", retryCount + 1 }
                },
                Persistent = true
            };

            await channel.BasicPublishAsync(
                exchange: string.Empty,
                routingKey: retryQueue,
                mandatory: true,
                basicProperties: props,
                body: args.Body);

            await channel.BasicAckAsync(deliveryTag: args.DeliveryTag, multiple: false);
        }
        else
        {
            Console.WriteLine("Exceeded max retry count. Sending to DLQ.");
            await channel.BasicNackAsync(deliveryTag: args.DeliveryTag, multiple: false, requeue: false);
        }
    }
};

await channel.BasicConsumeAsync(mainQueue, autoAck: false, consumer);

Console.Read();
