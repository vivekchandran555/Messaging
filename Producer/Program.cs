using RabbitMQ.Client;
using System.Text;

Console.WriteLine("Producer!");

var factory = new ConnectionFactory { HostName = "localhost", UserName = "guest", Password = "guest" };
using var connection = await factory.CreateConnectionAsync();
using var channel = await connection.CreateChannelAsync();

string mainQueue = "message-queue";
string retryQueue = mainQueue + "_retry";
string deadLetterQueue = mainQueue + "_dead";

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

var message = $"Message fail";
var body = Encoding.UTF8.GetBytes(message);
await channel.BasicPublishAsync(
    exchange: string.Empty,
    routingKey: "message-queue",
    mandatory: true,
    basicProperties: new BasicProperties { Persistent = true },
    body: body);

Console.WriteLine("Sent: {0}", message);

Console.Read();