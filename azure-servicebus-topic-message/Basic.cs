using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace azure_servicebus_topic_message
{
    interface IBasicTopicConnectionStringSample
    {
        Task Run(string queueName, string connectionString);
    }

    public class Basic : IBasicTopicConnectionStringSample
    {
        TopicClient sendClient;
        SubscriptionClient subscription1Client;
        SubscriptionClient subscription2Client;
        SubscriptionClient subscription3Client;

        public async Task Run(string topicName, string connectionString)
        {
            this.sendClient = TopicClient.CreateFromConnectionString(connectionString, topicName);

            this.subscription1Client = SubscriptionClient.CreateFromConnectionString(connectionString, topicName, "Subscription1");
            this.subscription2Client = SubscriptionClient.CreateFromConnectionString(connectionString, topicName, "Subscription2");
            this.subscription3Client = SubscriptionClient.CreateFromConnectionString(connectionString, topicName, "Subscription3");

            this.InitializeReceiver(this.subscription1Client, ConsoleColor.Cyan);
            this.InitializeReceiver(this.subscription2Client, ConsoleColor.Green);
            this.InitializeReceiver(this.subscription3Client, ConsoleColor.Yellow);

            await this.SendMessagesAsync();

            Console.WriteLine("\nEnd of scenario, press any key to exit.");
            Console.ReadKey();

            await this.subscription1Client.CloseAsync();
            await this.subscription2Client.CloseAsync();
            await this.subscription3Client.CloseAsync();
        }

        async Task SendMessagesAsync()
        {
            dynamic data = new[]
            {
                new {name = "Einstein", firstName = "Albert"},
                new {name = "Heisenberg", firstName = "Werner"},
                new {name = "Curie", firstName = "Marie"},
                new {name = "Hawking", firstName = "Steven"},
                new {name = "Newton", firstName = "Isaac"},
                new {name = "Bohr", firstName = "Niels"},
                new {name = "Faraday", firstName = "Michael"},
                new {name = "Galilei", firstName = "Galileo"},
                new {name = "Kepler", firstName = "Johannes"},
                new {name = "Kopernikus", firstName = "Nikolaus"}
            };


            for (int i = 0; i < data.Length; i++)
            {
                var message = new BrokeredMessage(new MemoryStream(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(data[i]))))
                {
                    ContentType = "application/json",
                    Label = "Scientist",
                    MessageId = i.ToString(),
                    TimeToLive = TimeSpan.FromMinutes(2)
                };

                await this.sendClient.SendAsync(message);
                lock (Console.Out)
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine("Message sent: Id = {0}", message.MessageId);
                    Console.ResetColor();
                }
            }
        }

        void InitializeReceiver(SubscriptionClient receiver, ConsoleColor color)
        {
            // register the OnMessageAsync callback
            receiver.OnMessageAsync(
                async message =>
                {
                    if (message.Label != null &&
                        message.ContentType != null &&
                        message.Label.Equals("Scientist", StringComparison.InvariantCultureIgnoreCase) &&
                        message.ContentType.Equals("application/json", StringComparison.InvariantCultureIgnoreCase))
                    {
                        var body = message.GetBody<Stream>();

                        dynamic scientist = JsonConvert.DeserializeObject(new StreamReader(body, true).ReadToEnd());

                        lock (Console.Out)
                        {
                            Console.ForegroundColor = color;
                            Console.WriteLine(
                                "\t\t\t\tMessage received ("+receiver.Name+"): \n\t\t\t\t\t\tMessageId = {0}, \n\t\t\t\t\t\tSequenceNumber = {1}, \n\t\t\t\t\t\tEnqueuedTimeUtc = {2}," +
                                "\n\t\t\t\t\t\tExpiresAtUtc = {5}, \n\t\t\t\t\t\tContentType = \"{3}\", \n\t\t\t\t\t\tSize = {4},  \n\t\t\t\t\t\tContent: [ firstName = {6}, name = {7} ]",
                                message.MessageId,
                                message.SequenceNumber,
                                message.EnqueuedTimeUtc,
                                message.ContentType,
                                message.Size,
                                message.ExpiresAtUtc,
                                scientist.firstName,
                                scientist.name);
                            Console.ResetColor();
                        }
                    }
                    await message.CompleteAsync();
                },
                new OnMessageOptions { AutoComplete = false, MaxConcurrentCalls = 1 });
        }

    }
}
