namespace MultipleSubscriptionUpdateUsingTopics
{
    using Microsoft.Azure.ServiceBus;
    using System;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    class Program
    {
        const string ServiceBusConnectionString = "Endpoint=sb://latvianvillage-servicebus.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=pzW5MFKceTdEBZNpURyhSEE7eFC0fvqKwaE/kf0Znqg=";
        const string QueueName = "latvianvillage-queue-0001";
        static IQueueClient queueClient;

        static void Main(string[] args)
        {
            string ServiceBusConnectionString = "";
            string QueueName = "";

            for (int i = 0; i < args.Length; i++)
            {
                var p = new Program();
                if (args[i] == "-ConnectionString")
                {
                    Console.WriteLine($"ConnectionString: {args[i + 1]}");
                    ServiceBusConnectionString = args[i + 1];
                }
                else if (args[i] == "-QueueName")
                {
                    Console.WriteLine($"QueueName: {args[i + 1]}");
                    QueueName = args[i + 1];
                }
            }

            if (ServiceBusConnectionString != "" && QueueName != "")
                MainAsync(ServiceBusConnectionString, QueueName).GetAwaiter().GetResult();
            else
            {
                Console.WriteLine("Specify -Connectionstring and -QueueName to execute the example.");
                Console.ReadKey();
            }
        }

        static async Task MainAsync(string ServiceBusConnectionString, string QueueName)
        {
            const int numberOfMessages = 10;
            queueClient = new QueueClient(ServiceBusConnectionString, QueueName);

            Console.WriteLine("======================================================");
            Console.WriteLine("Press any key to exit after receiving all the messages.");
            Console.WriteLine("======================================================");

            // Register QueueClient's MessageHandler and receive messages in a loop
            RegisterOnMessageHandlerAndReceiveMessages();

            // Send Messages
            await SendMessagesAsync(numberOfMessages);

            Console.ReadKey();

            await queueClient.CloseAsync();
        }

        static void RegisterOnMessageHandlerAndReceiveMessages()
        {
            // Configure the MessageHandler Options in terms of exception handling, number of concurrent messages to deliver etc.
            var messageHandlerOptions = new MessageHandlerOptions(ExceptionReceivedHandler)
            {
                // Maximum number of Concurrent calls to the callback `ProcessMessagesAsync`, set to 1 for simplicity.
                // Set it according to how many messages the application wants to process in parallel.
                MaxConcurrentCalls = 1,

                // Indicates whether MessagePump should automatically complete the messages after returning from User Callback.
                // False below indicates the Complete will be handled by the User Callback as in `ProcessMessagesAsync` below.
                AutoComplete = false
            };

            // Register the function that will process messages
            queueClient.RegisterMessageHandler(ProcessMessagesAsync, messageHandlerOptions);
        }

        private static Task ExceptionReceivedHandler(ExceptionReceivedEventArgs arg)
        {
            // ToDo: Figure out what to do on message receiving exceptions 
            throw new NotImplementedException();
        }

        static async Task SendMessagesAsync(int numberOfMessagesToSend)
        {
            try
            {
                for (var i = 0; i < numberOfMessagesToSend; i++)
                {
                    // Create a new message to send to the queue
                    string messageBody = $"Message {i}";
                    var message = new Message(Encoding.UTF8.GetBytes(messageBody));

                    // Write the body of the message to the console
                    Console.WriteLine($"Sending message: {messageBody}");

                    // Send the message to the queue
                    await queueClient.SendAsync(message);
                }
            }
            catch (Exception exception)
            {
                Console.WriteLine($"{DateTime.Now} :: Exception: {exception.Message}");
            }
        }

        static async Task ProcessMessagesAsync(Message message, CancellationToken token)
        {
            // Process the message
            Console.WriteLine($"Received message: SequenceNumber:{message.SystemProperties.SequenceNumber} Body:{Encoding.UTF8.GetString(message.Body)}");

            // Complete the message so that it is not received again.
            await queueClient.CompleteAsync(message.SystemProperties.LockToken);
        }
    }
}
