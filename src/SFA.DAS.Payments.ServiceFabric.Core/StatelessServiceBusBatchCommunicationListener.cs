using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Autofac;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Management;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using SFA.DAS.Payments.Application.Infrastructure.Ioc;
using SFA.DAS.Payments.Application.Infrastructure.Logging;
using SFA.DAS.Payments.Application.Infrastructure.Telemetry;
using SFA.DAS.Payments.Application.Messaging;
using SFA.DAS.Payments.Messaging.Serialization;
using RuleDescription = Microsoft.Azure.ServiceBus.RuleDescription;
using SqlFilter = Microsoft.Azure.ServiceBus.SqlFilter;

namespace SFA.DAS.Payments.ServiceFabric.Core
{

    public interface IStatelessServiceBusBatchCommunicationListener : ICommunicationListener
    {
        string EndpointName { get; set; }
    }

    public class StatelessServiceBusBatchCommunicationListener : IStatelessServiceBusBatchCommunicationListener
    {
        private readonly IPaymentLogger logger;
        private readonly IContainerScopeFactory scopeFactory;
        private readonly ITelemetry telemetry;
        private readonly IMessageDeserializer messageDeserializer;
        private readonly IApplicationMessageModifier messageModifier;
        private readonly string connectionString;
        public string EndpointName { get; set; }
        private readonly string errorQueueName;
        private CancellationToken startingCancellationToken;

        private const string TopicPath = "bundle-1";

        public StatelessServiceBusBatchCommunicationListener(string connectionString, string endpointName, string errorQueueName, IPaymentLogger logger,
            IContainerScopeFactory scopeFactory, ITelemetry telemetry, IMessageDeserializer messageDeserializer, IApplicationMessageModifier messageModifier)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            this.scopeFactory = scopeFactory ?? throw new ArgumentNullException(nameof(scopeFactory));
            this.telemetry = telemetry ?? throw new ArgumentNullException(nameof(telemetry));
            this.messageDeserializer = messageDeserializer ?? throw new ArgumentNullException(nameof(messageDeserializer));
            this.messageModifier = messageModifier ?? throw new ArgumentNullException(nameof(messageModifier));
            this.connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            EndpointName = endpointName ?? throw new ArgumentNullException(nameof(endpointName));
            this.errorQueueName = errorQueueName ?? endpointName + "-Errors";
        }

        public Task<string> OpenAsync(CancellationToken cancellationToken)
        {
            startingCancellationToken = cancellationToken;
            _ = ListenForMessages(cancellationToken);
            return Task.FromResult(EndpointName);
        }

        protected virtual async Task ListenForMessages(CancellationToken cancellationToken)
        {
            await EnsureQueue(EndpointName).ConfigureAwait(false);
            await EnsureSubscriptions(EndpointName, cancellationToken).ConfigureAwait(false);
            await EnsureQueue(errorQueueName).ConfigureAwait(false);
            try
            {
                await Listen(cancellationToken);
            }
            catch (Exception ex)
            {
                logger.LogFatal($"Encountered fatal error. Error: {ex.Message}", ex);
            }
        }

        private async Task EnsureSubscriptions(string endpointName, CancellationToken cancellationToken)
        {
            try
            {
                //get class names that are subscribing to IHandleBatchMessages
                List<Type> subscribedMessageTypes = GetBatchHandledMessageTypes();
                if (!subscribedMessageTypes.Any()) return;

                //GetCurrentSubscriptions
                _ = await GetOrCreateSubscription(endpointName, cancellationToken);

                var existingRules = await GetExistingRules(endpointName, cancellationToken);

                foreach (var type in subscribedMessageTypes)
                {
                    if (!existingRules.Any(x => x.Name == type.Name))
                    {
                        CreateNewSubscriptionRule(type, endpointName, cancellationToken);
                    }
                }
            }
            catch (MessagingEntityAlreadyExistsException ex)
            {
                logger.LogInfo($"The message queue entity already exists: {ex.Message}. This could be because another instance of the service has already ensured the entity exists");
            }
            catch (Exception e)
            {
                logger.LogFatal($"Error ensuring subscription, or rule: {e.Message}.", e);
                throw;
            }
        }

        private void CreateNewSubscriptionRule(Type type, string endpointName, CancellationToken cancellationToken)
        {
            var manageClient = new ManagementClient(connectionString);
            var ruleDescription = new RuleDescription
            {
                Filter = new SqlFilter($"[NServiceBus.EnclosedMessageTypes] LIKE '%{type.FullName}%'"),
                Name = type.Name
            };

            manageClient.CreateRuleAsync(TopicPath, endpointName, ruleDescription, cancellationToken);
        }

        private async Task<IList<RuleDescription>> GetExistingRules(string subscriptionName, CancellationToken cancellationToken)
        {
            var manageClient = new ManagementClient(connectionString);
            return await manageClient.GetRulesAsync(TopicPath, subscriptionName, cancellationToken: cancellationToken);
        }

        private async Task<SubscriptionDescription> GetOrCreateSubscription(string endpointName, CancellationToken cancellationToken)
        {
            var manageClient = new ManagementClient(connectionString);

            SubscriptionDescription subscriptionDescription;
            if (!await manageClient.SubscriptionExistsAsync(TopicPath, endpointName, cancellationToken))
            {
                subscriptionDescription = new SubscriptionDescription(TopicPath, endpointName)
                {
                    ForwardTo = endpointName,
                    UserMetadata = endpointName,
                    EnableBatchedOperations = true,
                    MaxDeliveryCount = Int32.MaxValue,
                    EnableDeadLetteringOnFilterEvaluationExceptions = false,
                    LockDuration = TimeSpan.FromMinutes(5)
                };
                var defaultRule = new RuleDescription("$default") { Filter = new SqlFilter("1=0") };
                await manageClient.CreateSubscriptionAsync(
                   subscriptionDescription, defaultRule, cancellationToken);
            }
            else
            {
                subscriptionDescription =
                    await manageClient.GetSubscriptionAsync(TopicPath, endpointName, cancellationToken);
            }

            return subscriptionDescription;
        }

        private List<Type> GetBatchHandledMessageTypes()
        {
            var genericTypes = new List<Type>();

            var types = AppDomain.CurrentDomain.GetAssemblies()
                .SelectMany(GetLoadableTypes);

            foreach (var type in types)
            {
                foreach (var intType in type.GetInterfaces())
                {
                    if (intType.IsGenericType &&
                        intType.GetGenericTypeDefinition() == typeof(IHandleMessageBatches<>))
                    {
                        genericTypes.Add(intType.GetGenericArguments()[0]);
                    }
                }
            }

            return genericTypes;
        }

        private static IEnumerable<Type> GetLoadableTypes(Assembly assembly)
        {
            try
            {
                return assembly.GetTypes();
            }
            catch (ReflectionTypeLoadException ex)
            {
                return ex.Types
                    .Where(type => type != null)
                    .Cast<Type>();
            }
        }

        private async Task<List<(Object Message, BatchMessageReceiver Receiver, ServiceBusReceivedMessage ReceivedMessage)>> ReceiveMessages(BatchMessageReceiver messageReceiver, CancellationToken cancellationToken)
        {
            var applicationMessages = new List<(Object Message, BatchMessageReceiver Receiver, ServiceBusReceivedMessage ReceivedMessage)>();
            var messages = await messageReceiver.ReceiveMessages(200, cancellationToken).ConfigureAwait(false);
            if (!messages.Any())
                return applicationMessages;

            foreach (var message in messages)
            {
                cancellationToken.ThrowIfCancellationRequested();
                try
                {
                    var applicationMessage = GetApplicationMessage(message);
                    applicationMessages.Add((applicationMessage, messageReceiver, message));
                }
                catch (Exception e)
                {
                    logger.LogError($"Error deserializing the message. Error: {e.Message}", e);
                    //TODO: should use the error queue instead of dead letter queue
                    await messageReceiver.DeadLetter(message, new CancellationToken())
                        .ConfigureAwait(false);
                }
            }

            return applicationMessages;
        }

        private async Task Listen(CancellationToken cancellationToken)
        {
            var connection = new ServiceBusConnection(connectionString);
            var client = new ServiceBusClient(connectionString);
            var messageReceivers = new List<BatchMessageReceiver>();
            messageReceivers.AddRange(Enumerable.Range(0, 3)
                .Select(i => new BatchMessageReceiver(client, EndpointName)));
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        var receiveTasks =
                            messageReceivers.Select(receiver => ReceiveMessages(receiver, cancellationToken)).ToList();
                        await Task.WhenAll(receiveTasks).ConfigureAwait(false);

                        var messages = receiveTasks.SelectMany(task => task.Result).ToList();

                        if (!messages.Any())
                        {
                            await Task.Delay(2000, cancellationToken);
                            continue;
                        }

                        var groupedMessages = new Dictionary<Type, List<(object Message, BatchMessageReceiver MessageReceiver, ServiceBusReceivedMessage ReceivedMessage)>>();
                        foreach (var message in messages)
                        {
                            cancellationToken.ThrowIfCancellationRequested();
                            var key = message.Message.GetType();
                            var applicationMessages = groupedMessages.ContainsKey(key)
                                ? groupedMessages[key]
                                : groupedMessages[key] = new List<(object Message, BatchMessageReceiver MessageReceiver, ServiceBusReceivedMessage ReceivedMessage)>();
                            applicationMessages.Add(message);
                        }

                        await Task.WhenAll(groupedMessages.Select(group =>
                            ProcessMessages(group.Key, group.Value, cancellationToken)));
                    }
                    catch (TaskCanceledException)
                    {
                        logger.LogWarning("Cancelling communication listener.");
                        return;
                    }
                    catch (OperationCanceledException)
                    {
                        logger.LogWarning("Cancelling communication listener.");
                        return;
                    }
                    catch (Exception ex)
                    {
                        logger.LogError($"Error listening for message.  Error: {ex.Message}", ex);
                    }
                }
            }
            finally
            {
                await Task.WhenAll(messageReceivers.Select(receiver => receiver.Close())).ConfigureAwait(false);
                if (!connection.IsClosedOrClosing)
                    await connection.CloseAsync();
            }
        }

        private object GetApplicationMessage(ServiceBusReceivedMessage message)
        {
            var applicationMessage = DeserializeMessage(message);
            return messageModifier.Modify(applicationMessage);
        }

        private object DeserializeMessage(ServiceBusReceivedMessage message)
        {
            return messageDeserializer.DeserializeMessage(message);
        }

        protected async Task ProcessMessages(Type groupType, List<(object Message, BatchMessageReceiver MessageReceiver, ServiceBusReceivedMessage ReceivedMessage)> messages,
            CancellationToken cancellationToken)
        {
            try
            {
                using (var containerScope = scopeFactory.CreateScope())
                {
                    if (!containerScope.TryResolve(typeof(IHandleMessageBatches<>).MakeGenericType(groupType),
                        out object handler))
                    {
                        logger.LogError($"No handler found for message: {groupType.FullName}");
                        await Task.WhenAll(messages.Select(message => message.MessageReceiver.DeadLetter(message.ReceivedMessage, new CancellationToken())));
                        return;
                    }

                    var methodInfo = handler.GetType().GetMethod("Handle");
                    if (methodInfo == null)
                        throw new InvalidOperationException($"Handle method not found on handler: {handler.GetType().Name} for message type: {groupType.FullName}");

                    var listType = typeof(List<>).MakeGenericType(groupType);
                    var list = (IList)Activator.CreateInstance(listType);
                    messages.ForEach(message => list.Add(message.Message));

                    await (Task)methodInfo.Invoke(handler, new object[] { list, cancellationToken });
                    await Task.WhenAll(messages.GroupBy(msg => msg.MessageReceiver).Select(group =>
                        group.Key.Complete(group.Select(msg => msg.ReceivedMessage.LockToken)))).ConfigureAwait(false);
                }
            }
            catch (Exception e)
            {
                logger.LogError($"Error in StatelessServiceBusBatchCommunicationListener, Message Type: {messages.First().Message.GetType().Name}, Message Count: {messages.Count}, Error: {e.Message}", e);
                await Task.WhenAll(messages.Where(msg => msg.ReceivedMessage.DeliveryCount < 10).GroupBy(msg => msg.MessageReceiver).Select(group =>
                        group.Key.Abandon(group.Select(msg => msg.ReceivedMessage.LockToken)
                            .ToList())))
                    .ConfigureAwait(false);
                await RetryFailedMessages(groupType, messages.Where(msg => msg.ReceivedMessage.DeliveryCount >= 10).ToList(), cancellationToken);
            }
        }

        protected async Task RetryFailedMessages(Type groupType,
            List<(object Message, BatchMessageReceiver MessageReceiver, ServiceBusReceivedMessage ReceivedMessage)> messages,
            CancellationToken cancellationToken)
        {
            var listType = typeof(List<>).MakeGenericType(groupType);
            var list = (IList)Activator.CreateInstance(listType);
            foreach (var retryMessage in messages)
            {
                try
                {
                    using (var scope = scopeFactory.CreateScope())
                    {
                        if (!scope.TryResolve(typeof(IHandleMessageBatches<>).MakeGenericType(groupType),
                            out object handler))
                        {
                            logger.LogError($"No handler found for message: {groupType.FullName}");
                            await Task.WhenAll(messages.Select(message => message.MessageReceiver.DeadLetter(message.ReceivedMessage.LockToken, new CancellationToken())));
                            return;
                        }

                        var methodInfo = handler.GetType().GetMethod("Handle");
                        if (methodInfo == null)
                            throw new InvalidOperationException($"Handle method not found on handler: {handler.GetType().Name} for message type: {groupType.FullName}");

                        list.Clear();
                        list.Add(retryMessage.Message);

                        await (Task)methodInfo.Invoke(handler, new object[] { list, cancellationToken });

                        await retryMessage.MessageReceiver.Complete(new List<string> { retryMessage.ReceivedMessage.LockToken });
                    }
                }
                catch (Exception e)
                {
                    logger.LogError($"Error in StatelessServiceBusBatchCommunicationListener, Message Type:  {retryMessage.GetType().Name}, Error: {e.Message}.  ASB Message id: {retryMessage.ReceivedMessage.MessageId}, Message label: {retryMessage.ReceivedMessage.Subject}.", e);
                    await retryMessage.MessageReceiver.Abandon(new List<string> { retryMessage.ReceivedMessage.LockToken });
                }
            }
        }

        public Task CloseAsync(CancellationToken cancellationToken)
        {
            if (!startingCancellationToken.IsCancellationRequested)
                startingCancellationToken = cancellationToken;
            return Task.CompletedTask;
        }

        public void Abort()
        {
        }

        private async Task EnsureQueue(string queuePath)
        {
            try
            {
                var manageClient = new ManagementClient(connectionString);
                if (await manageClient.QueueExistsAsync(queuePath, startingCancellationToken).ConfigureAwait(false))
                {
                    logger.LogInfo($"Queue '{queuePath}' already exists, skipping queue creation.");
                    return;
                }

                logger.LogInfo(
                    $"Creating queue '{queuePath}' with properties: TimeToLive: 7 days, Lock Duration: 5 Minutes, Max Delivery Count: 50, Max Size: 5Gb.");
                var queueDescription = new QueueDescription(queuePath)
                {
                    DefaultMessageTimeToLive = TimeSpan.FromDays(7),
                    EnableDeadLetteringOnMessageExpiration = true,
                    LockDuration = TimeSpan.FromMinutes(5),
                    MaxDeliveryCount = 50,
                    MaxSizeInMB = 5120,
                    Path = queuePath
                };

                await manageClient.CreateQueueAsync(queueDescription, startingCancellationToken).ConfigureAwait(false);
            }
            catch (MessagingEntityAlreadyExistsException ex)
            {
                logger.LogInfo($"Queue already exists: {ex.Message}. This could be because another instance of the service has already ensured the queue exists");
            }
            catch (Exception e)
            {
                logger.LogFatal($"Error ensuring queue: {e.Message}.", e);
                throw;
            }
        }
    }
}