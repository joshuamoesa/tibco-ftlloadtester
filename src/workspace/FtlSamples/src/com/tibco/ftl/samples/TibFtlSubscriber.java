/*
 * Copyright (c) 2010-2020 TIBCO Software Inc.
 * All Rights Reserved. Confidential & Proprietary.
 * For more information, please contact:
 * TIBCO Software Inc., Palo Alto, California, USA
 */

/*
 * This is a sample of a Java publisher program which creates multiple separate
 * publishers, and sends messages at a specified rate.
 */

package com.tibco.ftl.samples;

import java.util.*;
import java.util.concurrent.*;
import java.time.*;
import com.tibco.ftl.*;
import com.tibco.ftl.exceptions.*;


public class TibFtlSubscriber
{
    public final class ApplicationConfiguration
    {
        static final String DEFAULT_REALM_SERVICE = TibFtlAux.DEFAULT_URL;
        static final String DEFAULT_APPLICATION_NAME = "default";
        static final String DEFAULT_ENDPOINT_NAME = "default";
        static final String DEFAULT_USER = "guest";
        static final String DEFAULT_PASSWORD = "guest-pw";
        static final int DEFAULT_SUBSCRIBER_COUNT = 2;

        private String realmService;
        public String getRealmService() { return realmService; }
        public void setRealmService(String v) { realmService = v; }
        private String applicationName;
        public String getApplicationName() { return applicationName; }
        public void setApplicationName(String v) { applicationName = v; }
        private String endpointName;
        public String getEndpointName() { return endpointName; }
        public void setEndpointName(String v) { endpointName = v; }
        private String user;
        public String getUser() { return user; }
        public void setUser(String v) { user = v; }
        private String password;
        public String getPassword() { return password; }
        public void setPassword(String v) { password = v; }
        private String durableName;
        public String getDurableName() { return durableName; }
        public void setDurableName(String v) { durableName = v; }
        private String matcherString;
        public String getMatcherString() { return matcherString; }
        public void setMatcherString(String v) { matcherString = v; }
        private boolean trustAll;
        public boolean getTrustAll() { return trustAll; }
        public void setTrustAll(boolean v) { trustAll = v; }
        private String trustFile;
        public String getTrustFile() { return trustFile; }
        public void setTrustFile(String v) { trustFile = v; }
        private String clientLabelBase;
        public String getClientLabelBase() { return clientLabelBase; }
        public void setClientLabelBase(String v) { clientLabelBase = v; }
        private String traceLevel;
        public String getTraceLevel() { return traceLevel; }
        public void setTraceLevel(String v) { traceLevel = v; }
        private int subscriberCount;
        public int getSubscriberCount() { return subscriberCount; }
        public void setSubscriberCount(int v) { subscriberCount = v; }
        private boolean explicitAck;
        public boolean getExplicitAck() { return explicitAck; }
        public void setExplicitAck(boolean v) { explicitAck = v; }
        private boolean unsubscribe;
        public boolean getUnsubscribe() { return unsubscribe; }
        public void setUnsubscribe(boolean v) { unsubscribe = v; }

        public ApplicationConfiguration()
        {
            this.realmService = DEFAULT_REALM_SERVICE;
            this.applicationName = DEFAULT_APPLICATION_NAME;
            this.endpointName = DEFAULT_ENDPOINT_NAME;
            this.user = DEFAULT_USER;
            this.password = DEFAULT_PASSWORD;
            this.durableName = null;
            this.matcherString = null;
            this.trustAll = false;
            this.trustFile = null;
            this.clientLabelBase = null;
            this.traceLevel = null;
            this.subscriberCount = DEFAULT_SUBSCRIBER_COUNT;
            this.explicitAck = false;
            this.unsubscribe = false;
        }
    }

    public final class Statistics
    {
        private long messages;
        private long startTime;
        private long endTime;
        public Statistics(long messages, long startTime, long endTime)
        {
            this.messages = messages;
            this.startTime = startTime;
            this.endTime = endTime;
        }
        private Statistics() { }
    }

    public class SubscriberConfiguration extends Thread implements SubscriberListener, NotificationHandler
    {
        // Configuration data
        private String realmService;
        private String applicationName;
        private String endpointName;
        private String user;
        private String password;
        private boolean trustAll;
        private String trustFile;
        private String clientLabel;
        private String matcherString;
        private boolean explicitAck;
        private boolean unsubscribe;
        private String durableName;
        // FTL objects
        private Realm realm;

        private int messagesReceived;
        private long receiveStartTime;
        private long receiveEndTime;
        private boolean disabled;
        private boolean finished;
        private boolean failed;
        ConcurrentLinkedQueue<Message> ackList;

        public synchronized boolean isDisabled()
        {
            return this.disabled;
        }

        public synchronized void setDisabled()
        {
            this.disabled = true;
        }

        public synchronized boolean isFinished()
        {
            return this.finished;
        }

        public synchronized void setFinished()
        {
            this.finished = true;
        }

        public synchronized boolean isFailed()
        {
            return this.failed;
        }

        public synchronized void setFailed()
        {
            this.failed = true;
        }

        public synchronized Statistics getStatistics()
        {
            return new Statistics(this.messagesReceived, this.receiveStartTime, this.receiveEndTime);
        }

        public SubscriberConfiguration(ApplicationConfiguration config, String clientLabel, String durable)
        {
            this.realmService = config.getRealmService();
            this.applicationName = config.getApplicationName();
            this.endpointName = config.getEndpointName();
            this.user = config.getUser();
            this.password = config.getPassword();
            this.trustAll = config.getTrustAll();
            this.trustFile = config.getTrustFile();
            this.clientLabel = clientLabel;
            this.matcherString = config.getMatcherString();
            this.durableName = durable;
            this.realm = null;
            this.messagesReceived = 0;
            this.receiveStartTime = -1;
            this.receiveEndTime = -1;
            this.disabled = false;
            this.finished = false;
            this.failed = false;
            this.ackList = new ConcurrentLinkedQueue<>();
        }

        public void log(String format, Object... list)
        {
            String fmt = "[" + this.clientLabel + "] " + format;
            TibFtlLogger.getInstance().log(fmt, list);
        }

        public void logException(Exception e)
        {
            log("Unexpected Exception: " + e);
        }

        @Override
        public void run()
        {
            TibProperties realmProps = null;
            AdvisoryHandler advisory = null;
            EventQueue queue = null;
            TibProperties subscriberProps = null;
            Subscriber subscriber = null;
            ContentMatcher contentMatcher = null;

            try
            {
                // A properties object (ITibProperties) allows the client application to set attributes (or properties)
                // for an object when the object is created. In this case, set the client label property
                // (Realm.PROPERTY_STRING_CLIENT_LABEL) for the realm connection.
                realmProps = FTL.createProperties();
                realmProps.set(Realm.PROPERTY_STRING_CLIENT_LABEL, this.clientLabel);
                realmProps.set(Realm.PROPERTY_STRING_USERNAME, this.user);
                realmProps.set(Realm.PROPERTY_STRING_USERPASSWORD, this.password);
                if (this.trustAll)
                {
                    realmProps.set(Realm.PROPERTY_LONG_TRUST_TYPE, Realm.HTTPS_CONNECTION_TRUST_EVERYONE);
                }
                else if (this.trustFile != null)
                {
                    realmProps.set(Realm.PROPERTY_LONG_TRUST_TYPE, Realm.HTTPS_CONNECTION_USE_SPECIFIED_TRUST_FILE);
                    realmProps.set(Realm.PROPERTY_STRING_TRUST_FILE, this.trustFile);
                }
                realmProps.set(Realm.PROPERTY_LONG_CONNECT_RETRIES, (long) 5);
                realmProps.set(Realm.PROPERTY_DOUBLE_CONNECT_INTERVAL, 5.0);

                // Establish a connection to the realm service. Specify a string containing the realm service URL, a string containing
                // the application name, and the properties object created above.
                this.realm = FTL.connectToRealmServer(this.realmService, this.applicationName, realmProps);

                // It is good practice to clean up objects when they are no longer needed. Since the realm properties object is no
                // longer needed, destroy it now.
                realmProps.destroy();

                // Register a notification handler which is invoked when an administrative action occurs which requires the client
                // application to be notified. Specify an object which implements INotificationHandler, and provides an OnNotification()
                // method.
                this.realm.setNotificationHandler(this);

                // Create an AdvisoryHandler object. This creates a separate event queue, creates a subscriber to the advisory
                // endpoint, adds the subscriber to the queue, then creates a separate thread to dispatch the advisory event queue.
                advisory = new AdvisoryHandler(this);

                // A timer will be used to drive message production. For events (such as messages and timers) to be delivered to a
                // client application, an event queue is required.
                queue = this.realm.createEventQueue();

                // A content matcher is used to limit which messages are delivered to a client application based on the content
                // of the message. The string passed to realm.CreateContentMatcher() contains JSON which specifies the field name
                if (this.matcherString != null)
                {
                    contentMatcher = this.realm.createContentMatcher(this.matcherString);
                }

                subscriberProps = FTL.createProperties();
                if (this.explicitAck)
                {
                    subscriberProps.set(Subscriber.PROPERTY_BOOL_EXPLICIT_ACK, true);
                }
                if (this.durableName != null)
                {
                    subscriberProps.set(Subscriber.PROPERTY_STRING_DURABLE_NAME, this.durableName);
                }

                // Create the subscriber object. Specify the endpoint name, content matcher, and properties object.
                subscriber = realm.createSubscriber(this.endpointName, contentMatcher, subscriberProps);
                subscriberProps.destroy();

                // Add the subscriber to the event queue. This allows the event queue to dispatch messages sent to that subscriber.
                queue.addSubscriber(subscriber, this);

                // Establish the receive start time for this subscriber
                this.receiveStartTime = System.nanoTime();

                while (!this.isFinished() && !this.isDisabled())
                {
                    queue.dispatch(1.0);
                    if (this.explicitAck)
                    {
                        while (this.ackList.isEmpty())
                        {
                            Message msg = this.ackList.remove();
                            msg.acknowledge();
                            msg.destroy();
                        }
                    }
                }

                queue.removeSubscriber(subscriber);
                subscriber.close();
                if ((this.durableName != null) && this.unsubscribe)
                {
                    this.realm.unsubscribe(this.endpointName, this.durableName);
                }
                if (contentMatcher != null)
                {
                    contentMatcher.destroy();
                }
                queue.destroy();
                advisory.stop();
                this.realm.close();
            }
            catch (Exception e)
            {
                logException(e);
                this.setFailed();
            }
        }

        public void messagesReceived(List<Message> messages, EventQueue eventQueue)
        {
            // messagesReceived() is the message delivery callback function. It is a method of
            // the SubscriberListener interface. Callbacks are associated with a subscriber via 
            // addSubscriber() method of an event queue. The callback must be a function that returns void,
            // and takes two arguments:
            // - a list of messages (Message) being delivered to the callback in this invocation
            // - an event queue (EventQueue), the event queue which dispatched the messages
            //
            // More than one message may be delivered in a single invocation. The callback must be able to
            // handle the delivery of multiple messages.

            int i;
            int msgNum = messages.size();
             
            for (i = 0;  i < msgNum;  i++) 
            {
                this.messagesReceived++;
                if (this.explicitAck)
                {
                    this.ackList.add(messages.get(i).mutableCopy());
                }
                this.receiveEndTime = System.nanoTime();
            }
        }

        public void onNotification(int type, String reason)
        {
            // onNotification() is the notification handler callback. In some situation, FTL must notify a
            // client application of a condition which can't be delivered through an event queue. Instead of sending
            // an advisory (which would require an event queue), FTL uses an out-of-band mechanism to notify the
            // client application.
            //
            // The notification callback must be named OnNotification, be a member of a class which implements INotificationHandler,
            // return void, and take two arguments:
            //   - a RealmNotificationType which indicates the type of condition being notified
            //   - a string describing the reason for the notification
            if (type == NotificationHandler.CLIENT_DISABLED)
            {
                log("Application administratively disabled: " + reason);
                this.setDisabled();
            }
            else
            {
                log("Notification type " + type + ": " + reason);
            }
        }

    }

    public class AdvisoryHandler implements SubscriberListener, Runnable
    {
        boolean terminate = false;
        Subscriber advisorySubscriber = null;
        EventQueue advisoryQueue = null;
        Thread dispatchThread;
        SubscriberConfiguration subscriber = null;

        private AdvisoryHandler() { }

        public AdvisoryHandler(SubscriberConfiguration subscriber)
        {
            TibProperties advisoryQueueProps = null;

            this.subscriber = subscriber;

            try
            {
                // By default, a randomly-chosen name is given to an event queue by the FTL library. If the queue encounters any
                // issues (such as discarding events), an advisory message is generated which includes the queue name. It is good
                // practice to assign a meaningful name when creating the event queue, to facilitate diagnosing event queue issues.
                // The queue name, as well as the discard policy, is set via a properties object.
                advisoryQueueProps = FTL.createProperties();
                // Set the queue name property (FTL.EVENTQUEUE_PROPERTY_STRING_NAME) to "advisory_queue".
                advisoryQueueProps.set(EventQueue.PROPERTY_STRING_NAME, "advisory_queue");
                // Also set the discard policy (FTL.EVENTQUEUE_PROPERTY_INT_DISCARD_POLICY) to FTL.EVENTQUEUE_DISCARD_NONE,
                // indicating not to discard any events. This is the default, but setting it explicitly is good practice.
                advisoryQueueProps.set(EventQueue.PROPERTY_INT_DISCARD_POLICY, EventQueue.DISCARD_NONE);
                advisoryQueue = subscriber.realm.createEventQueue(advisoryQueueProps);
                advisoryQueueProps.destroy();
                // Create the advisory subscriber object. Specify the endpoint name and an optional content matcher.
                // All advisory messages are delivered to the endpoint defined by Advisory.ENDPOINT_NAME.
                advisorySubscriber = subscriber.realm.createSubscriber(Advisory.ENDPOINT_NAME, null);
                // Next add the advisory subscriber object to the event queue. This allows the event queue to
                // dispatch messages sent to that subscriber. Specify the subscriber object, and an object which implements
                // ISubscriberListener (this class) and provides a MessagesReceived() method.
                advisoryQueue.addSubscriber(advisorySubscriber, this);
                // Create and start the thread to dispatch the event queue.
                dispatchThread = new Thread(this);
                dispatchThread.start();
            }
            catch (Exception e)
            {
                this.subscriber.logException(e);
                this.subscriber.setFailed();
            }
        }

        public void run()
        {
            do
            {
                try
                {
                    advisoryQueue.dispatch(0.1);
                }
                catch (Exception e)
                {
                    subscriber.logException(e);
                    subscriber.setFailed();
                    terminate = true;
                }
            } while (!terminate);
        }

        public void stop()
        {
            terminate = true;
            try
            {
                dispatchThread.join();
                advisoryQueue.removeSubscriber(advisorySubscriber);
                advisorySubscriber.close();
                advisoryQueue.destroy();
            }
            catch (Exception e)
            {
                subscriber.logException(e);
                subscriber.setFailed();
            }
        }

        public void messagesReceived(List<Message> messages, EventQueue eventQueue)
        {
            // messagesReceived() is the message delivery callback method for the AdvisoryHandler class. Callbacks are associated
            // with a subscriber via queue.AddSubscriber(). The callback must be named MessagesReceived(), be a member of a class
            // which implements ISubscriberListener, return void, and take two arguments:
            //   - a list of messages (Message) being delivered to the callback in this invocation
            //   - an event queue (EventQueue), the event queue which dispatched the messages
            //
            // More than one message may be delivered in a single invocation. The callback must be able to handle the delivery
            // of multiple messages.
            int i;

            // Since more than one message may be delivered to this callback in a single invocation, loop through the messages
            // and process them.
            for (i = 0;  i < messages.size();  i++)
            {
                try
                {
                    Message msg = messages.get(i);
                    subscriber.log("Received advisory message:");
                    subscriber.log("advisory:");
                    subscriber.log("  %s: %s", Advisory.FIELD_SEVERITY, msg.getString(Advisory.FIELD_SEVERITY));
                    subscriber.log("  %s: %s", Advisory.FIELD_MODULE, msg.getString(Advisory.FIELD_MODULE));
                    subscriber.log("  %s: %s", Advisory.FIELD_NAME, msg.getString(Advisory.FIELD_NAME));
                    subscriber.log("  %s: %s", Advisory.FIELD_REASON, msg.getString(Advisory.FIELD_REASON));
                    subscriber.log("  %s: %s", Advisory.FIELD_TIMESTAMP, msg.getDateTime(Advisory.FIELD_TIMESTAMP).toString());
                    if (msg.isFieldSet(Advisory.FIELD_AGGREGATION_COUNT))
                    {
                        subscriber.log("  %s: %d", Advisory.FIELD_AGGREGATION_COUNT, msg.getLong(Advisory.FIELD_AGGREGATION_COUNT));
                    }
                    if (msg.isFieldSet(Advisory.FIELD_AGGREGATION_TIME))
                    {
                        subscriber.log("  %s: %f", Advisory.FIELD_AGGREGATION_TIME, msg.getDouble(Advisory.FIELD_AGGREGATION_TIME));
                    }
                    if (msg.isFieldSet(Advisory.FIELD_QUEUE_NAME))
                    {
                        subscriber.log("  %s: %s", Advisory.FIELD_QUEUE_NAME, msg.getString(Advisory.FIELD_QUEUE_NAME));
                    }
                    if (msg.isFieldSet(Advisory.FIELD_ENDPOINTS))
                    {
                        String[] eps = msg.getStringArray(Advisory.FIELD_ENDPOINTS);
                        if (eps.length > 0)
                        {
                            subscriber.log("  %s:", Advisory.FIELD_ENDPOINTS);
                            for (int idx = 0; idx < eps.length; idx++)
                            {
                                subscriber.log("    %s", eps[idx]);
                            }
                        }
                    }
                    if (msg.isFieldSet(Advisory.FIELD_SUBSCRIBER_NAME))
                    {
                        subscriber.log("  %s: %s", Advisory.FIELD_SUBSCRIBER_NAME, msg.getString(Advisory.FIELD_SUBSCRIBER_NAME));
                    }
                    if (msg.isFieldSet(Advisory.FIELD_LOCK_NAME))
                    {
                        subscriber.log("  %s: %s", Advisory.FIELD_LOCK_NAME, msg.getString(Advisory.FIELD_LOCK_NAME));
                    }
                }
                catch (Exception e)
                {
                    subscriber.logException(e);
                    subscriber.setFailed();
                }
            }
        }
    }

    public class SigIntHandler extends Thread
    {
        TibFtlSubscriber subscriber;
        public SigIntHandler(TibFtlSubscriber sub)
        {
            subscriber = sub;
        }

        @Override
        public void run()
        {
            int idx;
            int size = getSubscriberConfigLength();

            for (idx = 0; idx < size; idx++)
            {
                getSubscriberConfig(idx).setFinished();
            }
            TibFtlSubscriber.printStats();
        }
    }

    ApplicationConfiguration appConfig;
    private static SubscriberConfiguration[] subscriberConfig;
    public int getSubscriberConfigLength()
    {
        return subscriberConfig.length;
    }
    public SubscriberConfiguration getSubscriberConfig(int idx)
    {
        return subscriberConfig[idx];
    }

    private void usage()
    {
        System.out.println("Usage: TibFtlSubscriber [options] url");
        System.out.printf ("Default url is %s%n", ApplicationConfiguration.DEFAULT_REALM_SERVICE);
        System.out.println("Options (default value is in parentheses):");
        System.out.println("    --application appname");
        System.out.printf ("        Connect to realm service using application appname (\"%s\")%n", ApplicationConfiguration.DEFAULT_APPLICATION_NAME);
        System.out.println("    --client-label clabel");
        System.out.println("        Use clabel as client label prefix (\"tibftlsubscriber_PID\")");
        System.out.println("    --durablename name");
        System.out.println("        Use name as the durable name base");
        System.out.println("    --endpoint ep");
        System.out.printf ("        Publish on endpoint ep (\"%s\")%n", ApplicationConfiguration.DEFAULT_ENDPOINT_NAME);
        System.out.println("    --explicitack");
        System.out.println("        Explicitly acknowledge each message");
        System.out.println("    --help");
        System.out.println("        Display this help");
        System.out.println("    --matcher m");
        System.out.println("        Use m as the matcher string for each subscriber");
        System.out.println("    --password pw");
        System.out.printf ("        Authenticate to realm service using password pw (\"%s\")%n", ApplicationConfiguration.DEFAULT_PASSWORD);
        System.out.println("    --subscribers sub");
        System.out.printf ("        Create sub subscribers (%d)%n", ApplicationConfiguration.DEFAULT_SUBSCRIBER_COUNT);
        System.out.println("    --trace level");
        System.out.println("        Set trace level (off, severe, warn, info, debug, verbose)");
        System.out.println("    --trustall");
        System.out.println("        Trust all realm services");
        System.out.println("    --trustfile file");
        System.out.println("        Authenticate realm service using trustfile file");
        System.out.println("    --unsubscribe");
        System.out.println("        Subscribers will unsubscribe from the durable at termination");
        System.out.println("    --user username");
        System.out.printf ("        Authenticate to realm service as user username (\"%s\")%n", ApplicationConfiguration.DEFAULT_USER);
        System.exit(1);
    }

    public ApplicationConfiguration parseArgs(String[] args)
    {
        int i = 0;
        int argc = args.length;
        String s = null;
        int n = 0;
        TibFtlAux aux = new TibFtlAux(args, null);
        ApplicationConfiguration config = new ApplicationConfiguration();

        for (i = 0; i < argc; i++)
        {
            s = aux.getString(i, "--application", "-application");
            if (s != null)
            {
                config.setApplicationName(s);
                i++;
                continue;
            }
            s = aux.getString(i, "--client-label", "-client-label");
            if (s != null)
            {
                config.setClientLabelBase(s);
                i++;
                continue;
            }
            s = aux.getString(i, "--durablename", "-durablename");
            if (s != null)
            {
                config.setDurableName(s);
                i++;
                continue;
            }
            s = aux.getString(i, "--endpoint", "-endpoint");
            if (s != null)
            {
                config.setEndpointName(s);
                i++;
                continue;
            }
            if (aux.getFlag(i, "--explicitack", "-explicitack"))
            {
                config.setExplicitAck(true);
                continue;
            }
            if (aux.getFlag(i, "--help", "-help"))
            {
                usage();
            }
            s = aux.getString(i, "--matcher", "-matcher");
            if (s != null)
            {
                config.setMatcherString(s);
                i++;
                continue;
            }
            s = aux.getString(i, "--password", "-password");
            if (s != null)
            {
                config.setPassword(s);
                i++;
                continue;
            }
            n = aux.getInt(i, "--subscribers", "-subscribers");
            if (n > -1)
            {
                if (n > 0)
                {
                    config.setSubscriberCount(n);
                    i++;
                    continue;
                }
                else
                {
                    System.out.println("--subscribers must be greater than 0");
                    usage();
                }
            }
            s = aux.getString(i, "--trace", "-trace");
            if (s != null)
            {
                config.setTraceLevel(s);
                i++;
                continue;
            }
            if (aux.getFlag(i, "--trustall", "-trustall"))
            {
                if (config.getTrustFile() != null)
                {
                    System.out.println("cannot specify both --trustall and --trustfile");
                    usage();
                }
                config.setTrustAll(true);
                continue;
            }
            s = aux.getString(i, "--trustfile", "-trustfile");
            if (s != null)
            {
                config.setTrustFile(s);
                if (config.getTrustAll())
                {
                    System.out.println("cannot specify both --trustall and --trustfile");
                    usage();
                }
                i++;
                continue;
            }
            if (aux.getFlag(i, "--unsubscribe", "-unsubscribe"))
            {
                config.setUnsubscribe(true);
                continue;
            }
            s = aux.getString(i, "--user", "-u");
            if (s != null)
            {
                config.setUser(s);
                i++;
                continue;
            }
            if (args[i].startsWith("-", 0))
            {
                System.out.printf("invalid option: %s%n", args[i]);
                usage();
            }
            config.setRealmService(args[i]);
        }

        return config;
    }

    public static void printStats()
    {
        if (subscriberConfig != null)
        {
            int idx;
            long cumulativeMessagesReceived = 0;
            long earliestStartTime = 0x7fffffffffffffffL;
            long latestEndTime = -1;
            int len = subscriberConfig.length;

            TibFtlLogger.getInstance().log(" ");
            for (idx = 0; idx < len; idx++)
            {
                Statistics stats;
                SubscriberConfiguration sub = subscriberConfig[idx];

                stats = sub.getStatistics();
                if (stats.messages > 0)
                {
                    double elapsed = ((double) (stats.endTime - stats.startTime)) / 1000000000.0;
                    double rate = ((double) stats.messages) / elapsed;

                    cumulativeMessagesReceived += stats.messages;
                    if (stats.startTime < earliestStartTime)
                    {
                        earliestStartTime = stats.startTime;
                    }
                    if (stats.endTime > latestEndTime)
                    {
                        latestEndTime = stats.endTime;
                    }
                    TibFtlLogger.getInstance().log("Subscriber %s received %d messages in %fs at %f msgs/sec", sub.clientLabel, stats.messages, elapsed, rate);
                }
                else
                {
                    TibFtlLogger.getInstance().log("Subscriber %s received no messages", sub.clientLabel);
                }
            }
            if (cumulativeMessagesReceived == 0)
            {
                TibFtlLogger.getInstance().log("No messages were received");
            }
            else
            {
                double totalReceiveTime = ((double) (latestEndTime - earliestStartTime)) / 1000000000.0;
                TibFtlLogger.getInstance().log("All subscribers received %d messages in %fs, rate=%f msgs/sec", cumulativeMessagesReceived, totalReceiveTime, ((double) cumulativeMessagesReceived) / totalReceiveTime);
            }
        }
    }

    public TibFtlSubscriber(String[] args)
    {
        this.appConfig = this.parseArgs(args);
        if (this.appConfig.getClientLabelBase() == null)
        {
            this.appConfig.setClientLabelBase(TibFtlAux.getUniqueName("tibftlsubscriber"));
        }
    }

    public int work() throws FTLException
    {
        int idx;
        SigIntHandler handler = null;
        boolean applicationFailed = false;

        TibFtlLogger.getInstance().log("#");
        TibFtlLogger.getInstance().log("# ./TibFtlSubscriber");
        TibFtlLogger.getInstance().log("# (FTL) %s", FTL.getVersionInformation());
        TibFtlLogger.getInstance().log("#");
        TibFtlLogger.getInstance().log("# Client base label %s", this.appConfig.getClientLabelBase());
        TibFtlLogger.getInstance().log("#");

        // Print the configuration info
        TibFtlLogger.getInstance().log("# Application: %s", this.appConfig.getApplicationName());
        TibFtlLogger.getInstance().log("# Endpoint: %s", this.appConfig.getEndpointName());
        TibFtlLogger.getInstance().log("# Subscribers: %s", this.appConfig.getSubscriberCount());
        if (this.appConfig.getMatcherString() != null)
        {
            TibFtlLogger.getInstance().log("# Matcher string: %s", this.appConfig.getMatcherString());
        }
        if (this.appConfig.getDurableName() != null)
        {
            TibFtlLogger.getInstance().log("# Durable base name: %s", this.appConfig.getDurableName());
        }
        TibFtlLogger.getInstance().log("# Messages are acked %s", (this.appConfig.getExplicitAck() ? "explicitly" : "automatically"));
        TibFtlLogger.getInstance().log("# Subscribers %s unsubscribe from the durable", (this.appConfig.getUnsubscribe() ? "will" : "do not"));

        subscriberConfig = new SubscriberConfiguration[this.appConfig.getSubscriberCount()];
        for (idx = 0; idx < this.appConfig.getSubscriberCount(); idx++)
        {
            String clientLabel = this.appConfig.getClientLabelBase() + "_" + String.format("%d", idx);
            String durable = null;

            if (this.appConfig.getDurableName() != null)
            {
                durable = this.appConfig.getDurableName() + "_" + String.format("%d", idx);
            }
            subscriberConfig[idx] = new SubscriberConfiguration(this.appConfig, clientLabel, durable);
        }

        if (this.appConfig.getTraceLevel() != null)
        {
            FTL.setLogLevel(this.appConfig.getTraceLevel());
        }

        try
        {
            for (idx = 0; idx < this.appConfig.getSubscriberCount(); idx++)
            {
                subscriberConfig[idx].start();
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.exit(1);
        }

        handler = new SigIntHandler(this);
        Runtime.getRuntime().addShutdownHook(handler);
        try
        {
            for (idx = 0; idx < this.appConfig.getSubscriberCount(); idx++)
            {
                subscriberConfig[idx].join();
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.exit(1);
        }
        Runtime.getRuntime().removeShutdownHook(handler);
        TibFtlSubscriber.printStats();

        for (idx = 0; idx < this.appConfig.getSubscriberCount(); idx++)
        {
            applicationFailed |= subscriberConfig[idx].isFailed();
        }
        if (applicationFailed)
        {
            System.exit(1);
        }
        return 0;
    }
    
    public static void main(String[] args)
    {
        TibFtlSubscriber s  = new TibFtlSubscriber(args);
        try
        {
            s.work();
        }
        catch (Throwable e)
        {
            // Handles all exceptions including FTLException.
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }
}

