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
import java.time.*;
import com.tibco.ftl.*;
import com.tibco.ftl.exceptions.*;


public class TibFtlPublisher
{
    public final class ApplicationConfiguration
    {
        static final String DEFAULT_REALM_SERVICE = TibFtlAux.DEFAULT_URL;
        static final String DEFAULT_APPLICATION_NAME = "default";
        static final String DEFAULT_ENDPOINT_NAME = "default";
        static final String DEFAULT_USER = "guest";
        static final String DEFAULT_PASSWORD = "guest-pw";
        static final String DEFAULT_FORMAT_NAME = "Format-1";
        static final int DEFAULT_BATCH_SIZE = 1;
        static final int DEFAULT_MESSAGE_COUNT = 10000;
        static final int DEFAULT_PUBLISHER_COUNT = 2;
        static final int DEFAULT_MESSAGES_PER_SECOND = 100;
        static final int DEFAULT_OPAQUE_VALUE_SIZE = 5;

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
        private String formatName;
        public String getFormatName() { return formatName; }
        public void setFormatName(String v) { formatName = v; }
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
        private boolean keyedOpaque;
        public boolean getKeyedOpaque() { return keyedOpaque; }
        public void setKeyedOpaque(boolean v) { keyedOpaque = v; }
        private int batchSize;
        public int getBatchSize() { return batchSize; }
        public void setBatchSize(int v) { batchSize = v; }
        private int publisherCount;
        public int getPublisherCount() { return publisherCount; }
        public void setPublisherCount(int v) { publisherCount = v; }
        private int messagesPerSecond;
        public int getMessagesPerSecond() { return messagesPerSecond; }
        public void setMessagesPerSecond(int v) { messagesPerSecond = v; }
        private int messageCount;
        public int getMessageCount() { return messageCount; }
        public void setMessageCount(int v) { messageCount = v; }
        private int opaqueValueSize;
        public int getOpaqueValueSize() { return opaqueValueSize; }
        public void setOpaqueValueSize(int v) { opaqueValueSize = v;}
        private byte[] opaqueValue;
        public byte[] getOpaqueValue() { return opaqueValue; }

        public ApplicationConfiguration()
        {
            this.realmService = DEFAULT_REALM_SERVICE;
            this.applicationName = DEFAULT_APPLICATION_NAME;
            this.endpointName = DEFAULT_ENDPOINT_NAME;
            this.user = DEFAULT_USER;
            this.password = DEFAULT_PASSWORD;
            this.formatName = DEFAULT_FORMAT_NAME;
            this.trustAll = false;
            this.trustFile = null;
            this.clientLabelBase = null;
            this.traceLevel = null;
            this.keyedOpaque = false;
            this.batchSize = DEFAULT_BATCH_SIZE;
            this.publisherCount = DEFAULT_PUBLISHER_COUNT;
            this.messagesPerSecond = DEFAULT_MESSAGES_PER_SECOND;
            this.messageCount = DEFAULT_MESSAGE_COUNT;
            this.opaqueValueSize = DEFAULT_OPAQUE_VALUE_SIZE;
        }

        public void initOpaqueValue()
        {
            this.opaqueValue = new byte[this.opaqueValueSize];
            Arrays.fill(this.opaqueValue, (byte) '@');
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

    public class PublisherConfiguration extends Thread implements EventTimerListener, NotificationHandler
    {
        // Configuration data
        private String realmService;
        private String applicationName;
        private String endpointName;
        private String user;
        private String password;
        private String formatName;
        private boolean trustAll;
        private String trustFile;
        private boolean keyedOpaque;
        private String clientLabel;
        private int batchSize;
        private ApplicationConfiguration config;
        // FTL objects
        private Realm realm;
        private Publisher publisher;
        private Message[] message;
        private int messageCount;
        private MessageFieldRef refKey;
        private MessageFieldRef refData;
        private MessageFieldRef refLong;
        private MessageFieldRef refString;
        private MessageFieldRef refOpaque;

        private int messagesToSend;
        private int messagesPerSecond;
        private double sendInterval;
        private int messagesSent;
        private long sendStartTime;
        private long sendEndTime;
        private boolean disabled;
        private boolean finished;
        private boolean failed;

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
            return new Statistics(this.messagesSent, this.sendStartTime, this.sendEndTime);
        }

        public PublisherConfiguration(ApplicationConfiguration config, String clientLabel, int msgsToSend, int msgsPerSecond, double sendInterval)
        {
            this.config = config;
            this.realmService = config.getRealmService();
            this.applicationName = config.getApplicationName();
            this.endpointName = config.getEndpointName();
            this.user = config.getUser();
            this.password = config.getPassword();
            this.formatName = config.getFormatName();
            this.trustAll = config.getTrustAll();
            this.trustFile = config.getTrustFile();
            this.keyedOpaque = config.getKeyedOpaque();
            this.clientLabel = clientLabel;
            this.batchSize = config.getBatchSize();
            this.realm = null;
            this.publisher = null;
            this.message = null;
            this.messageCount = 0;
            this.refKey = null;
            this.refData = null;
            this.refLong = null;
            this.refString = null;
            this.refOpaque = null;
            this.messagesToSend = msgsToSend;
            this.messagesPerSecond = msgsPerSecond;
            this.sendInterval = sendInterval;
            this.messagesSent = 0;
            this.sendStartTime = -1;
            this.sendEndTime = -1;
            this.disabled = false;
            this.finished = false;
            this.failed = false;
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

        private void initializeFieldReferences() throws FTLException
        {
            if (this.keyedOpaque)
            {
                this.refKey = this.realm.createMessageFieldRef(Message.TIB_BUILTIN_MSG_FMT_KEY_FIELDNAME);
                this.refData = this.realm.createMessageFieldRef(Message.TIB_BUILTIN_MSG_FMT_OPAQUE_FIELDNAME);
            }
            else
            {
                this.refLong = this.realm.createMessageFieldRef("My-Long");
                this.refString = this.realm.createMessageFieldRef("My-String");
                this.refOpaque = this.realm.createMessageFieldRef("My-Opaque");
            }
        }

        private void cleanupFieldReferences() throws FTLException
        {
            if (this.keyedOpaque)
            {
                this.refKey.destroy();
                this.refData.destroy();
            }
            else
            {
                this.refLong.destroy();
                this.refString.destroy();
                this.refOpaque.destroy();
            }
        }

        private void setMessageFields(Message msg, long sqn) throws FTLException
        {
            if (this.keyedOpaque)
            {
                String value = String.format("%d", sqn);
                msg.setString(this.refKey, value);
                msg.setOpaque(this.refData, this.config.getOpaqueValue());
            }
            else
            {
                msg.setLong(this.refLong, sqn);
                msg.setString(this.refString, this.clientLabel);
                msg.setOpaque(this.refOpaque, this.config.getOpaqueValue());
            }
        }

        @Override
        public void run()
        {
            TibProperties realmProps = null;
            AdvisoryHandler advisory = null;
            EventQueue queue = null;
            int idx;
            EventTimer timer = null;

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

                this.initializeFieldReferences();

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

                // Before a message can be published, the publisher must be created on the previously-created realm. Specify the
                // endpoint on which messages are published.
                this.publisher = this.realm.createPublisher(endpointName);

                this.messageCount = this.batchSize;
                this.message = new Message[this.messageCount];
                for (idx = 0; idx < this.messageCount; idx++)
                {
                    this.message[idx] = this.realm.createMessage(this.formatName);
                }

                timer = queue.createTimer(this.sendInterval, this);
                // Establish the send start time for this publisher
                this.sendStartTime = System.nanoTime();

                while (!this.isFinished() && !this.isDisabled())
                {
                    queue.dispatch(1.0);
                }

                // Clean up objects in the reverse order of creation.
                queue.destroyTimer(timer);
                for (idx = 0; idx < this.messageCount; idx++)
                {
                    this.message[idx].destroy();
                }
                this.publisher.close();
                queue.destroy();
                advisory.stop();
                this.cleanupFieldReferences();
                this.realm.close();
            }
            catch (Exception e)
            {
                logException(e);
                this.setFailed();
            }
        }

        public void timerFired(EventTimer timer, EventQueue queue)
        {
            int msgsToSendThisInterval;
            int messagesRemainingToSend = (this.messagesToSend - this.messagesSent);
            int idx;
            long now = System.nanoTime();
            int messagesToHaveSentSoFar;
            double elapsed;

            if (this.isFinished() || this.isDisabled())
            {
                return;
            }
            if (messagesRemainingToSend <= 0)
            {
                this.setFinished();
                return;
            }
            elapsed = (now - this.sendStartTime) / 1000000000.0;
            messagesToHaveSentSoFar = (int) (Math.ceil(elapsed * (double) this.messagesPerSecond));
            if (messagesToHaveSentSoFar > this.messagesToSend)
            {
                messagesToHaveSentSoFar = this.messagesToSend;
            }
            msgsToSendThisInterval = messagesToHaveSentSoFar - this.messagesSent;
            if (msgsToSendThisInterval > messagesRemainingToSend)
            {
                msgsToSendThisInterval = messagesRemainingToSend;
            }
            while (msgsToSendThisInterval > 0)
            {
                int msgsToSendThisIteration = msgsToSendThisInterval;

                if ((this.batchSize > 1) && (this.batchSize < msgsToSendThisIteration))
                {
                    msgsToSendThisIteration = this.batchSize;
                }
                try
                {
                    if (this.batchSize > 1)
                    {
                        for (idx = 0; idx < this.messageCount && idx < msgsToSendThisIteration; idx++)
                        {
                            setMessageFields(this.message[idx], this.messagesSent + idx);
                        }
                        this.publisher.send(this.message, msgsToSendThisIteration);
                    }
                    else
                    {
                        for (idx = 0; idx < msgsToSendThisIteration; idx++)
                        {
                            this.setMessageFields(this.message[0], this.messagesSent + idx);
                            this.publisher.send(this.message[0]);
                        }
                    }
                }
                catch (Exception e)
                {
                    logException(e);
                    this.setFailed();
                }
                this.messagesSent += msgsToSendThisIteration;
                msgsToSendThisInterval -= msgsToSendThisIteration;
            }
            this.sendEndTime = System.nanoTime();
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
        PublisherConfiguration publisher = null;

        private AdvisoryHandler() { }

        public AdvisoryHandler(PublisherConfiguration publisher)
        {
            TibProperties advisoryQueueProps = null;

            this.publisher = publisher;

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
                advisoryQueue = publisher.realm.createEventQueue(advisoryQueueProps);
                advisoryQueueProps.destroy();
                // Create the advisory subscriber object. Specify the endpoint name and an optional content matcher.
                // All advisory messages are delivered to the endpoint defined by Advisory.ENDPOINT_NAME.
                advisorySubscriber = publisher.realm.createSubscriber(Advisory.ENDPOINT_NAME, null);
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
                this.publisher.logException(e);
                this.publisher.setFailed();
            }
        }

        @Override
        public void run()
        {
            do
            {
                try
                {
                    advisoryQueue.dispatch(1.0);
                }
                catch (Exception e)
                {
                    publisher.logException(e);
                    publisher.setFailed();
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
                publisher.logException(e);
                publisher.setFailed();
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
                    publisher.log("Received advisory message:");
                    publisher.log("advisory:");
                    publisher.log("  %s: %s", Advisory.FIELD_SEVERITY, msg.getString(Advisory.FIELD_SEVERITY));
                    publisher.log("  %s: %s", Advisory.FIELD_MODULE, msg.getString(Advisory.FIELD_MODULE));
                    publisher.log("  %s: %s", Advisory.FIELD_NAME, msg.getString(Advisory.FIELD_NAME));
                    publisher.log("  %s: %s", Advisory.FIELD_REASON, msg.getString(Advisory.FIELD_REASON));
                    publisher.log("  %s: %s", Advisory.FIELD_TIMESTAMP, msg.getDateTime(Advisory.FIELD_TIMESTAMP).toString());
                    if (msg.isFieldSet(Advisory.FIELD_AGGREGATION_COUNT))
                    {
                        publisher.log("  %s: %d", Advisory.FIELD_AGGREGATION_COUNT, msg.getLong(Advisory.FIELD_AGGREGATION_COUNT));
                    }
                    if (msg.isFieldSet(Advisory.FIELD_AGGREGATION_TIME))
                    {
                        publisher.log("  %s: %f", Advisory.FIELD_AGGREGATION_TIME, msg.getDouble(Advisory.FIELD_AGGREGATION_TIME));
                    }
                    if (msg.isFieldSet(Advisory.FIELD_QUEUE_NAME))
                    {
                        publisher.log("  %s: %s", Advisory.FIELD_QUEUE_NAME, msg.getString(Advisory.FIELD_QUEUE_NAME));
                    }
                    if (msg.isFieldSet(Advisory.FIELD_ENDPOINTS))
                    {
                        String[] eps = msg.getStringArray(Advisory.FIELD_ENDPOINTS);
                        if (eps.length > 0)
                        {
                            publisher.log("  %s:", Advisory.FIELD_ENDPOINTS);
                            for (int idx = 0; idx < eps.length; idx++)
                            {
                                publisher.log("    %s", eps[idx]);
                            }
                        }
                    }
                    if (msg.isFieldSet(Advisory.FIELD_SUBSCRIBER_NAME))
                    {
                        publisher.log("  %s: %s", Advisory.FIELD_SUBSCRIBER_NAME, msg.getString(Advisory.FIELD_SUBSCRIBER_NAME));
                    }
                    if (msg.isFieldSet(Advisory.FIELD_LOCK_NAME))
                    {
                        publisher.log("  %s: %s", Advisory.FIELD_LOCK_NAME, msg.getString(Advisory.FIELD_LOCK_NAME));
                    }
                }
                catch (Exception e)
                {
                    publisher.log("Unexpected Exception: " + e);
                    publisher.setFailed();
                }
            }
        }
    }

    public class SigIntHandler extends Thread
    {
        TibFtlPublisher publisher;
        public SigIntHandler(TibFtlPublisher pub)
        {
            publisher = pub;
        }
        @Override
        public void run()
        {
            int idx;
            int size = getPublisherConfigLength();

            for (idx = 0; idx < size; idx++)
            {
                getPublisherConfig(idx).setFinished();
            }
            TibFtlPublisher.printStats();
        }
    }

    public class IntervalDefinition
    {
        private double interval;
        public double getInterval() { return interval; }
        private int intervalsPerSecond;
        public int getIntervalsPerSecond() { return intervalsPerSecond; }
        public IntervalDefinition(double ivl, int ips)
        {
            this.interval = ivl;
            this.intervalsPerSecond = ips;
        }
    }

    public final class NormalizedInterval
    {
        private static final int INTERVAL_COUNT = 16;
        private static final int MAX_INTERVAL = (INTERVAL_COUNT - 1);
        private IntervalDefinition[] intervals;
        public NormalizedInterval()
        {
            this.intervals = new IntervalDefinition[INTERVAL_COUNT];
            this.intervals[0] = new IntervalDefinition(0.001, 1000); /* 1ms */
            this.intervals[1] = new IntervalDefinition(0.002, 500);  /* 2ms */
            this.intervals[2] = new IntervalDefinition(0.004, 250);  /* 4ms */
            this.intervals[3] = new IntervalDefinition(0.005, 200);  /* 5ms */
            this.intervals[4] = new IntervalDefinition(0.008, 125);  /* 8ms */
            this.intervals[5] = new IntervalDefinition(0.010, 100);  /* 10ms */
            this.intervals[6] = new IntervalDefinition(0.020, 50);  /* 20ms */
            this.intervals[7] = new IntervalDefinition(0.025, 40);  /* 25ms */
            this.intervals[8] = new IntervalDefinition(0.040, 25);  /* 40ms */
            this.intervals[9] = new IntervalDefinition(0.050, 20);  /* 50ms */
            this.intervals[10] = new IntervalDefinition(0.100, 10);  /* 100ms */
            this.intervals[11] = new IntervalDefinition(0.125, 8);   /* 125ms */
            this.intervals[12] = new IntervalDefinition(0.200, 5);   /* 200ms */
            this.intervals[13] = new IntervalDefinition(0.250, 4);   /* 250ms */
            this.intervals[14] = new IntervalDefinition(0.500, 2);   /* 500ms */
            this.intervals[15] = new IntervalDefinition(1.000, 1);   /* 1000ms */
        }

        public IntervalDefinition normalize(int msgsPerSecond, int minimumMsgsPerInterval)
        {
            // Normalize the interval (and thus the number of intervals per second) and the number of messages per interval so that the same number
            // of messages are sent in each interval - with the possible exception of the last interval in a second.
            int mpi;
            int idx;

            for (mpi = minimumMsgsPerInterval; mpi < msgsPerSecond; mpi++)
            {
                int ivls = (int) (Math.ceil(((double) msgsPerSecond) / ((double) mpi)));
                for (idx = 0; idx < INTERVAL_COUNT; idx++)
                {
                    if (ivls == intervals[idx].getIntervalsPerSecond())
                    {
                        return intervals[idx];
                    }
                }
            }
            return intervals[MAX_INTERVAL];
        }
    }

    ApplicationConfiguration appConfig;
    private static PublisherConfiguration[] publisherConfig;
    public int getPublisherConfigLength()
    {
        return publisherConfig.length;
    }

    public PublisherConfiguration getPublisherConfig(int idx)
    {
        return publisherConfig[idx];
    }

    private void usage()
    {
        System.out.println("Usage: TibFtlPublisher [options] url");
        System.out.printf ("Default url is %s%n", ApplicationConfiguration.DEFAULT_REALM_SERVICE);
        System.out.println("Options (default value is in parentheses):");
        System.out.println("    --application appname");
        System.out.printf ("        Connect to realm service using application appname (\"%s\")%n", ApplicationConfiguration.DEFAULT_APPLICATION_NAME); 
        System.out.println("    --batch n");
        System.out.printf ("        Publish in batches of n messages (%d)%n", ApplicationConfiguration.DEFAULT_BATCH_SIZE);
        System.out.println("    --client-label clabel");
        System.out.println("        Use clabel as client label prefix (\"tibftlpublisher_PID\")");
        System.out.println("    --count c");
        System.out.printf ("        Publish a total of c messages spread across all publishers (%d)%n", ApplicationConfiguration.DEFAULT_MESSAGE_COUNT);
        System.out.println("    --endpoint ep");
        System.out.printf ("        Publish on endpoint ep (\"%s\")%n", ApplicationConfiguration.DEFAULT_ENDPOINT_NAME);
        System.out.println("    --help");
        System.out.println("        Display this help");
        System.out.println("    --keyedopaque");
        System.out.println("        Messages are created using the keyed opaque built-in format");
        System.out.println("    --password pw");
        System.out.printf ("        Authenticate to realm service using password pw (\"%s\")%n", ApplicationConfiguration.DEFAULT_PASSWORD);
        System.out.println("    --publishers pub");
        System.out.printf ("        Create pub publishers (%d)%n", ApplicationConfiguration.DEFAULT_PUBLISHER_COUNT);
        System.out.println("    --rate mps");
        System.out.printf ("        Publish messages at mps messages per second across all publishers (%d messages per second)%n", ApplicationConfiguration.DEFAULT_MESSAGES_PER_SECOND);
        System.out.println("    --size len");
        System.out.printf ("        Set length of opaque field value to len bytes (%d)%n", ApplicationConfiguration.DEFAULT_OPAQUE_VALUE_SIZE);
        System.out.println("    --trace level");
        System.out.println("        Set trace level (off, severe, warn, info, debug, verbose)");
        System.out.println("    --trustall");
        System.out.println("        Trust all realm services");
        System.out.println("    --trustfile file");
        System.out.println("        Authenticate realm service using trustfile file");
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
            n = aux.getInt(i, "--batch", "-batch");
            if (n > -1)
            {
                if (n > 0)
                {
                    config.setBatchSize(n);
                    i++;
                    continue;
                }
                else
                {
                    System.out.println("--batch must be greater than 0");
                    usage();
                }
            }
            s = aux.getString(i, "--client-label", "-client-label");
            if (s != null)
            {
                config.setClientLabelBase(s);
                i++;
                continue;
            }
            n = aux.getInt(i, "--count", "-count");
            if (n > -1)
            {
                if (n > 0)
                {
                    config.setMessageCount(n);
                    i++;
                    continue;
                }
                else
                {
                    System.out.println("--count must be greater than 0");
                    usage();
                }
            }
            s = aux.getString(i, "--endpoint", "-endpoint");
            if (s != null)
            {
                config.setEndpointName(s);
                i++;
                continue;
            }
            if (aux.getFlag(i, "--help", "-help"))
            {
                usage();
            }
            if (aux.getFlag(i, "--keyedopaque", "-keyedopaque"))
            {
                config.setKeyedOpaque(true);
                continue;
            }
            s = aux.getString(i, "--password", "-password");
            if (s != null)
            {
                config.setPassword(s);
                i++;
                continue;
            }
            n = aux.getInt(i, "--publishers", "-publishers");
            if (n > -1)
            {
                if (n > 0)
                {
                    config.setPublisherCount(n);
                    i++;
                    continue;
                }
                else
                {
                    System.out.println("--publishers must be greater than 0");
                    usage();
                }
            }
            n = aux.getInt(i, "--rate", "-rate");
            if (n > -1)
            {
                if (n > 0)
                {
                    config.setMessagesPerSecond(n);
                    i++;
                    continue;
                }
                else
                {
                    System.out.println("--rate must be greater than 0");
                    usage();
                }
            }
            n = aux.getInt(i, "--size", "-size");
            if (n > -1)
            {
                if (n > 0)
                {
                    config.setOpaqueValueSize(n);
                    i++;
                    continue;
                }
                else
                {
                    System.out.println("--size must be greater than 0");
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

        if (config.getKeyedOpaque())
        {
            config.setFormatName(Message.TIB_BUILTIN_MSG_FMT_KEYED_OPAQUE);
        }
        return config;
    }

    public static void printStats()
    {
        if (publisherConfig != null)
        {
            int idx;
            long cumulativeMessagesSent = 0;
            long earliestStartTime = 0x7fffffffffffffffL;
            long latestEndTime = -1;
            int len = publisherConfig.length;

            TibFtlLogger.getInstance().log(" ");
            for (idx = 0; idx < len; idx++)
            {
                Statistics stats;
                PublisherConfiguration pub = publisherConfig[idx];

                stats = pub.getStatistics();
                if (stats.messages > 0)
                {
                    double elapsed = ((double) (stats.endTime - stats.startTime)) / 1000000000.0;
                    double rate = ((double) stats.messages) / elapsed;

                    cumulativeMessagesSent += stats.messages;
                    if (stats.startTime < earliestStartTime)
                    {
                        earliestStartTime = stats.startTime;
                    }
                    if (stats.endTime > latestEndTime)
                    {
                        latestEndTime = stats.endTime;
                    }
                    TibFtlLogger.getInstance().log("Publisher %s sent %d messages in %fs at %f msgs/sec", pub.clientLabel, stats.messages, elapsed, rate);
                }
                else
                {
                    TibFtlLogger.getInstance().log("Publisher %s sent no messages", pub.clientLabel);
                }
            }
            if (cumulativeMessagesSent == 0)
            {
                TibFtlLogger.getInstance().log("No messages were sent");
            }
            else
            {
                double totalSendTime = ((double) (latestEndTime - earliestStartTime)) / 1000000000.0;
                TibFtlLogger.getInstance().log("All publishers sent %d messages in %fs, rate=%f msgs/sec", cumulativeMessagesSent, totalSendTime, ((double) cumulativeMessagesSent) / totalSendTime);
            }
        }
    }

    public TibFtlPublisher(String[] args)
    {
        this.appConfig = this.parseArgs(args);
        if (this.appConfig.clientLabelBase == null)
        {
            this.appConfig.clientLabelBase = TibFtlAux.getUniqueName("tibftlpublisher");
        }
    }

    public int work() throws FTLException
    {
        int messagesPerPublisher;
        boolean shortMessageCountForLastPublisher = false;
        boolean lesserRateForLastPublisher = false;
        boolean batchSizeAdjusted = false;
        boolean rateAdjusted = false;
        int mpsPerPublisher;
        int idx;
        int remainingMessagesToSend;
        double sendInterval;
        int messagesPerInterval;
        int intervalsPerSecond;
        boolean publisherCountAdjusted = false;
        int remainingMessagesPerSecond;
        SigIntHandler handler = null;
        NormalizedInterval normalizer = new NormalizedInterval();
        IntervalDefinition ivl = null;
        boolean applicationFailed = false;

        TibFtlLogger.getInstance().log("#");
        TibFtlLogger.getInstance().log("# ./TibFtlPublisher");
        TibFtlLogger.getInstance().log("# (FTL) %s", FTL.getVersionInformation());
        TibFtlLogger.getInstance().log("#");
        TibFtlLogger.getInstance().log("# Client base label %s", this.appConfig.getClientLabelBase());
        TibFtlLogger.getInstance().log("#");

        // If fewer messages than publishers were specified, adjust the publisher count to allow 1 message per publisher.
        if (this.appConfig.getMessageCount() < this.appConfig.getPublisherCount())
        {
            this.appConfig.setPublisherCount(this.appConfig.getMessageCount());
        }
        // We need to have a high enough rate to send at least 1 message per second for each publisher.
        if (this.appConfig.getMessagesPerSecond() < this.appConfig.getPublisherCount())
        {
            this.appConfig.setMessagesPerSecond(this.appConfig.getPublisherCount());
            rateAdjusted = true;
        }
        // If the message count is not a multiple of the publisher count, the last publisher will send fewer messages.
        if ((this.appConfig.getMessageCount() % this.appConfig.getPublisherCount()) != 0)
        {
            shortMessageCountForLastPublisher = true;
        }
        // If the rate is not evenly divisable across all publishers, the last publisher will send at a slightly lesser rate.
        // For example, at 105 messages per second with 2 publishers, the first publisher will send at 53 mps, and the second
        // at 52 mps.
        if ((this.appConfig.getMessagesPerSecond() % this.appConfig.getPublisherCount()) != 0)
        {
            lesserRateForLastPublisher = true;
        }
        // Compute the number of messages per publisher. ceil() is used to round up to the smallest integral value
        // not less than the quotient.
        messagesPerPublisher = (int)(Math.ceil(((double) this.appConfig.getMessageCount()) / ((double) this.appConfig.getPublisherCount())));
        // Same for the messages per second per publisher.
        mpsPerPublisher = (int)(Math.ceil(((double) this.appConfig.getMessagesPerSecond()) / ((double) this.appConfig.getPublisherCount())));
        // If the batch size is larger than the messages per publisher, we can never send a full batch. So adjust
        // the batch size down.
        if (this.appConfig.getBatchSize() > messagesPerPublisher)
        {
            this.appConfig.setBatchSize(messagesPerPublisher);
            batchSizeAdjusted = true;
        }
        ivl = normalizer.normalize(mpsPerPublisher, this.appConfig.getBatchSize());
        sendInterval = ivl.getInterval();
        intervalsPerSecond = ivl.getIntervalsPerSecond();
        messagesPerInterval = (int) (Math.ceil((double) mpsPerPublisher / (double) intervalsPerSecond));

        // Print the configuration info
        TibFtlLogger.getInstance().log("# Messages per second: %d%s", this.appConfig.getMessagesPerSecond(), (rateAdjusted ? " (adjusted)" : ""));
        TibFtlLogger.getInstance().log("# Publishers: %d%s", this.appConfig.getPublisherCount(), (publisherCountAdjusted ? " (adjusted)": ""));
        TibFtlLogger.getInstance().log("# Batch size: %d%s", this.appConfig.getBatchSize(), (batchSizeAdjusted ? " (adjusted)" : ""));
        TibFtlLogger.getInstance().log("# Messages per publisher: %d%s", messagesPerPublisher, (shortMessageCountForLastPublisher ? " (less for the last publisher)" : ""));
        TibFtlLogger.getInstance().log("# Messages per second per publisher: %d%s", mpsPerPublisher, (lesserRateForLastPublisher ? " (less for the last publisher)" : ""));
        TibFtlLogger.getInstance().log("# Send interval: %dms, %d intervals per second", (long) (sendInterval * 1000.0), intervalsPerSecond);
        TibFtlLogger.getInstance().log("# Messages per interval per publisher: %d", messagesPerInterval);
        TibFtlLogger.getInstance().log("# Opaque field value size: %d", this.appConfig.getOpaqueValueSize());

        remainingMessagesToSend = this.appConfig.getMessageCount();
        remainingMessagesPerSecond = this.appConfig.getMessagesPerSecond();
        publisherConfig = new PublisherConfiguration[this.appConfig.getPublisherCount()];
        for (idx = 0; idx < this.appConfig.getPublisherCount(); idx++)
        {
            String clientLabel = this.appConfig.getClientLabelBase() + "_" + String.format("%d", idx);
            int msgsToSend;
            int msgsPerSecond;

            if (remainingMessagesToSend < messagesPerPublisher)
            {
                msgsToSend = remainingMessagesToSend;
            }
            else
            {
                msgsToSend = messagesPerPublisher;
            }
            remainingMessagesToSend -= msgsToSend;
            if (remainingMessagesPerSecond < mpsPerPublisher)
            {
                msgsPerSecond = remainingMessagesPerSecond;
            }
            else
            {
                msgsPerSecond = mpsPerPublisher;
            }
            remainingMessagesPerSecond -= msgsPerSecond;
            publisherConfig[idx] = new PublisherConfiguration(this.appConfig, clientLabel, msgsToSend, msgsPerSecond, sendInterval);
        }

        if (this.appConfig.getTraceLevel() != null)
        {
            FTL.setLogLevel(this.appConfig.getTraceLevel());
        }

        try
        {
            for (idx = 0; idx < this.appConfig.getPublisherCount(); idx++)
            {
                publisherConfig[idx].start();
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
            for (idx = 0; idx < this.appConfig.getPublisherCount(); idx++)
            {
                publisherConfig[idx].join();
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.exit(1);
        }
        Runtime.getRuntime().removeShutdownHook(handler);
        TibFtlPublisher.printStats();

        for (idx = 0; idx < this.appConfig.getPublisherCount(); idx++)
        {
            applicationFailed |= publisherConfig[idx].isFailed();
        }
        if (applicationFailed)
        {
            System.exit(1);
        }
        return 0;
    }
    
    public static void main(String[] args)
    {
        TibFtlPublisher s  = new TibFtlPublisher(args);
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

