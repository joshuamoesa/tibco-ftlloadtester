/*
 * Copyright (c) 2010-2020 TIBCO Software Inc.
 * All Rights Reserved. Confidential & Proprietary.
 * For more information, please contact:
 * TIBCO Software Inc., Palo Alto, California, USA
 */

/*
 * This is a sample of a basic Java FTL shared durable producer program.
 */

package com.tibco.ftl.samples;

import java.util.*;

import com.tibco.ftl.*;
import com.tibco.ftl.exceptions.*;

/*
 * This class implements two interfaces

 *   - EventTimerListener which receive notifications from FTL timer objects.

 *   - NotificationHandler which is used to process notifications.
 *      In some situations, FTL must notify programs of conditions that preclude the use
 *      of event queues (as the communications medium). Instead of sending an advisory, 
 *      FTL notifies the program through this out-of-band mechanism.
 */
public class TibFtlSharedProducer implements EventTimerListener, NotificationHandler
{
    String          sampleName      = "tibftlsharedproducer";
    String          realmService    = TibFtlAux.DEFAULT_URL;
    String          applicationName = "default";
    String          endpointName    = "shared";
    boolean         finished        = false;
    long            messagesToSend  = 1000;
    long            messagesSent    = 0;
    double          interval        = 0.5;
    Publisher       pub             = null;
    Message         msg             = null;
    String          clientName;

    public TibFtlSharedProducer(String[] args)
    {
        clientName = TibFtlAux.getUniqueName(sampleName);

        System.out.printf("#\n");
        System.out.printf("# ./%s\n", sampleName);
        System.out.printf("# (FTL) %s\n", FTL.getVersionInformation());
        System.out.printf("#\n");
        System.out.printf("# Client name %s\n", clientName);
        System.out.printf("#\n");

        // See if a url is specified. If so, use it rather than the default.
        if (args.length > 0 && args[0] != null)
            realmService = args[0];    
    }

    public void onNotification(int type, String reason)
    {
        /*
         * onNotification() is the notification handler callback. In some situation, FTL must notify a
         * client application of a condition which can't be delivered through an event queue. Instead 
         * of sending an advisory (which would require an event queue), FTL uses an out-of-band mechanism 
         * to notify the client application.
     
         * The notification callback must be a function that returns void, and takes two arguments:
         *   - an integer value (notification code), which indicates the type of condition.
         *   - a string describing the reason for the notification
    
         * Currently there is only one type of notification: CLIENT_DISABLED, indicating the client 
         * application has been administratively disabled.
         */
        if (type == NotificationHandler.CLIENT_DISABLED)
        {
            System.out.println("Application administratively disabled: " + reason);
            finished = true;
        }
        else
        {
            // But just in case additional notification types are added, handle the rest of them.
            System.out.println("Notification type " + type + ": " + reason);
        }
    }

    public void timerFired(EventTimer timer,EventQueue eventQueue)
    {
        if (finished) 
        {
            return;
        }

        if (messagesSent >= messagesToSend)
        {
            return;
        }
 
        try
        {
            msg.clearAllFields();
            msg.setString("sender", clientName);
            msg.setLong("sequence", messagesSent);
            System.out.println("Sending message sequence " + messagesSent);
            pub.send(msg);
        }
        catch (FTLException e)
        {
            e.printStackTrace();
        }
        messagesSent = messagesSent + 1;
        if (messagesSent == messagesToSend)
        {
            finished = true;
        }
    }

    public int produce() throws FTLException
    {
        Realm              realm               = null;
        EventQueue         queue               = null;
        Thread             advisoryDispatch    = null;
        EventTimer         timer               = null;   
 
        /*
         * A properties object (TibProperties) allows the client application to set attributes (or properties) for an object
         * when the object is created. In this case, set the client label property (PROPERTY_STRING_CLIENT_LABEL) for the
         * realm connection.
         */
        TibProperties   props  = null;  
        props = FTL.createProperties();
        props.set(Realm.PROPERTY_STRING_CLIENT_LABEL, clientName);
        
        /*
         * Establish a connection to the realm service. Specify a string containing the realm service URL, and
         * the application name. The last argument is the properties object, which was created above.

         * Note the application name (the value contained in applicationName, which is "default"). This is the default application,
         * provided automatically by the realm service. It could also be specified as null - it mean the same thing,
         * the default application.
         */
        realm = FTL.connectToRealmServer(realmService, applicationName, props);

        /* 
         * It is good practice to clean up objects when they are no longer needed. Since the realm properties object is no
         * longer needed, destroy it now.
         */
        props.destroy();

        /*
         * Register a notification handler. This handler will be invoked when an administrative action occurs which
         * requires the client application to be notified.
         */
        realm.setNotificationHandler(this);

        /*
         * Create an HandleAdvisories object. This creates a separate event queue, creates a subscriber to the advisory
         * endpoint, adds the subscriber to the queue, to dispatch the advisory event queue.
         */        
        HandleAdvisories handleAdvisory = new HandleAdvisories(realm);    

        // Create a separate thread to do the advisory handling. 
        advisoryDispatch = new Thread(handleAdvisory);
        advisoryDispatch.start(); 

        /* For messages to be delivered to a client application, two things are required:
         *    1) A subscriber object must be created on a particular endpoint
         *    2) The created subscriber object must be added to an event queue to dispatch the received messages
         * Before the subscriber object can be added to an event queue, the event queue must exist. So that is the first
         * step. createEventQueue() method call on previously-created realm creates an event queue.
         */
        queue = realm.createEventQueue();

        /*
         * Before a message can be published, the publisher must be created on the previously-created realm. Specify the
         * endpoint on which messages are published.     
         */
        pub = realm.createPublisher(endpointName);

        /*
         * In order to send a message, the message object must first be created via a call to realm.createMessage(). Specify
         * the format name. Here the format "sample_shared" is used. This is a dynamic format, which means it is not defined in
         * the realm configuration. Dynamic formats allow for greater flexibility in that changes to the format only require
         * changes to the client applications using it, rather than an administrative change to the realm configuration.
         */
        msg = realm.createMessage("sample_shared");

        /*
         * A timer is created to drive the message production process. Timers are created with an interval (in seconds) specified.
         * When the interval expires, an event is placed on the event queue, to be dispatched by the thread dispatching that
         * queue. The timer is then automatically re-armed with the same interval. This process repeats until the timer is
         * destroyed.
         */
        timer = queue.createTimer(interval, this);
        
        while (!finished)
            queue.dispatch(1.0);

        // Once objects are no longer needed, they should be disposed of - and generally in reverse order of creation.

        if (timer != null)
        {
            // Destroy the timer first.
            queue.destroyTimer(timer);
        }
        // Next the message.
        msg.destroy();
        // And the publisher.
        pub.close();
        // Next destroy the event queue.
        queue.destroy();
        // Stop the advisory message processing.
        handleAdvisory.terminate();
        // Next close the connection to the realm service.        
        realm.close();

        return 0;
    }
    
    public static void main(String[] args)
    {
        TibFtlSharedProducer s  = new TibFtlSharedProducer(args);

        try {
            s.produce();
        }
        catch (Throwable e) {
            // Handles all exceptions including FTLException.
            e.printStackTrace();
        }
    } 
}

/*
 * This class implements SubscriberListener which is a Message event handler to process inbound messages.
 * This class will process advisory messages.
 */
class HandleAdvisories implements SubscriberListener, Runnable
{
    public volatile boolean terminate     = false;
    Subscriber      advisorySub   = null;         
    EventQueue      advisoryQueue = null;       

    HandleAdvisories(Realm realm){
        try
        {
            TibProperties   advisoryQueueProps  = null;
            /*
             * By default, a randomly-chosen name is given to an event queue by the FTL library. If the queue encounters any
             * issues (such as discarding events), an advisory message is generated which includes the queue name. It is good
             * practice to assign a meaningful name when creating the event queue, to facilitate diagnosing event queue issues.
             * The queue name, as well as the discard policy, is set via a properties object.
             */
            advisoryQueueProps = FTL.createProperties();
            // Set the queue name property (EventQueue.PROPERTY_STRING_NAME) to "advisory_queue".
            advisoryQueueProps.set(EventQueue.PROPERTY_STRING_NAME, "advisory_queue");
            /*
             * Also set the discard policy (EventQueue.PROPERTY_INT_DISCARD_POLICY) to EventQueue.DISCARD_NONE,
             * indicating not to discard any events. This is the default, but setting it explicitly is good practice.
             */
            advisoryQueueProps.set(EventQueue.PROPERTY_INT_DISCARD_POLICY, EventQueue.DISCARD_NONE);
            advisoryQueue = realm.createEventQueue(advisoryQueueProps);
            advisoryQueueProps.destroy();
            /*
             * Create the advisory subscriber object. Specify the endpoint name and an optional content matcher.
             * All advisory messages are delivered to the endpoint defined by Advisory.ENDPOINT_NAME.
             */
            advisorySub = realm.createSubscriber(Advisory.ENDPOINT_NAME, null);
            /*
             * Next add the advisory subscriber object to the event queue. This allows the event queue to
             * dispatch messages sent to that subscriber. Specify the subscriber object, and an object which implements
             * SubscriberListener (this class) and provides a messagesReceived() method.
             */
             advisoryQueue.addSubscriber(advisorySub, this);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }        
    }

    public void run() {
        try
        {
            do {
                advisoryQueue.dispatch();
            }while (!terminate);
        
            advisoryQueue.removeSubscriber(advisorySub);

            advisorySub.close();

            advisoryQueue.destroy();
        }
        catch (Exception e) 
        {
            e.printStackTrace();
        }                    
    }

    public void terminate() {
        terminate = true;    
    }    
 
    public void messagesReceived(List<Message> messages, EventQueue eventQueue)
    {
        /*
         * messagesReceived() is the message delivery callback function for this sample. It is a
         * method of SubscriberListener interface. Callbacks are associated with a subscriber via
         * addSubscriber() method of event queue. The callback must be a function that returns void,
         * and takes two arguments:
         *   - an array of messages (Message) being delivered to the callback in this invocation
         *   - an event queue (EventQueue), the event queue which dispatched the messages

         * More than one message may be delivered in a single invocation. The number of messages
         * is contained in the msgNum argument. Each message is an element in the messages argument.
         * The callback must be able to handle the delivery of multiple messages.

         * Since more than one message may be delivered to this callback in a single invocation,
         * loop through the messages and process them.
         */
        int msgNum = messages.size();

        for (int i = 0;  i < msgNum;  i++)
        {
            Message msg = messages.get(i);
            try
            {
                System.out.print("Received advisory message:");
    
                System.out.print("advisory:");
    
                System.out.println("Name: " + msg.getString(Advisory.FIELD_NAME));

                System.out.println("    Severity: " +
                                       msg.getString(Advisory.FIELD_SEVERITY));
                System.out.println("    Module: " +
                                       msg.getString(Advisory.FIELD_MODULE));
                System.out.println("    Reason: " +
                                       msg.getString(Advisory.FIELD_REASON));
                System.out.println("    Timestamp: " +
                                       msg.getDateTime(Advisory.FIELD_TIMESTAMP).toString());

                if (msg.isFieldSet(Advisory.FIELD_AGGREGATION_COUNT))
                {
                    System.out.println("    Aggregation Count: " +
                                       msg.getLong(Advisory.FIELD_AGGREGATION_COUNT));
                }

                if (msg.isFieldSet(Advisory.FIELD_AGGREGATION_TIME))
                {
                    System.out.println("    Aggregation Time: " +
                                       msg.getDouble(Advisory.FIELD_AGGREGATION_TIME));
                }

                if (msg.isFieldSet(Advisory.FIELD_ENDPOINTS))
                {
                    System.out.println("    Endpoints:");
                    String[] sarray = msg.getStringArray(Advisory.FIELD_ENDPOINTS);
                    for (int j = 0; j < sarray.length; j++)
                        System.out.println("      " + sarray[j]);
                }

                if (msg.isFieldSet(Advisory.FIELD_SUBSCRIBER_NAME))
                {
                    System.out.println("  Subscriber Name: " +
                                           msg.getString(Advisory.FIELD_SUBSCRIBER_NAME));
                }

                if (msg.isFieldSet(Advisory.FIELD_LOCK_NAME))
                {
                    System.out.println("  Lock Name: " +
                                       msg.getString(Advisory.FIELD_LOCK_NAME));
                }

            }
            catch(FTLException exp)
            {
                exp.printStackTrace();
            }
        }        
    }
}    
