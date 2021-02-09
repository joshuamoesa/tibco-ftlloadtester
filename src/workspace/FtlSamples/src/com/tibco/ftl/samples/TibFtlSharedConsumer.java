/*
 * Copyright (c) 2010-2020 TIBCO Software Inc.
 * All Rights Reserved. Confidential & Proprietary.
 * For more information, please contact:
 * TIBCO Software Inc., Palo Alto, California, USA
 */

/*
 *This is a sample of a basic Java FTL shared durable consumer program.
 */

package com.tibco.ftl.samples;

import java.util.*;

import com.tibco.ftl.*;
import com.tibco.ftl.exceptions.*;

/*
 * This class implements two interfaces

 *   - SubscriberListener which is a Message event handler to process inbound messages.

 *   - NotificationHandler which is used to process notifications.
 *      In some situations, FTL must notify programs of conditions that preclude the use
 *      of event queues (as the communications medium). Instead of sending an advisory, 
 *      FTL notifies the program through this out-of-band mechanism.
 */
public class TibFtlSharedConsumer implements SubscriberListener, NotificationHandler
{
    String          sampleName      = "tibftlsharedconsumer";
    String          realmService    = TibFtlAux.DEFAULT_URL;
    String          applicationName = "default";
    String          endpointName    = "shared";
    String          durableName     = "sample-shared-durable";
    boolean         finished        = false;
    String          clientName;

    public TibFtlSharedConsumer(String[] args)
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
        int i;
        int msgNum = messages.size();
         
        for (i = 0;  i < msgNum;  i++) 
        {
            if (messages.get(i) != null){
                System.out.print("Received message");
                System.out.println(" " + messages.get(i).toString());            
            }
        }
    }
    
    public int consume() throws FTLException
    {
        Realm           realm               = null;
        Subscriber      sub                 = null;
        EventQueue      queue               = null;
        TibProperties   subscriberProps     = null;
        Thread          advisoryDispatch    = null;     
 
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
         * Create an AdvisoryHandler object. This creates a separate event queue, creates a subscriber to the advisory
         * endpoint, adds the subscriber to the queue, to dispatch the advisory event queue.        
         */
        AdvisoryHandler handleAdvisory = new AdvisoryHandler(realm);    

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
         * A shared durable essentially acts like a queue. Messages are stored in the persistent server, and apportioned among
         * active subscribers to the durable in a round-robin fashion. Each subscriber must specify the same durable name.
         * The durable name is defined by properties specified when the subscriber is created.     
         */
        subscriberProps = FTL.createProperties();
        subscriberProps.set(Subscriber.PROPERTY_STRING_DURABLE_NAME, durableName);   

        /*
         * Next, create the subscriber object. Specify the endpoint name, any content matcher to be used, and the
         * properties object. 
         */
        sub = realm.createSubscriber(endpointName, null, subscriberProps);
        subscriberProps.destroy();

        /*
         * The last step is to add the subscriber object to the event queue. This allows the event queue to dispatch messages
         * sent to that subscriber. Specify the subscriber object, and an object which implements SubscriberListener ("this", which
         * is the TibFtlSharedConsumer class) and provides a messagesReceived() method.
         */
        queue.addSubscriber(sub, this);

        // Begin dispatching messages
        System.out.println("Waiting for message(s)");
        while (!finished)
            queue.dispatch(1.0);

        // Once objects are no longer needed, they should be disposed of - and generally in reverse order of creation.

        // First remove the subscriber from the event queue.
        queue.removeSubscriber(sub);
        // then close (destroy) the subscriber object.
        sub.close();
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
        TibFtlSharedConsumer s  = new TibFtlSharedConsumer(args);

        try {
            s.consume();
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
class AdvisoryHandler implements SubscriberListener, Runnable
{
    public volatile boolean terminate     = false;
    Subscriber      advisorySub           = null;         
    EventQueue      advisoryQueue         = null;       

    AdvisoryHandler(Realm realm){
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
            do 
            {
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
