/*
 * Copyright (c) 2010-2020 TIBCO Software Inc.
 * All Rights Reserved. Confidential & Proprietary.
 * For more information, please contact:
 * TIBCO Software Inc., Palo Alto, California, USA
 */

/*
 * This is a sample of a basic Jave Hello World FTL subscriber program which receives a
 * message and then cleans up and exits.
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
public class TibFtlRecv implements SubscriberListener, NotificationHandler
{
    String          sampleName      = "tibftlrecv";
    String          realmService    = TibFtlAux.DEFAULT_URL;
    String          applicationName = "default";
    String          endpointName    = "default";
    boolean         finished        = false;
    String          clientName;

    public TibFtlRecv(String[] args)
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

        // For this short sample only one message is expected. So when the message has been received, set a global flag
        // to communicate back to the main program that all messages have been received.
        finished = true;
    }
    
    public int recv() throws FTLException
    {
        Realm           realm  = null;
        Subscriber      sub    = null;
        EventQueue      queue  = null;
        ContentMatcher  cm     = null;
      
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

        /* For messages to be delivered to a client application, two things are required:
         *    1) A subscriber object must be created on a particular endpoint
         *    2) The created subscriber object must be added to an event queue to dispatch the received messages
         * Before the subscriber object can be added to an event queue, the event queue must exist. So that is the first
         * step. createEventQueue() method call on previously-created realm creates an event queue.
         */
        queue = realm.createEventQueue();

        /*
         * A content matcher is used to limit which messages are delivered to a client application based on the content
         * of the message. The string passed to createContentMatcher() contains JSON which specifies the field name
         * in the message, and the required value of that field.
     
         * In this case, the string
         *     {"type":"hello"}
         * matches any message which contains a field named "type" with the specific value "hello".
         */
        cm = realm.createContentMatcher("{\"type\":\"hello\"}");

        /*
         * Next, create the subscriber object. Specify the endpoint name, any content matcher to be used, 
         * and a properties object(which is not used in this case).
    
         * In the same manner as the application name above, the value of endpointName ("default") specifies the 
         * default endpoint, provided automatically by the realm service.
         */
        sub = realm.createSubscriber(endpointName, cm, null);

        /*
         * The last step is to add the subscriber object to the event queue. This allows the event queue to dispatch 
         * messages sent to that subscriber. Specify the subscriber object and callback (SubscriberListener).
     
         * The callback must be a function with a specific signature: see the messagesReceived() function above for 
         * an explanation of what it does.
         */
        queue.addSubscriber(sub, this);

        /*
         * For an event queue to dispatch messages, it must be run on a thread. That thread may be separately created and
         * managed, or it may be the main program thread - as it is in this sample.
     
         * dispatch() method call on previously-created event queue is the FTL API function that actually dispatches messages.
         * It should wait for messages to be available for dispatch before returning.

         * Since messages arrive asynchronously, the dispatch() method call is contained in a loop, which terminates
         * only when the finished flag is true, indicating the message has been received.
         */
        System.out.printf("waiting for message(s)\n");
        while (!finished)
            queue.dispatch();


        // Once objects are no longer needed, they should be disposed of - and generally in reverse order of creation.

        // First remove the subscriber from the event queue.
        queue.removeSubscriber(sub);
        // then close (destroy) the subscriber object.
        sub.close();
        // Since the content matcher is associated with the subscriber object, it should be destroyed only after the
        // subscriber object is destroyed.
        cm.destroy();
        // Next destroy the event queue.
        queue.destroy();
        // Next close the connection to the realm service.        
        realm.close();

        return 0;
    }
    
    public static void main(String[] args)
    {
        TibFtlRecv s  = new TibFtlRecv(args);

        try {
            s.recv();
        }
        catch (Throwable e) {
            // Handles all exceptions including FTLException.
            e.printStackTrace();
        }
    } 
}
