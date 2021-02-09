/*
 * Copyright (c) 2010-2020 TIBCO Software Inc.
 * All Rights Reserved. Confidential & Proprietary.
 * For more information, please contact:
 * TIBCO Software Inc., Palo Alto, California, USA
 */

/*
 * This is a sample of a basic Java FTL program that replies to a request.
 */

package com.tibco.ftl.samples;

import java.util.*;
import com.tibco.ftl.*;

/*
 * This class implements two interfaces

 *   - SubscriberListener which is a Message event handler to process inbound messages.

 *   - NotificationHandler which is used to process notifications.
 *      In some situations, FTL must notify programs of conditions that preclude the use
 *      of event queues (as the communications medium). Instead of sending an advisory,
 *      FTL notifies the program through this out-of-band mechanism.
 */
public class TibFtlReply implements SubscriberListener, NotificationHandler
{
    String          sampleName      = "tibftlreply";
    String          realmService    = TibFtlAux.DEFAULT_URL;
    String          applicationName = "default";
    String          endpointName    = "default";
    boolean         finished        = false;
    Realm           realm           = null;
    String          clientName;

    public TibFtlReply(String[] args)
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
         * loop through the messages and process them.Also, note that because of the matcher specified
         * when adding this subscriber to the event queue,only request messages will be processed here.
         */  
        Publisher pub    = null;
        Message msg      = null;
        Message replyMsg = null;

        int i;
   
        try {
            int msgNum = messages.size();

            for (i = 0;  i < msgNum;  i++)
            {
                 msg = messages.get(i);
                 if (msg != null){
                     System.out.println("Received message");
                     System.out.println(" " + msg.toString());
         
                     /*
                      * Replying to a request requires a publisher. For this sample, a publisher will be 
                      * created for each reply being sent. This is not the most efficient way, but is the 
                      * simplest for this sample. Pass the endpoint onto which the messages will be 
                      * published.
                     
                      * In the same manner as the subscriber endpoint in the main program, the value of 
                      * endpointName ("default") specifies the default endpoint, provided automatically by 
                      * the realm service.
                      */                
                     pub = realm.createPublisher(endpointName);       

                     /*
                      * In order to send a message, the message object must first be created via a call to
                      * createMessage() on realm. Pass the the format name. Here the format "sample_reply" 
                      * is used. This is a dynamic format, which means it is not defined in the realm 
                      * configuration. Dynamic formats allow for greater flexibility in that changes to the 
                      * format only require changes to the client applications using it, rather than an 
                      * administrative change to the realm configuration.
                      */      
                     replyMsg = realm.createMessage("sample_reply");
          
                     /*
                      * A newly-created message is empty - it contains no fields. The next step is to add 
                      * one or more fields to the message. setString() method call on previously-created 
                      * message adds a string field. First, a string field named "type" is added, with the 
                      * value "reply". Then a long field named "timeInMills" with the help of setLong() 
                      * method call on previously-created message is added with the current time in 
                      * milliseconds as value.
                      */
                     replyMsg.setString("type", "reply");
                     replyMsg.setLong("timeInMills", TibFtlAux.getCurrentTime());
        
                     /* 
                      * To actually send the reply, sendReply() method call is used. Specify the reply message,
                      * and the original request message.
                      * Unlike sendRequest(), sendReply() does NOT complete synchronously.      
                      */
                     pub.sendReply(replyMsg, msg);

                     System.out.print("Reply sent:");
                     System.out.println(" " + replyMsg.toString());

                     // Once objects are no longer needed, they should be disposed of - and generally in reverse 
                     // order of creation.
                     
                     // First destroy the message.                     
                     replyMsg.destroy();
                     // Then close the publisher.
                     pub.close();
                 }
             }
        }
        catch(FTLException exp)
        {
            exp.printStackTrace();
        }
        finally 
        {
            /*
             * For this short sample only one request is expected. So when the message has been received, set a 
             * global flag to communicate back to the main program that all request messages have been received.
             */
            finished = true;
        }
    }

    public int reply() throws FTLException
    {
        Subscriber      sub      = null;
        EventQueue      queue    = null;
        ContentMatcher  cm       = null;
        TibProperties   props    = null;

        /*
         * A properties object (TibProperties) allows the client application to set attributes (or properties) for an object
         * when the object is created. In this case, set the client label property (PROPERTY_STRING_CLIENT_LABEL) for the
         * realm connection.
         */
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
         *     {"type":"request"}
         * matches any message which contains a field named "type" with the specific value "request".
         */
        cm = realm.createContentMatcher("{\"type\":\"request\"}");

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
        System.out.println("Waiting for message(s)");
        while (!finished)
            queue.dispatch();

        // Once objects are no longer needed, they should be disposed of - and generally in reverse order of creation.

        // First remove the subscriber from the event queue.
        queue.removeSubscriber(sub);
        // Then close (destroy) the subscriber object.
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
        TibFtlReply s  = new TibFtlReply(args);
        try
        {
            s.reply();
        }
        catch (Throwable e)
        {
            // Handles all exceptions including FTLException.
            e.printStackTrace();
        }
    } 
}
