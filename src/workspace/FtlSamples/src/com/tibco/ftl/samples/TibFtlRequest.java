/*
 * Copyright (c) 2010-2020 TIBCO Software Inc.
 * All Rights Reserved. Confidential & Proprietary.
 * For more information, please contact:
 * TIBCO Software Inc., Palo Alto, California, USA
 */

/*
 * This is a sample of a basic Java FTL request program. It sends a single request, then waits for
 * the reply.
 */

package com.tibco.ftl.samples;

import com.tibco.ftl.*;
import com.tibco.ftl.exceptions.*;

import java.util.*;

/*
 * This class implements NotificationHandler interface which is used to process notifications.In some 
 * situations, FTL must notify programs of conditions that preclude the use of event queues (as the 
 * communications medium). Instead of sending an advisory, FTL notifies the program through this 
 * out-of-band mechanism.
 */
public class TibFtlRequest implements NotificationHandler
{
    String          sampleName      = "tibftlrequest";
    String          realmService    = TibFtlAux.DEFAULT_URL;
    String          applicationName = "default";
    String          endpointName    = "default";
    double          timeout         = 30.0;
    boolean         finished        = false;
    String          clientName;

    public TibFtlRequest(String[] args)
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

    public int request() throws FTLException
    {
        Realm           realm     = null;
        Publisher       pub       = null;
        Message         msg       = null; 
        TibProperties   props     = null;
        Message         replyMsg  = null;

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

        /*
         * Before a message can be published, the publisher must be created on the previously-created realm. Pass the endpoint
         * onto which the messages will be published.

         * In the same manner as the application name above, the value of endpointName ("default") specifies the default endpoint,
         * provided automatically by the realm service.
         */
        pub = realm.createPublisher(endpointName);

        /*
         * In order to send a message, the message object must first be created via a call to createMessage() on realm. Pass the
         * the format name. Here the format "sample_request" is used. This is a dynamic format, which means it is not defined in the
         * realm configuration. Dynamic formats allow for greater flexibility in that changes to the format only require changes
         * to the client applications using it, rather than an administrative change to the realm configuration.
         */
        msg = realm.createMessage("sample_request");

        /*
         * A newly-created message is empty - it contains no fields. The next step is to add one or more fields to the message.
         * setString() method call on previously-created message adds a string field. First, a string field named "type" is added,
         * with the value "request".Then a long field named "timeInMills" with the help of setLong() method call on previously-created
         * message is added with the current time in milliseconds as value.
         */
        msg.setString("type", "request");
        msg.setLong("timeInMills", TibFtlAux.getCurrentTime());

        /*
         * Once the message is complete, it can be sent via sendRequest() method call on previously-created publisher. 
         * This call operates differently from send() method call in that it completes synchronously, not returning until 
         * the reply is received, and returning the reply message. Specify the request message to be sent, and a timeout 
         * value in seconds. If the reply is not received within the timeout period, an FTL exception will be thrown.
        
         * Note that no subscriber or event queue is required to receive a reply.
         */
        System.out.println("Sending request message:");
        System.out.println(" "  + msg.toString());
        
        try {
            replyMsg = pub.sendRequest(msg, timeout);
            if (!finished && (replyMsg != null)) 
            {
                System.out.println("Reply message received: " + replyMsg.toString());
                // A copy of the reply message is returned to the requester, so the requester is responsible for destroying
                // the returned reply message.
                replyMsg.destroy();
            }
        }
        catch (FTLTimeoutException timeoutEx) {
            System.out.println("No reply received in " + timeout + " seconds");
        }
        catch (Throwable ex) {
            ex.printStackTrace();
        }
        finally {
            // Once objects are no longer needed, they should be disposed of - and generally in reverse order of creation.
            // First destroy the message.
            msg.destroy();
            // Then close the publisher.
            pub.close();
            // Next close the connection to the realm service.
            realm.close();
        }   
        return 0;
    }
    
    public static void main(String[] args)
    {
        TibFtlRequest s  = new TibFtlRequest(args);
        try
        {
            s.request();
        }
        catch (Throwable e)
        {
            // Handles all exceptions including FTLException.           
            e.printStackTrace();
        }
    }
}
