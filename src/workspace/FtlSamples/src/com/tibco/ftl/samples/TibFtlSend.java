// TODO --> change the date and look at copyright info
/*
 * Copyright (c) 2010-2020 TIBCO Software Inc.
 * All Rights Reserved. Confidential & Proprietary.
 * For more information, please contact:
 * TIBCO Software Inc., Palo Alto, California, USA
 */

/*
 * This is a sample of a basic Hello world Java FTL publisher program which sends a
 * hello world message.
 */

package com.tibco.ftl.samples;

import com.tibco.ftl.*;
import com.tibco.ftl.exceptions.*;

public class TibFtlSend 
{
    String          sampleName      = "tibftlsend";
    String          realmService    = TibFtlAux.DEFAULT_URL;
    String          applicationName = "default";
    String          endpointName    = "default";
    Publisher       pub             = null;
    Message         msg             = null;
    Realm           realm           = null;
    String          clientName;

    public TibFtlSend(String[] args)
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

    public int send() throws FTLException
    {
        /*
         * A properties object (TibProperties) allows the client application to set attributes (or properties) for an object
         * when the object is created. In this case, set the client label property (PROPERTY_STRING_CLIENT_LABEL) for the 
         * realm connection. 
         */
        TibProperties   props = null;
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
         * Before a message can be published, the publisher must be created on the previously-created realm. Pass the endpoint 
         * onto which the messages will be published.

         * In the same manner as the application name above, the value of endpointName ("default") specifies the default endpoint,
         * provided automatically by the realm service.
         */
        pub = realm.createPublisher(endpointName);   

        /* 
         * In order to send a message, the message object must first be created via a call to createMessage() on realm. Pass the
         * the format name. Here the format "helloworld" is used. This is a dynamic format, which means it is not defined in the 
         * realm configuration. Dynamic formats allow for greater flexibility in that changes to the format only require changes 
         * to the client applications using it, rather than an administrative change to the realm configuration.
         */
        msg = realm.createMessage("helloworld");

        /*
         * A newly-created message is empty - it contains no fields. The next step is to add one or more fields to the message.
         * setString() method call on previously-created message adds a string field. First, a string field named "type" is added,
         * with the value "hello".Then a string field named "message" is added with the value "hello world earth".
         */
        msg.setString("type", "hello");
        msg.setString("message", "hello world earth");

        /* 
         * Once the message is complete, it can be sent via send() method call on previously-created publisher. Pass the message 
         * to be sent.
         */
        System.out.printf("Sending 'hello world' message\n");
        pub.send(msg);

        // Once objects are no longer needed, they should be disposed of - and generally in reverse order of creation.
       
        // First destroy the message.
        msg.destroy();
       
        // Then Close the Publisher
        pub.close();

        // Next close the connection to the realm service.
        realm.close();

        return 0;
    }
    
    public static void main(String[] args)
    {
        TibFtlSend s  = new TibFtlSend(args);
        try {
            s.send();
        }
        catch (Throwable e) {
            // Handles all exceptions including FTLException.
            e.printStackTrace();
        }
    }
}
