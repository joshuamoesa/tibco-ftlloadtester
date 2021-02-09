/*
 * Copyright (c) 2010-2020 TIBCO Software Inc.
 * All Rights Reserved. Confidential & Proprietary.
 * For more information, please contact:
 * TIBCO Software Inc., Palo Alto, California, USA
 */

/*
 * This is a sample of a basic Java FTL Map program which creates a map and sets several key values.
 */

package com.tibco.ftl.samples;

import java.util.*;
import com.tibco.ftl.*;
import com.tibco.ftl.exceptions.*;

public class TibFtlMapSet
{
    String          sampleName      = "tibftlmapset";
    String          realmService    = TibFtlAux.DEFAULT_URL;
    String          applicationName = "default";
    String          endpointName    = "map";
    String          mapName         = "sample_map";
    String          lockName        = "sample-map-lock";
    String[]        keyList         = {"key1","key2","key3"}; 
    TibMap          map             = null;
    Message         msg             = null;
    Realm           realm           = null;
    String          clientName;
    TibLock         lock            = null;

    public TibFtlMapSet(String[] args)
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
    
    public int work() throws FTLException
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
         * The map resides in the persistence store associated with the endpoint "map". Maps store key-value pairs: the
         * key is a string, and the value is an FTL message.
 
         * The endpoint "map" is the default endpoint for maps created using the default application.
     
         * To create the map, use the FTL API function createMap() of realm. Specify the endpoint, the map name, and an optional 
         * properties object.
         */
        map = realm.createMap(endpointName, mapName, null);

        /*
         * Create the lock. This allows coordinated access to the map between client applications.
         */
        lock = realm.createLock(lockName, null);

        /*
         * In order to send a message, the message object must first be created via a call to createMessage() on realm. Pass the
         * the format name. Here the format "mapsample" is used. This is a dynamic format, which means it is not defined in the
         * realm configuration. Dynamic formats allow for greater flexibility in that changes to the format only require changes
         * to the client applications using it, rather than an administrative change to the realm configuration.
         */
        msg = realm.createMessage("mapsample");

        /*
         * The variable keyList is an array of strings. Each element is a key to be added to the map. Iterate through the array of
         * keys, adding a key-value pair for each.             
         */
        int index;   
        for (index = 0; index < keyList.length; index++) {
            System.out.println("Setting value for key " + keyList[index]);
            // Add a long field to the message containing the key's index.
            msg.setLong("index", index);
            // Add a key-value pair to the map. The key is keyList[index], and the value is the message. Specifying the lock
            // allows coordinated access between client applications.
            map.set(keyList[index], msg, lock);
        }
      
        // Once objects are no longer needed, they should be disposed of - and generally in reverse order of creation. 
        
        // First destroy the message.
        msg.destroy();
        // Next destroy the lock.
        lock.destroy();
        // Next, close the map. Note that the map remains in the persistent store, available for use by other client applications.
        map.close();
        // Next close the connection to the realm service.
        realm.close();

        return 0;
    }
    
    public static void main(String[] args)
    {
        TibFtlMapSet s  = new TibFtlMapSet(args);
        try
        {
            s.work();
        }
        catch (Throwable e)
        {
            // Handles all exceptions including FTLException.
            e.printStackTrace();
        }
    }
}
