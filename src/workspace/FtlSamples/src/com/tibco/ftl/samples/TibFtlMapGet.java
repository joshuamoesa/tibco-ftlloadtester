/*
 * Copyright (c) 2010-2020 TIBCO Software Inc.
 * All Rights Reserved. Confidential & Proprietary.
 * For more information, please contact:
 * TIBCO Software Inc., Palo Alto, California, USA
 */

/*
 * This is a sample of a basic Java FTL Map program which creates/opens a map and gets all key values.
 */

package com.tibco.ftl.samples;

import java.util.*;
import com.tibco.ftl.*;
import com.tibco.ftl.exceptions.*;

public class TibFtlMapGet
{
    String          sampleName      = "tibftlmapget";
    String          realmService    = TibFtlAux.DEFAULT_URL;
    String          applicationName = "default";
    String          endpointName    = "map";
    String          mapName         = "sample_map";
    String          lockName        = "sample-map-lock";
    TibMap          map             = null;
    Realm           realm           = null;
    String          clientName;
    TibLock         lock            = null;

    public TibFtlMapGet(String[] args)
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

        Message msg = null;
        String  key = null;
        TibMapIterator iter = null;

        try 
        {
            /*
             * Iterate through the keys in the map. Create an iterator via the FTL API function tibMap_CreateIterator() of TibMap.
             * Specify the lock, and an optional properties object (which is not used in this case).
             */
            iter = map.createIterator(lock, null);

            /*
             * Once the iterator has been created, a call to next() method of TibMapIterator positions the iterator to the next 
             * (or first) key-value pair in the map. It returns true if the next key-value pair can be fetched, and false if no 
             * more key-value pairs are available.
             */           
            while(iter.next())
            {
                /*
                 * The key of the current key-value pair is fetched via the FTL API call currentKey() of TibMapIterator . 
                 * It returns the pair's key value as a string.
                 */
                key = iter.currentKey();

                /* 
                 * The value of the current key-value pair is fetched via the FTL API call currentValue() of TibMapIterator . 
                 * It returns the pair's value as a FTL message.
                 */                
                msg = iter.currentValue();

                // Print the key and value for this pair. 
                System.out.println("Key: " + key + ", Value = " + msg.toString());
            }
         }
         catch (FTLException  e)
         {
            e.printStackTrace(); 
         }

        // Once objects are no longer needed, they should be disposed of - and generally in reverse order of creation.

        // First destroy the iterator.
        if (iter != null){
            iter.destroy();
        }
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
        TibFtlMapGet s  = new TibFtlMapGet(args);
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
