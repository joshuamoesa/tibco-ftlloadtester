/*
 * Copyright (c) 2010-2020 TIBCO Software Inc.
 * All Rights Reserved. Confidential & Proprietary.
 * For more information, please contact:
 * TIBCO Software Inc., Palo Alto, California, USA
 */

/*
 * This is a sample of a basic Java FTL Map program which removes an existing map.
 */

package com.tibco.ftl.samples;

import java.util.*;
import com.tibco.ftl.*;
import com.tibco.ftl.exceptions.*;

public class TibFtlMapRemove
{
    String          sampleName      = "tibftlmapremove";
    String          realmService    = TibFtlAux.DEFAULT_URL;
    String          applicationName = "default";
    String          endpointName    = "map";
    String          mapName         = "sample_map"; 
    Realm           realm           = null;
    String          clientName;

    public TibFtlMapRemove(String[] args)
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
       
         * Removing an existing map is done via the FTL API function removeMap() of Realm. Specify the endpoint name, map name, 
         * and a properties object (which must be null).
         */
        realm.removeMap(endpointName, mapName, null);

        System.out.println("Map " + mapName + " removed");
      
        // Once objects are no longer needed, they should be disposed of - and generally in reverse order of creation.
        // Close the connection to the realm service. 
        realm.close();

        return 0;
    }
    
    public static void main(String[] args)
    {
        TibFtlMapRemove s  = new TibFtlMapRemove(args);
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
