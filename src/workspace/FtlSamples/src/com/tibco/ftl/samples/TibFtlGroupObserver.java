/*
 * Copyright (c) 2010-2020 TIBCO Software Inc.
 * All Rights Reserved. Confidential & Proprietary.
 * For more information, please contact:
 * TIBCO Software Inc., Palo Alto, California, USA
 */

/*
 * This is a sample of a basic FTL GROUP membership program which joins a group
 * as an observer and displays changes to the group membership status for a given
 * amount of time. After that, it leaves the group, cleans up and exits.
 */

package com.tibco.ftl.samples;

import java.util.*;
 
import com.tibco.ftl.*;
import com.tibco.ftl.group.*;
import com.tibco.ftl.exceptions.*;

/*
 * This class implements NotificationHandler interface which is used to process notifications.In some
 * situations, FTL must notify programs of conditions that preclude the use of event queues (as the
 * communications medium). Instead of sending an advisory, FTL notifies the program through this
 * out-of-band mechanism.
 */
public class TibFtlGroupObserver implements NotificationHandler
{
    String          sampleName      = "tibftlgroupobserver";
    String          realmService    = TibFtlAux.DEFAULT_URL;
    String          applicationName = "default";
    String          groupName       = "sample_group";
    double          duration        = 100.0;
    double          activation      = 5.0;
    boolean         finished        = false;
    TibProperties   realmProps  = null;
    TibProperties   groupProps  = null;
    Realm           realm  = null;
    String          groupStatusAdvisoryMatcherString;
    ContentMatcher  groupStatusAdvisoryMatcher     = null;
    Subscriber      groupStatusAdvisorySubscriber    = null;
    EventQueue      queue  = null;
    Group           group  = null;
    long            start;
    String          clientName;

    public TibFtlGroupObserver(String[] args)
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

    public int work() throws FTLException
    {
        /*
         * A properties object (TibProperties) allows the client application to set attributes (or properties) for an object
         * when the object is created. In this case, set the client label property (PROPERTY_STRING_CLIENT_LABEL) for the
         * realm connection.
         */
        realmProps = FTL.createProperties();
        realmProps.set(Realm.PROPERTY_STRING_CLIENT_LABEL, clientName);
       
        /*
         * Establish a connection to the realm service. Specify a string containing the realm service URL, and
         * the application name. The last argument is the properties object, which was created above.

         * Note the application name (the value contained in applicationName, which is "default"). This is the default application,
         * provided automatically by the realm service. It could also be specified as null - it mean the same thing,
         * the default application.
         */ 
        realm = FTL.connectToRealmServer(realmService, applicationName, realmProps);

        /*
         * It is good practice to clean up objects when they are no longer needed. Since the realm properties object is no
         * longer needed, destroy it now.
         */
        realmProps.destroy();

        /*
         * Register a notification handler. This handler will be invoked when an administrative action occurs which
         * requires the client application to be notified.
         */
        realm.setNotificationHandler(this);

        /* For messages to be delivered to a client application, two things are required:
         *   1) A subscriber object must be created on a particular endpoint
         *   2) The created subscriber object must be added to an event queue to dispatch the received messages
         * Before the subscriber object can be added to an event queue, the event queue must exist. So that is the first
         * step. createEventQueue() method call on previously-created realm creates an event queue.
         */
        queue = realm.createEventQueue();

        /*
         * Changes in the status of a group are reported to the client application through an advisory message.
         * These advisory messages have the field "name" set to the specific value "ADVISORY_NAME_GROUP_STATUS".
         * So create a content matcher to cause only those messages to be delivered.

         * A content matcher is used to limit which messages are delivered to a client application based on the content
         * of the message. The string passed to createContentMatcher() contains JSON which specifies the field name
         * in the message, and the required value of that field.

         * In this case, the matcher string created is:
         *     {"name":"ADVISORY_NAME_GROUP_STATUS"}
         * which matches any message which contains a field named "name" with the specific value "ADVISORY_NAME_GROUP_STATUS".

         * When creating the matcher string, it is good practice to use the predefined constants FIELD_NAME (defined in Advisory
         * Interface, which is automatically included in com.tibco.ftl.*) and ADVISORY_NAME_GROUP_STATUS (defined in Group
         * Interface) rather than the literal values.
         */
        groupStatusAdvisoryMatcher = realm.createContentMatcher("{\"" + Advisory.FIELD_NAME + "\":\"" + Group.ADVISORY_NAME_GROUP_STATUS + "\"}");

        /*
         * Next, create the group status change advisory subscriber object. Specify the endpoint name, the content matcher, and a properties object.

         * The endpoint specified is a predefined endpoint on which advisory messages are received.
         */
        groupStatusAdvisorySubscriber = realm.createSubscriber(Advisory.ENDPOINT_NAME, groupStatusAdvisoryMatcher, null);

        /*
         * Create instance of groupStatusAdvisoryHandler which will provide the callback for handling group status change advisory specifically.
         */
        groupStatusAdvisoryHandler onGroupStatusAdvisory = new groupStatusAdvisoryHandler();
 
        /*
         * Next add the group status change advisory subscriber object to the event queue. This allows the event queue to
         * dispatch messages sent to that subscriber. Specify the subscriber object, callback.

         * The callback must be a function with a specific signature: see the messagesReceived() method of groupStatusAdvisoryHandler below
         * for an explanation of what it does.
         */
        queue.addSubscriber(groupStatusAdvisorySubscriber, onGroupStatusAdvisory);

        /*
         * To join a group, two things are required: a group name (shared by all client applications participating in the
         * group), and a set of properties. Group management is handled by a group server within the realm service.

         * Two properties are used here. The first is the activation interval (PROPERTY_DOUBLE_ACTIVATION_INTERVAL).
         * This value, which defaults to 5 seconds, drives the values used by the group server for heartbeat and timeout
         * intervals between the group server and group members. This allows the group server to determine if a group member
         * has unexpectedly terminated, possibly requiring a reassignment of group ordinals to the remaining group members.

         * The second property is a boolean which indicates that this client application is joining the group as an observer.
         */
        groupProps = FTL.createProperties();
        // Set the activation interval property.
        groupProps.set(Group.PROPERTY_DOUBLE_ACTIVATION_INTERVAL, activation);
        // Set the property indicating this client application is joining as an observer.        
        groupProps.set(Group.PROPERTY_BOOLEAN_OBSERVER, true);

        // Finally, join the group and create the group object using the FTL API function join() of GroupFactory.
        group = GroupFactory.join(realm, groupName, groupProps);

        // Since the group properties are no longer needed, destroy them.
        groupProps.destroy();

        // getName() allows a client application to get the name of the group to which it has joined.
        System.out.println("Joined group: " +  group.getName());

        /*
         * For an event queue to dispatch messages, it must be run on a thread. That thread may be separately created and
         * managed, or it may be the main program thread - as it is in this sample.

         * dispatch() method call on previously-created event queue is the FTL API function that actually dispatches messages.
         * It should wait for messages to be available for dispatch before returning. Specifying duration (defined at the
         * beginning of this sample) will wait no longer than duration seconds before returning.

         * Since messages arrive asynchronously, the dispatch() method call is contained in a loop.Each trip through
         * the loop dispatches the event queue, then computes the time remaining to run the loop. The loop continues so long as
         * 1) the remaining duration is greater than zero. and
         * 2) when the finished flag is true, indicating the client has been disabled.
         */
        // Begin dispatching messages
        System.out.println("Waiting for " + duration + " seconds");
        start = new Date().getTime()/1000;
        while (duration > 0 && !finished)
        {
            queue.dispatch(duration);
            duration -= (new Date().getTime()/1000) - start;
            start = new Date().getTime()/1000;
        }
        System.out.println("Done waiting");

        // Processing is finished, so leave the group.
        group.leave();

        // Once objects are no longer needed, they should be disposed of - and generally in reverse order of creation.

        // Start with the group status advisory subscriber and associated objects.
        if ((queue != null) && (groupStatusAdvisorySubscriber != null))
        {
            queue.removeSubscriber(groupStatusAdvisorySubscriber);
            groupStatusAdvisorySubscriber.close();
            if (groupStatusAdvisoryMatcher != null)
            {
                groupStatusAdvisoryMatcher.destroy();
            }
        }

        // Next destroy the event queue.
        queue.destroy();

        // Then close the connection to the realm service.
        realm.close();

        return 0;
    }
    
    public static void main(String[] args)
    {
        TibFtlGroupObserver  s  = new TibFtlGroupObserver(args);

        try {
            s.work();
        }
        catch (Throwable e) {
            // Handles all exceptions including FTLException.
            e.printStackTrace();
        }
    } 
}

/*
 * Message delivery callback functions are invoked when the event queue dispatches messages destined for
 * a subscriber. Callbacks are associated with a subscriber via addSubscriber(). The callback must be a
 * function that returns void, and takes two arguments:
 *   - an array of messages (Message) being delivered to the callback in this invocation
 *   - an event queue (EventQueue), the event queue which dispatched the messages

 * More than one message may be delivered in a single invocation. The number of messages
 * is contained in the msgNum argument. Each message is an element in the messages argument.
 * The callback must be able to handle the delivery of multiple messages.

 * The layout of a group advisory message is as follows:

 * Four fields are common to every advisory message:
 *   - FIELD_ADVISORY - a long field, always contains the value 1
 *   - FIELD_SEVERITY - a string field containing one of the values SEVERITY_WARN, SEVERITY_INFO, or SEVERITY_DEBUG
 *   - FIELD_MODULE   - a string field. For group advisories it contains ADVISORY_MODULE_GROUP
 *   - FIELD_NAME     - a string field. For group advisories it contains one of the values
 *                      ADVISORY_NAME_ORDINAL_UPDATE or ADVISORY_NAME_GROUP_STATUS

 * Group advisory messages also include this field:
 *   - ADVISORY_FIELD_GROUP - a string field containing the group name to which the advisory pertains

 * Ordinal update group advisory messages contain an additional field:
 *   - ADVISORY_FIELD_ORDINAL - a long field containing the new ordinal for the client application

 * Group status advisory messages contain two additional fields:
 *   - FIELD_GROUP_SERVER_AVAILABLE   - a long field containing 1 if the group server is currently available,
 *                                      and zero otherwise
 *   - FIELD_GROUP_MEMBER_STATUS_LIST - a message array field. Each element is a message containing two fields:
 *       - FIELD_GROUP_MEMBER_DESCRIPTOR - a message field containing the client descriptor message set when the
 *                                         client joined the group.
 *       - FIELD_GROUP_MEMBER_EVENT      - a long field indicating the member event. It contains one of the value
 *           GROUP_MEMBER_JOINED, GROUP_MEMBER_LEFT, or GROUP_MEMBER_DISCONNECTED
 */


/*
 * This class implements SubscriberListener interface which is a Message event handler to process inbound messages.
 */
class groupStatusAdvisoryHandler implements SubscriberListener
{
    // This callback is invoked when one or more group ordinal change advisory messages are dispatched.
    public void messagesReceived(List<Message> messages, EventQueue eventQueue)
    {
        int i;
        Message msg;
        String svalue;

        try {
            int msgNum = messages.size();
            // Since a callback invocation may deliver more than one message, loop through all messages delivered.      
            for (i = 0;  i < msgNum;  i++)
            {
                svalue = "";
		msg = messages.get(i);

                System.out.println("Group status advisory:");

                // Use the FTL API function getString() of Message to fetch and print the standard advisory message
                // fields FIELD_SEVERITY, FIELD_MODULE, and FIELD_NAME.
                svalue = msg.getString(Advisory.FIELD_SEVERITY);
                System.out.println("  " + Advisory.FIELD_SEVERITY + ": " + svalue);

                svalue = msg.getString(Advisory.FIELD_MODULE);
                System.out.println("  " + Advisory.FIELD_MODULE + ": " + svalue);

                svalue = msg.getString(Advisory.FIELD_NAME);
                System.out.println("  " + Advisory.FIELD_NAME + ": " + svalue);

                // Since a matcher was used when creating the group status advisory subscriber, only messages in
                // which the FIELD_NAME field contains the value ADVISORY_NAME_GROUP_STATUS are delivered to this
                // callback. This helps to simplify the callback code.
                svalue = msg.getString(Group.ADVISORY_FIELD_GROUP);
                System.out.println("  " + Group.ADVISORY_FIELD_GROUP + ": " + svalue);

                Message   memberDesc = null;

                // If the server connection is not valid, nothing else can be assumed about the contents
                // of the message.
                if (serverConnectionValid(msg)){
                    // Get the member status list (actually an array of messages).
                    Message[] membersStatusList = getMembersStatusList(msg);
                    if (membersStatusList == null || membersStatusList.length == 0)
                    {
                        System.out.println("No members have joined the group yet");
                    } 

                    // Loop through each of the member status messages in the array if not empty.                    
                    for(int members = 0; members < membersStatusList.length; members++)
                    {
                        // Fetch and print the group member's status.
                        Group.GroupMemberEvent statusEvent = getMemberStatusEvent(membersStatusList[members]);
                        System.out.println("Members status: " + valueOf(statusEvent));

                        // Fetch and print the group member's descriptor message.
                        memberDesc = getMembersDescriptor(membersStatusList[members]);
                        if (memberDesc != null)
                        {
                            System.out.println(memberDesc.toString());
                        }
                        else
                        {
                            System.out.println("This member is anonymous");
                        }
                    }                                     
                }
                else
                {   
                    System.out.println("Connection to group server LOST, member information is not available");
                }                
            }
        }
        catch (Throwable ex)
        {
            ex.printStackTrace();
        }
    }

    /*
     * This call returns the descriptor message for the member that generated the event
     * The descriptor is null if the member did not provided one at the time of joining
     */
    private Message getMembersDescriptor(Message statusMsg) throws FTLException
    {
        Message result = null;
        if (statusMsg.isFieldSet(Group.FIELD_GROUP_MEMBER_DESCRIPTOR))
        {
            result = statusMsg.getMessage(Group.FIELD_GROUP_MEMBER_DESCRIPTOR);
        }
        return result;
    }

    /*
     * Group Member Status is passed from group server to members as a long field
     * of the group members status message. This function does the conversion
     * from long to GroupMemberStatus
     */
    private Group.GroupMemberEvent valueOf(long memberStatus) throws FTLException
    {
        Group.GroupMemberEvent result = null;
        switch((int)memberStatus)
        {
            case 0:
                result = Group.GroupMemberEvent.JOINED;
                break;
            case 1:
                result = Group.GroupMemberEvent.LEFT;
                break;
            case 2:
                result = Group.GroupMemberEvent.DISCONNECTED;
                break;
            default:
                break;
        }
        if (result == null)
        {
            throw new FTLInvalidValueException("Invalid member status value " + memberStatus);
        }
        return result;
    }

    /*
     * This call returns the GroupMemberStatus  name string
     */
    private String valueOf(Group.GroupMemberEvent memberStatus) throws FTLException
    {
        String result = null;
        switch(memberStatus)
        {
            case JOINED:
                result = "GROUP_MEMBER_JOINED";
                break;
            case LEFT:
                result = "GROUP_MEMBER_LEFT";
                break;
            case DISCONNECTED:
                result = "GROUP_MEMBER_DISCONNECTED";
                break;
        }
        return result;
    }

    /*
     * This call returns the GroupMemberStatus
     */
    private Group.GroupMemberEvent getMemberStatusEvent(Message statusMsg) throws FTLException
    {
        Group.GroupMemberEvent result = null;
        if (statusMsg.isFieldSet(Group.FIELD_GROUP_MEMBER_EVENT))
        {
           result = valueOf(statusMsg.getLong(Group.FIELD_GROUP_MEMBER_EVENT));
        }
        else
        {
            throw new FTLInvalidFormatException("This method requires a group status message!");
        }
        return result;
    }

   /*
    * This call returns list of status messages from a status advisory message
    */
    private Message[] getMembersStatusList(Message statusAdvMsg) throws FTLException
    {
        Message[] result = null;
        if (statusAdvMsg.isFieldSet(Group.FIELD_GROUP_MEMBER_STATUS_LIST))
        {
            result = statusAdvMsg.getMessageArray(Group.FIELD_GROUP_MEMBER_STATUS_LIST);
        }
        else
        {
            throw new FTLInvalidFormatException("This method requires a group status advisory message!");
        }
        return result;
    }

    /*
     * The Group Member Server connection status is passed from group library as a long field in the
     * group status advisory message. This function does the conversion from long to Group.GroupMemberServerConnection.
     */
    private Group.GroupMemberServerConnection serverConnectionValueOf(long serverConnStatus) throws FTLException
    {
        Group.GroupMemberServerConnection result = null;
        switch((int)serverConnStatus)
        {
            case 0:
                result = Group.GroupMemberServerConnection.UNAVAILABLE;
                break;
            case 1:
                result = Group.GroupMemberServerConnection.AVAILABLE;
                break;
            default:
                break;
        }
        if (result == null)
        {
            throw new FTLInvalidValueException("Invalid server connection status value " + serverConnStatus);
        }

        return result;
    }

    /*
     * This is a convenience function which checks the group status advisory message to determine if
     * the group server is available. Return true if it is, false otherwise.
     */
    private boolean serverConnectionValid(Message statusMsg) throws FTLException
    {
        Group.GroupMemberServerConnection value = null;

        value = serverConnectionValueOf(statusMsg.getLong(Group.FIELD_GROUP_SERVER_AVAILABLE));

        if (value == Group.GroupMemberServerConnection.AVAILABLE)
        {
            return true;
        }

        return false;
    }
} 


