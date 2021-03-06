package ftlsender;

import java.util.Random;

import com.tibco.ftl.FTL;
import com.tibco.ftl.FTLException;
import com.tibco.ftl.Message;
import com.tibco.ftl.Publisher;
import com.tibco.ftl.Realm;
import com.tibco.ftl.TibProperties;

public class Sender implements Runnable {
	
	ArgParser argParser;
	
	Random rand = new Random();
	String choices="X";
	long totalNrOfBytes=0;
	
	Publisher       pub         = null;
    Message         msg         = null;
    Realm           realm       = null;
   
    @Override
	public void run() 
    {
		
		try {
			handle();
		} catch (FTLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
    
    
    public void setArgs(ArgParser inputArgParser)
    {
    	argParser=inputArgParser;
    }
    
    
    public void handle() throws FTLException
    {
    	long startTime;
    	long curDuration;
    	long lastHit;
    	long timeStamp;
    	long lastPrint;
    	long hitCounter=0;
    	
    	
		initConnection();
		startTime=System.currentTimeMillis(); 
		curDuration=0;
		lastHit=startTime;
		lastPrint=startTime;
		while ((curDuration/1000)<(argParser.duration*60))
		{
			timeStamp=System.currentTimeMillis();
			curDuration=timeStamp-startTime;
			if ((timeStamp-lastHit)>(1000/argParser.nrOfHitsPerSec))
			{
				lastHit=timeStamp;
				send();
				hitCounter++;
			}
			if ((timeStamp-lastPrint)>(10000))
			{
				lastPrint=timeStamp;
				System.out.println("Run time for instance "+argParser.instanceName+" thread "+Thread.currentThread().getId()+" is "+(curDuration/1000)+"s, hits: "+hitCounter);
				argParser.log(argParser.instanceName+","+Thread.currentThread().getId()+","+(curDuration/1000)+","+hitCounter);
			}
		}
		long durInSec=curDuration/1000;
		
		argParser.log(argParser.instanceName+","+Thread.currentThread().getId()+","+(curDuration/1000)+","+hitCounter);
		System.out.println("Final run time for thread: "+Thread.currentThread().getId()+" = "+(curDuration/1000)+"s, hits: "+hitCounter+"=("+(hitCounter/durInSec)+" per sec), average number of bytes per message:"+totalNrOfBytes/hitCounter);
		closeConnection();
    }
    
    
    public int getNrOfHitsPerSec()
    {
    	return argParser.nrOfHitsPerSec;
    }
    
	
	public void initConnection()
	{
		// connect to the realmserver.
		
		TibProperties realmProps;
		
        try {
        	realmProps = FTL.createProperties();
			realmProps.set(Realm.PROPERTY_STRING_USERNAME, argParser.userID);
			realmProps.set(Realm.PROPERTY_STRING_USERPASSWORD, argParser.password);	
        	
			realm = FTL.connectToRealmServer(argParser.realmServer, argParser.applicationName, null);
			// Create sender object and message to be sent
			
			pub = realm.createPublisher(argParser.endPointName);
		} catch (FTLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void closeConnection()
	{
        try {
			pub.close();
			realm.close();
		} catch (FTLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void send() throws FTLException
    {
		//String msgString;
		int i,index;
		int nrOfChars;
		
		if (argParser.maxSize==argParser.minSize)
		{
			nrOfChars=argParser.maxSize;
		} else {
			nrOfChars=rand.nextInt(argParser.maxSize-argParser.minSize+1)+argParser.minSize;	
		}
		totalNrOfBytes=totalNrOfBytes+nrOfChars;
		
		StringBuilder sb = new StringBuilder(nrOfChars); 
		
		
		for (i = 0; i < nrOfChars; i++) {  
            index = (int)(choices.length() * Math.random()); 
            sb.append(choices.charAt(index)); 
        } 
		
		
        msg = realm.createMessage(null);
        msg.setString("type", argParser.messageName);
        msg.setString("message", sb.toString());

        // System.out.printf("sending "+nrOfChars+" chars for message type "+messageName);
         
        pub.send(msg);

        // Clean up
        msg.destroy();
    }
}
