package ftlreceiver;

import java.io.IOException;
import java.io.InputStreamReader;

public class FTLReceiver {

	public static void main(String[] args) {

		boolean exit=false;
		ArgParser argParser = new ArgParser();
		
		long startTime;
    	long curDuration;
    	long timeStamp;
    	long lastPrint;
    	
		
    	System.out.println("This application is used to do receive load on an FTL environment.");
		System.out.println("The following switches can be used:");
		System.out.println("-i ==> instance name. This name is used to identify this instance in the log file. Default = instance1.");
		System.out.println("-R ==> realm host. By default http://localhost:8080 is used. Examle -R http//realm.org:8080");
		System.out.println("-e ==> end point name.");
		System.out.println("-a ==> application name.");
		System.out.println("-d ==> durable name.");
		System.out.println("-F ==> log file path and name. Default is not to use a file.");
		System.out.println("-u ==> user name. Default is empty.");
		System.out.println("-p ==> password. Default is empty.");
		
		argParser.handleArgs(args);
		argParser.openOutputFile();
		 // inputRealmServer,       inputAppName, inputUser,  inputPassword,  inputDurableName, inputEndPoint
		Receiver myFTLMessageReceiver = new Receiver(argParser.realmServer, argParser.applicationName, argParser.userID, argParser.password, argParser.endPointName, argParser.endPointName);
		
		
		
		InputStreamReader reader = new InputStreamReader(System.in); 

		
		
		
		System.out.println("FTLReceiver started....");
		System.out.println("Press enter to stop.");
		
		startTime=System.currentTimeMillis(); 
		curDuration=0;
		lastPrint=startTime;
	
		while(!exit) 
		{ 
			try {
				if ( reader.ready()) 
				{ 
					System.out.println("exit");
					exit=true; 
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			timeStamp=System.currentTimeMillis();
			curDuration=timeStamp-startTime;
			if ((timeStamp-lastPrint)>10000)
			{
				lastPrint=timeStamp;
				System.out.println("Run time for instance "+argParser.instanceName+" is "+(curDuration/1000)+"s, hits: "+myFTLMessageReceiver.totalNumberOfMessages);
				argParser.log(argParser.instanceName+","+(curDuration/1000)+","+myFTLMessageReceiver.totalNumberOfMessages);
			}
			
			
			myFTLMessageReceiver.dispatch();
		}
		
		System.out.println("Final run time for instance "+argParser.instanceName+" is "+(curDuration/1000)+"s, hits: "+myFTLMessageReceiver.totalNumberOfMessages);
		argParser.log(argParser.instanceName+","+(curDuration/1000)+","+myFTLMessageReceiver.totalNumberOfMessages);
		myFTLMessageReceiver.close();
		argParser.closeOutputFile();
	}
}