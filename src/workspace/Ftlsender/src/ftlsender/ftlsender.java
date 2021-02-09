package ftlsender;

import java.util.ArrayList;

//import com.tibco.ftl.FTL;
import com.tibco.ftl.FTLException;
//import com.tibco.ftl.Message;
//import com.tibco.ftl.Publisher;
//import com.tibco.ftl.Realm;

public class ftlsender {
	
	
	public static void main(String[] args) throws FTLException 
	{
		Sender sender;
		ArgParser argParser = new ArgParser();
		int i;
		ArrayList<Thread> threadList = new ArrayList<Thread>();
		boolean finished, foundRunningThread;
		
		System.out.println("This application is used to do put load on an FTL environment.");
		System.out.println("The following switches can be used:");
		System.out.println("-i ==> instance name. This name is used to identify this instance in the log file. Default = instance1.");
		System.out.println("-R ==> realm host. By default http://localhost:8080 is used. Examle -R http//realm.org:8080");
		System.out.println("-e ==> end point name.");
		System.out.println("-a ==> application name.");
		System.out.println("-Smin ==> minimal message size (in bytes). Default = 100.");
		System.out.println("-Smax ==> maximum message size (in bytes). Default = 100.");
		System.out.println("-n ==> message name.  Default = testMessage.");
		System.out.println("-h ==> nr of hits per second per thread.  Default = 10.");
		System.out.println("-d ==> duration in minutes. Default = 1");
		System.out.println("-T ==> number of threads. Default = 1");
		System.out.println("-F ==> log file path and name. Default is not to use a file.");
		System.out.println("-u ==> user name. Default is empty.");
		System.out.println("-p ==> password. Default is empty.");
		
		argParser.handleArgs(args);
		argParser.openOutputFile();
		
		for (i=0;i<argParser.nrOfThreads;i++)
		{
			sender = new Sender();
			sender.setArgs(argParser);
			Thread t =new Thread(sender);
			threadList.add(t);
			t.start();
		}
		
		System.out.println("Started run with "+argParser.nrOfThreads+" thread(s)......");
		finished=false;
		while (finished==false)
		{
			foundRunningThread=false;
			for (i=0;i<argParser.nrOfThreads && foundRunningThread==false;i++)
			{
				if (threadList.get(i).isAlive())
				{
					foundRunningThread=true;
				}
			}
			if (foundRunningThread==false)
			{
				finished=true;
			}
		}
		System.out.println("Finished running");
	}

}
