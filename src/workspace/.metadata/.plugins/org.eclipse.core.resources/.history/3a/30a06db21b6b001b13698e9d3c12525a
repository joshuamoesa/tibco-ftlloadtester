package ftlreceiver;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class ArgParser {
	public String realmServer="http://localhost:8080";
	public String endPointName=null;
	public String applicationName=null;
	public String userID="";
	public String password="";
	public String fileName="";
	public String durableName;
	public String instanceName="instance1";
	
	
	
	FileWriter outputWriter=null;
	
	public void handleArgs(String[] args)
	{
		int i;
		
		System.out.println("The following command line arguments are used:");
		
		for (i=0;i<args.length;i++)
		{
			if (args[i].equals("-R"))
			{
				realmServer=getArgument(args, i+1);
				System.out.println("realmServer:"+realmServer);
				i++;
			}
			
			if (args[i].equals("-d"))
			{
				durableName=getArgument(args, i+1);
				System.out.println("durableName:"+durableName);
				i++;
			}
			
			if (args[i].equals("-e"))
			{
				endPointName=getArgument(args, i+1);
				System.out.println("endPoint:"+endPointName);
				i++;
			}
			
			if (args[i].equals("-a"))
			{
				applicationName=getArgument(args, i+1);
				System.out.println("applicationName:"+applicationName);
				i++;
			}
			
			if (args[i].equals("-u"))
			{
				userID=getArgument(args, i+1);
				System.out.println("userID:"+userID);
				i++;
			}
			if (args[i].equals("-p"))
			{
				password=getArgument(args, i+1);
				System.out.println("password:"+password);
				i++;
			}
			
			if (args[i].equals("-F"))
			{
				fileName=getArgument(args, i+1);
				System.out.println("Output file: "+fileName);
				i++;
			}
			if (args[i].equals("-i"))
			{
				instanceName=getArgument(args, i+1);
				System.out.println("instance name: "+instanceName);
				i++;
			}
		}
	}
	
	public String getArgument(String[] args, int index)
	{
		if (args.length<=index)
		{
			System.out.println("Missing argument "+index);
			System.exit(-1);
		}
		return args[index];
	}
	
	public void openOutputFile()
	{
		File outputFile;
		if (fileName.length()!=0)
		{
			outputFile = new File(fileName);
			try {
				outputFile.createNewFile();
				outputWriter = new FileWriter(outputFile); 
				outputWriter.append("instanceName, duration, hits\r\n");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
			
		}	
	}
	
	public void log(String logLine)
	{
		if (outputWriter==null)
		{
			return;
		}
		try {
			
			outputWriter.append(logLine+"\r\n");
			outputWriter.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void closeOutputFile()
	{
		if (outputWriter==null)
		{
			return;
		}
		
		try {
			outputWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}