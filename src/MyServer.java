import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Statement;


public class MyServer {

    public static ArrayList<Socket> socketList = new ArrayList<Socket>();  
    public static HashMap<String , Socket> userList=new HashMap<String, Socket>();
    
    public static void main(String[] args) throws IOException, SQLException  
    {  
    	System.out.println("Server is running!");
    	
		
		
    	//port name
        ServerSocket ss = new ServerSocket(6012);  
        while(true)  
        {  
            //waiting for connecting
            Socket s = ss.accept();  
            System.out.println("New Device Connected!");
            socketList.add(s);   
            new Thread(new SocketThread(s)).start();  
        }  
    }	
}
