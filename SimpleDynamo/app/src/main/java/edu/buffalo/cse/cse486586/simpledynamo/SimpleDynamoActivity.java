package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.app.Activity;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.widget.TextView;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class SimpleDynamoActivity extends Activity {

	static final List<String> portList = new ArrayList<String>(Arrays.asList("11108","11112","11116","11120","11124"));
	static final int SERVER_PORT = 10000;
	static List<Node> connectedNodes = new ArrayList<Node>();
	static Node myNode = null;
	final String authority = "edu.buffalo.cse.cse486586.simpledht.provider";
	final String scheme = "content";
	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dynamo);
    
		TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

		for(String port : portList) {

			try {
				String nodeId = String.valueOf(Integer.parseInt(port)/2);
				String nodeKey = new SimpleDynamoProvider().genHash(nodeId);
				Node node = new Node(nodeId,nodeKey,port,null,null, null, null);
				connectedNodes.add(node);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}
		Collections.sort(connectedNodes);

		for(int i=0;i<connectedNodes.size();i++){
			if(i==connectedNodes.size()-1){
				connectedNodes.get(i).setSuccessor1(connectedNodes.get(0));
				connectedNodes.get(i).setSuccessor2(connectedNodes.get(1));
				connectedNodes.get(0).setPredecessor1(connectedNodes.get(i));
				connectedNodes.get(1).setPredecessor2(connectedNodes.get(i));
			} else if(i==connectedNodes.size()-2) {
				connectedNodes.get(i).setSuccessor1(connectedNodes.get(i+1));
				connectedNodes.get(i).setSuccessor2(connectedNodes.get(0));
				connectedNodes.get(i+1).setPredecessor1(connectedNodes.get(i));
				connectedNodes.get(0).setPredecessor2(connectedNodes.get(i));
			} else{
				connectedNodes.get(i).setSuccessor1(connectedNodes.get(i+1));
				connectedNodes.get(i).setSuccessor2(connectedNodes.get(i+2));
				connectedNodes.get(i+1).setPredecessor1(connectedNodes.get(i));
				connectedNodes.get(i+2).setPredecessor2(connectedNodes.get(i));
			}
		}

		TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

		for (Node node : connectedNodes) {
			if (node.getNodePort().equals(myPort)) {
				myNode = node;
			}
		}

		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			//serverSocket.setReuseAddress(true);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			return;
		}

		new RecoveryTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);

	}

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	public void getRecovery() {
		synchronized (this){
			SimpleDynamoProvider.Messages.clear();
		}
		Cursor queryResult = new SimpleDynamoProvider().query(buildUri(scheme,authority),null,"*",null,null);
		if(queryResult.getCount()!=0) {
			queryResult.moveToFirst();
			do {
				String key = queryResult.getString(0);
				String value = queryResult.getString(1);
				String location = new SimpleDynamoProvider().getLocationOfMsg(key);
				for(String node : location.split(":")){
					if(myNode.getNodePort().equalsIgnoreCase(node)){
						SimpleDynamoProvider.Messages.put(key,value);
					}
				}
			}
			while (queryResult.moveToNext());
		}
	}

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.simple_dynamo, menu);
		return true;
	}
	
	public void onStop() {
        super.onStop();
	    Log.v("Test", "onStop()");
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {


		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];

			while(!serverSocket.isClosed()) {
				try {
					Socket socket = serverSocket.accept();
					//socket.setReuseAddress(true);
					InputStream inputStream = socket.getInputStream();
					InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
					BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
					String msg = bufferedReader.readLine();
					String msgType = msg.split(":")[0];
					if(msgType.equalsIgnoreCase("insert")) {
						String key =  msg.split(":")[1];
						String value = msg.split(":")[2];
						synchronized (this){
							SimpleDynamoProvider.Messages.put(key,value);
						}
						OutputStream outputStream = socket.getOutputStream();
						PrintWriter outputStreamWriter = new PrintWriter(outputStream);
						outputStreamWriter.write("ack");
						outputStreamWriter.flush();
						outputStreamWriter.close();
					}else if(msgType.equalsIgnoreCase("delete")) {
						String key = msg.split(":")[1];
						synchronized (this){
							SimpleDynamoProvider.Messages.remove(key);
						}
						OutputStream outputStream = socket.getOutputStream();
						PrintWriter outputStreamWriter = new PrintWriter(outputStream);
						outputStreamWriter.write("ack");
						outputStreamWriter.flush();
						outputStreamWriter.close();
					} else if(msgType.equalsIgnoreCase("GlobalDelete")) {
						synchronized (this){
							SimpleDynamoProvider.Messages.clear();
						}
						OutputStream outputStream = socket.getOutputStream();
						PrintWriter outputStreamWriter = new PrintWriter(outputStream);
						outputStreamWriter.write("ack");
						outputStreamWriter.flush();
						outputStreamWriter.close();
					} else if(msgType.equalsIgnoreCase("query")) {
						String key =  msg.split(":")[1];
						String value = null;
						synchronized (this) {
							value = SimpleDynamoProvider.Messages.get(key);
						}
						String returnMsg = "nill";
						if(value != null) {
							returnMsg = key + ":" + value;
						}
						OutputStream outputStream = socket.getOutputStream();
						PrintWriter outputStreamWriter = new PrintWriter(outputStream);
						outputStreamWriter.write(returnMsg);
						outputStreamWriter.flush();
						outputStreamWriter.close();
					} else if(msgType.equalsIgnoreCase("GDump")) {
						String returnMsg = "";
						Cursor result = new SimpleDynamoProvider().LDump();
						if(result.getCount()!=0) {
							result.moveToFirst();
							do {
								returnMsg+=result.getString(0)+":"+result.getString(1)+":";
							}
							while (result.moveToNext());
						}

						if(returnMsg == "") {
							returnMsg = "nill";
						}
						OutputStream outputStream = socket.getOutputStream();
						PrintWriter outputStreamWriter = new PrintWriter(outputStream);
						outputStreamWriter.write(returnMsg);
						outputStreamWriter.flush();
						outputStreamWriter.close();
					}

					publishProgress(msg);
					bufferedReader.close();
					//socket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			return null;
		}

		protected void onProgressUpdate(String...strings) {

			TextView remoteTextView = (TextView) findViewById(R.id.textView1);
			String strReceived = strings[0].trim();
			remoteTextView.append(strReceived + "\t\n");

			return;
		}
	}

	private class RecoveryTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {
			synchronized (this){
				SimpleDynamoProvider.Messages.clear();
			}
			Cursor queryResult = new SimpleDynamoProvider().query(buildUri(scheme,authority),null,"*",null,null);
			if(queryResult.getCount()!=0) {
				queryResult.moveToFirst();
				do {
					String key = queryResult.getString(0);
					String value = queryResult.getString(1);
					String location = new SimpleDynamoProvider().getLocationOfMsg(key);
					for(String node : location.split(":")){
						if(myNode.getNodePort().equalsIgnoreCase(node)){
							SimpleDynamoProvider.Messages.put(key,value);
						}
					}
				}
				while (queryResult.moveToNext());
			}

			return null;
		}
	}

	public static String forwardQuery(String msgType,String selection,String port) {
		String msg = msgType+":"+selection+":"+port;

		try {
			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					Integer.parseInt(port));

			OutputStream outputStream = socket.getOutputStream();
			InputStream inputStream = socket.getInputStream();
			PrintWriter outputStreamWriter = new PrintWriter(outputStream,true);
			outputStreamWriter.println(msg);
			outputStreamWriter.flush();

			InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
			BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
			String dump = bufferedReader.readLine();
			if(dump != null) {
				socket.close();
				return dump;
			}
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}
}
