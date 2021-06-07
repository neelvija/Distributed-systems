package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentValues;
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
import android.view.View;
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
import java.util.TreeSet;

public class SimpleDhtActivity extends Activity {
    public static int keyIncrementor=0;
    static final String TAG = SimpleDhtActivity.class.getSimpleName();
    static final List<String> portList = new ArrayList<String>(Arrays.asList("11108","11112","11116","11120","11124"));
    static final int SERVER_PORT = 10000;
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    List<Node> connectedNodes = new ArrayList<Node>();
    final String authority = "edu.buffalo.cse.cse486586.simpledht.provider";
    final String scheme = "content";

    static String myNodeId = null;
    static String myNodePort = null;
    static String myNodeKey = null;
    static String myPredeccessorId = null;
    static String myPredeccessorPort = null;
    static String mySuccessorId = null;
    static String mySuccessorPort = null;
    static String headNodeId = null;
    static String headNodePort = null;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_simple_dht_main);

        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());


        findViewById(R.id.button1).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {

            }
        });

        findViewById(R.id.button2).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {

            }
        });

        findViewById(R.id.button3).setOnClickListener(
                new OnTestClickListener(tv, getContentResolver()));


        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            //serverSocket.setReuseAddress(true);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            return;
        }

        try {
            String myNodeKey = new SimpleDhtProvider(SimpleDhtActivity.this.getApplicationContext()).genHash(portStr);
            String ack = sendJoinRequest(portStr,myNodeKey,myPort);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

    }

    public String sendJoinRequest(String myNode,String myNodeKey,String myPort) {
        String ack = null;

        String msgType = "joinRequest";
        String msg = msgType+":"+myNode+":"+myNodeKey+":"+myPort;

        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msg);
        return ack;
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_simple_dht_main, menu);
        return true;
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
                    if(msgType.equalsIgnoreCase("joinRequest")) {
                        String nodeId = msg.split(":")[1];
                        String nodeKey = msg.split(":")[2];
                        String nodePort = msg.split(":")[3];
                        Node newNode = new Node();
                        newNode.setNodeId(nodeId);
                        newNode.setNodeKey(nodeKey);
                        newNode.setNodePort(nodePort);
                        connectedNodes.add(newNode);
                        OutputStream outputStream = socket.getOutputStream();
                        PrintWriter outputStreamWriter = new PrintWriter(outputStream);
                        outputStreamWriter.write("ack");
                        outputStreamWriter.flush();
                        outputStreamWriter.close();

                        int j =0;
                        for(Node node : connectedNodes) {
                            j++;
                        }
                        Collections.sort(connectedNodes);
                        int k =0;
                        for(Node node : connectedNodes) {
                            k++;
                        }
                        if(connectedNodes.size()>1) {
                            for(int i=0;i<connectedNodes.size();i++){
                                //Code to set successor of node
                                if(i==connectedNodes.size()-1){
                                    connectedNodes.get(i).setSuccessor(connectedNodes.get(0));
                                    connectedNodes.get(0).setPredecessor(connectedNodes.get(i));
                                } else{
                                    connectedNodes.get(i).setSuccessor(connectedNodes.get(i+1));
                                    connectedNodes.get(i+1).setPredecessor(connectedNodes.get(i));
                                }
                            }
                            String type = "updateConnectedNodes";
                            String headNodeId = connectedNodes.get(0).getNodeId();
                            String headNodePort = connectedNodes.get(0).getNodePort();
                            for (Node node : connectedNodes) {
                                String receivingNodeId = node.getNodeId();
                                String receivingNodePort = node.getNodePort();
                                String nodePredecessorId = node.getPredecessor().getNodeId();
                                String nodeSuccessorId = node.getSuccessor().getNodeId();
                                String nodePredecessorPort = node.getPredecessor().getNodePort();
                                String nodeSuccessorPort = node.getSuccessor().getNodePort();


                                String msgToSend = type+":"+receivingNodeId+":"+receivingNodePort+":"+nodePredecessorId+":"+nodeSuccessorId+":"+nodePredecessorPort+":"+nodeSuccessorPort+":"+headNodeId+":"+headNodePort;
                                sendMsg(msgToSend);
                            }
                        }


                    } else if(msgType.equalsIgnoreCase("updateConnectedNodes")) {
                        myNodeId = msg.split(":")[1];
                        myNodeKey = new SimpleDhtProvider(SimpleDhtActivity.this.getApplicationContext()).genHash(myNodeId);
                        myNodePort =  msg.split(":")[2];
                        myPredeccessorId =  msg.split(":")[3];
                        mySuccessorId =  msg.split(":")[4];
                        myPredeccessorPort =  msg.split(":")[5];
                        mySuccessorPort =  msg.split(":")[6];
                        headNodeId = msg.split(":")[7];
                        headNodePort = msg.split(":")[8];

                        OutputStream outputStream = socket.getOutputStream();
                        PrintWriter outputStreamWriter = new PrintWriter(outputStream);
                        outputStreamWriter.write("ack");
                        outputStreamWriter.flush();
                        outputStreamWriter.close();
                    } else if(msgType.equalsIgnoreCase("GDump")) {
                        String dump = new SimpleDhtProvider(SimpleDhtActivity.this.getApplicationContext()).GDump(msg.split(":")[1]);
                        OutputStream outputStream = socket.getOutputStream();
                        PrintWriter outputStreamWriter = new PrintWriter(outputStream);
                        if(dump == null) {
                            dump = "";
                        }
                        outputStreamWriter.write(dump);
                        outputStreamWriter.flush();
                        outputStreamWriter.close();
                    } else if (msgType.equalsIgnoreCase("GlobalDelete")) {
                        new SimpleDhtProvider(SimpleDhtActivity.this.getApplicationContext()).deleteGlobal(msg.split(":")[1]);
                        OutputStream outputStream = socket.getOutputStream();
                        PrintWriter outputStreamWriter = new PrintWriter(outputStream);
                        outputStreamWriter.write("ack");
                        outputStreamWriter.flush();
                        outputStreamWriter.close();
                    } else if(msgType.equalsIgnoreCase("query")) {
                        Cursor queryResult = new SimpleDhtProvider(SimpleDhtActivity.this.getApplicationContext()).query(buildUri(scheme,authority),null,msg.split(":")[1],null,null);
                        String returnMsg = "";
                        if(queryResult.getCount()!=0) {
                            queryResult.moveToFirst();
                            do {
                                returnMsg+=queryResult.getString(0)+":"+queryResult.getString(1)+":";
                            }
                            while (queryResult.moveToNext());
                        }
                        OutputStream outputStream = socket.getOutputStream();
                        PrintWriter outputStreamWriter = new PrintWriter(outputStream);
                        outputStreamWriter.write(returnMsg);
                        outputStreamWriter.flush();
                        outputStreamWriter.close();
                    } else if(msgType.equalsIgnoreCase("insert")) {
                        ContentValues cv = new ContentValues();
                        cv.put(KEY_FIELD, msg.split(":")[1]);
                        cv.put(VALUE_FIELD, msg.split(":")[2]);
                        new SimpleDhtProvider(SimpleDhtActivity.this.getApplicationContext()).insert(buildUri(scheme,authority),cv);
                        OutputStream outputStream = socket.getOutputStream();
                        PrintWriter outputStreamWriter = new PrintWriter(outputStream);
                        outputStreamWriter.write("ack");
                        outputStreamWriter.flush();
                        outputStreamWriter.close();
                    } else if(msgType.equalsIgnoreCase("delete")) {
                        new SimpleDhtProvider(SimpleDhtActivity.this.getApplicationContext()).delete(buildUri(scheme,authority),msg.split(":")[1],null);
                        OutputStream outputStream = socket.getOutputStream();
                        PrintWriter outputStreamWriter = new PrintWriter(outputStream);
                        outputStreamWriter.write("ack");
                        outputStreamWriter.flush();
                        outputStreamWriter.close();
                    }

                    publishProgress(msg);
                    bufferedReader.close();
                    //socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            }
            return null;
        }

        protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             */


            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
            String strReceived = strings[0].trim();
            remoteTextView.append(strReceived + "\t\n");

            String key = keyIncrementor+"";
            keyIncrementor++;
            String val = strReceived;


            //getContentResolver().insert(buildUri(scheme,authority),cv);
            /*
             * The following code creates a file in the AVD's internal storage and stores a file.
             *
             * For more information on file I/O on Android, please take a look at
             * http://developer.android.com/training/basics/data-storage/files.html
             */


            return;
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {
                String msg = msgs[0];

                String msgType = msg.split(":")[0];
                if(msgType.equalsIgnoreCase("joinRequest")) {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt("11108"));

                    OutputStream outputStream = socket.getOutputStream();
                    InputStream inputStream = socket.getInputStream();
                    PrintWriter outputStreamWriter = new PrintWriter(outputStream,true);
                    outputStreamWriter.println(msg);
                    outputStreamWriter.flush();

                    InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                    BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                    String ack = bufferedReader.readLine();
                    if(ack != null) {
                        socket.close();
                    }
                } else if(msgType.equalsIgnoreCase("updateConnectedNodes")) {
                    String receivingNode = msg.split(":")[2];
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(receivingNode));

                    OutputStream outputStream = socket.getOutputStream();
                    InputStream inputStream = socket.getInputStream();
                    PrintWriter outputStreamWriter = new PrintWriter(outputStream,true);
                    outputStreamWriter.println(msg);
                    outputStreamWriter.flush();

                    InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                    BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                    String ack = bufferedReader.readLine();
                    if(ack != null) {
                        socket.close();
                    }
                }

            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

            return null;
        }
    }

    public void sendMsg(String msg) {
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msg);
    }

    public static String forwardQuery(String msgType,String initiator) {
        String msg = msgType+":"+initiator;
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(mySuccessorPort));

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
