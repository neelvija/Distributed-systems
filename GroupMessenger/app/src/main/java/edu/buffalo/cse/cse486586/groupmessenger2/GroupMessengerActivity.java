package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.os.Environment;
import android.telephony.TelephonyManager;
import android.test.MoreAsserts;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.EditText;
import android.widget.TextView;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 * 
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {
    public static int myProposal=0;
    public static int keyIncrementor = 0;
    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final List<String> portList = new ArrayList<String>(Arrays.asList("11108","11112","11116","11120","11124"));
    static final int SERVER_PORT = 10000;
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";

    final String authority = "edu.buffalo.cse.cse486586.groupmessenger2.provider";
    final String scheme = "content";
    Uri uri = buildUri(scheme,authority);

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        final EditText editText = (EditText) findViewById(R.id.editText1);

        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        Log.e("port :",myPort);
        try {
            /*
             * Create a server socket as well as a thread (AsyncTask) that listens on the server
             * port.
             *
             * AsyncTask is a simplified thread construct that Android provides. Please make sure
             * you know how it works by reading
             * http://developer.android.com/reference/android/os/AsyncTask.html
             */
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            Log.e("serverSocket","serverSocketCreated");
            //serverSocket.setReuseAddress(true);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket :" + e);
            return;
        }

        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));

        findViewById(R.id.button4).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                String msg = editText.getText().toString();
                editText.setText("");

                msg = "req:"+myPort+":"+msg;
                //Log.e("firstOnclick",msg);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
            }
        });

        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */
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
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {


        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            String reqPort;
            String msgReceived;

            PriorityQueue<Message> Messages = new PriorityQueue<Message>();
            while(!serverSocket.isClosed()) {
                try {
                    Socket socket = serverSocket.accept();
                    //socket.setReuseAddress(true);
                    InputStream inputStream = socket.getInputStream();
                    InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                    BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                    String msg = bufferedReader.readLine();
                    //Log.e("msg received",msg);
                    if(msg.split(":")[0].equals("req")) {
                        reqPort = msg.split(":")[1];
                        msgReceived = msg.split(":")[2];

                        String msgToSend = "prop:"+myProposal+":"+msgReceived;
                        //Log.e("myproposal",msgToSend);
                        myProposal+=1;
                        //Log.e("myproposal aft incrmnt",myProposal+"");
                        try {
                            //socket.setReuseAddress(true);

                            //Log.e("proposal sent : ",propSocket.getPort()+","+propSocket.getInetAddress());
                            OutputStream outputStream = socket.getOutputStream();
                            PrintWriter outputStreamWriter = new PrintWriter(outputStream);
                            outputStreamWriter.write(msgToSend);
                            outputStreamWriter.flush();
                            outputStreamWriter.close();
                            //propSocket.close();
                        } catch (IOException e) {
                            Log.e("exception :",e.getMessage());
                            e.printStackTrace();
                            //myProposal-=1;
                        }
                    } else if(msg.split(":")[0].equals("final")) {
                        Message msg1 = new Message(msg.split(":")[2],Integer.parseInt(msg.split(":")[1].split("\\.")[1]),Integer.parseInt(msg.split(":")[1].split("\\.")[0]));
                        Messages.add(msg1);
                    }

                    while(!Messages.isEmpty()) {
                        publishProgress(Messages.poll().getMsg());
                    }
                    bufferedReader.close();
                    //socket.close();
                } catch (IOException e) {
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
            keyIncrementor+=1;

            String val = strReceived;
            Log.e("fileNameStored : value ",key+" : "+val);
            ContentValues cv = new ContentValues();
            cv.put(KEY_FIELD, key);
            cv.put(VALUE_FIELD, val);

            getContentResolver().insert(buildUri(scheme,authority),cv);
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
                String msgToSend = msgs[0];
                //Log.e("firstClientTask",msgToSend);
                String currentMsg = msgToSend.split(":")[2];
                List<Double> proposedNumbers = new ArrayList<Double>();
                List<String> failedPorts = new ArrayList<String>();
                List<String> currPortList = portList;

                for (String portNumber: currPortList) {
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(portNumber));
                        socket.setSoTimeout(500);
                        OutputStream outputStream = socket.getOutputStream();
                        InputStream inputStream = socket.getInputStream();
                        PrintWriter outputStreamWriter = new PrintWriter(outputStream,true);
                        outputStreamWriter.println(msgToSend);
                        outputStreamWriter.flush();

                        if(msgToSend.split(":")[0].equals("req")) {
                            InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                            String msg = bufferedReader.readLine();
                            if(msg == null) {
                                failedPorts.add(portNumber);
                                continue;
                            }
                            if (msg.split(":")[0].equals("prop") && msg.split(":")[2].equals(currentMsg)) {
                                String proposedNum = msg.split(":")[1]+"."+portNumber;
                                proposedNumbers.add(Double.parseDouble(proposedNum));
                            }

                        }
                        socket.close();
                    } catch (SocketTimeoutException e) {
                        Log.e("failed",portNumber+" failed for msg :"+msgToSend);
                        failedPorts.add(portNumber);
                        continue;
                    }
                }
                //currPortList.removeAll(failedPorts);
                if(proposedNumbers!=null) {
                    Double max = 0.0;
                    for (Double num : proposedNumbers) {
                        //Log.e("proposed num",num+"");
                        if (num > max) {
                            //Log.e("in if block with ",num+"");
                            max = num;
                        }
                    }
                    proposedNumbers.clear();
                    String finalMsg = "final:" + max + ":" + currentMsg;

                    for (String port : currPortList) {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(port));
                        OutputStream outputStream = socket.getOutputStream();
                        PrintWriter outputStreamWriter = new PrintWriter(outputStream);
                        outputStreamWriter.write(finalMsg);
                        outputStreamWriter.flush();
                        outputStreamWriter.close();
                        socket.close();
                    }
                }

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
                Log.e(TAG,e.getMessage());
                e.printStackTrace();
            }

            return null;
        }
    }
}
