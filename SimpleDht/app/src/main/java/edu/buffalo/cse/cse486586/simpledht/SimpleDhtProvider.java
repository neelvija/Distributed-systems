package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;

import android.app.Activity;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {
    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    static List<String> msgsStored = new ArrayList<String>();
    static Boolean GDumpInit = false;
    static Boolean GlobalDeleteInit = false;
    Context context = getContext();

    public SimpleDhtProvider(Context context) {
        this.context = context;
    }

    public SimpleDhtProvider() {
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        if(selection == "*") {
            GlobalDeleteInit = true;
            deleteGlobal(SimpleDhtActivity.myNodeId);
        } else if(selection == "@") {
            deleteLocal();
        } else {
            String selectionKey = "";
            try {
                selectionKey = genHash(selection);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            if(SimpleDhtActivity.myPredeccessorId == null) {
                if(msgsStored.contains(selection)) {
                    if(context == null) {
                        context = getContext();
                    }
                    context.deleteFile(selection);
                    msgsStored.remove(selection);
                }
            } else {
                String predKey = "";
                try {
                    predKey = genHash(SimpleDhtActivity.myPredeccessorId);
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
                if(SimpleDhtActivity.myNodeId.equalsIgnoreCase(SimpleDhtActivity.headNodeId) && (selectionKey.compareTo(SimpleDhtActivity.myNodeKey)<0 || selectionKey.compareTo(predKey)>0)) {
                        if(msgsStored.contains(selection)) {
                            if(context == null) {
                                context = getContext();
                            }
                            context.deleteFile(selection);
                            msgsStored.remove(selection);
                        }
                } else if(selectionKey.compareTo(SimpleDhtActivity.myNodeKey)<0 && selectionKey.compareTo(predKey)>0) {
                    if(msgsStored.contains(selection)) {
                        if(context == null) {
                            context = getContext();
                        }
                        context.deleteFile(selection);
                        msgsStored.remove(selection);
                    }
                } else {
                    SimpleDhtActivity.forwardQuery("delete",selection);
                }
            }
        }

        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        String key = values.get("key").toString();
        String value = values.get("value").toString();
        // TODO Auto-generated method stub
        String hashKey = "";
        try {
            hashKey = genHash(key);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        if(SimpleDhtActivity.myPredeccessorId != null && SimpleDhtActivity.myNodeKey != null){
            String predKey = "";
            try {
                predKey = genHash(SimpleDhtActivity.myPredeccessorId);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            if(SimpleDhtActivity.myNodeId.equalsIgnoreCase(SimpleDhtActivity.headNodeId) && (hashKey.compareTo(SimpleDhtActivity.myNodeKey) < 0 || hashKey.compareTo(predKey) > 0)) {
                insertLocally(key,value);
            } else if(hashKey.compareTo(SimpleDhtActivity.myNodeKey)<0 && hashKey.compareTo(predKey)>0) {
                insertLocally(key,value);
            } else {
                String contentValues = key+":"+value;
                SimpleDhtActivity.forwardQuery("insert",contentValues);
            }
        } else {
            insertLocally(key,value);
        }
        return uri;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub
        if(selection.equalsIgnoreCase("*")) {
            if(SimpleDhtActivity.myNodeId == null) {
                return LDump();
            }
            GDumpInit = true;
            String GDump = GDump(SimpleDhtActivity.myNodeId);
            String[] columnNames = new String[2];
            columnNames[0]=KEY_FIELD;
            columnNames[1]=VALUE_FIELD;

            MatrixCursor matrixCursor = new MatrixCursor(columnNames);
            String[] temp = GDump.split(":");
            for(int i=0;i<temp.length-1;i+=2) {
                Object[] result = new Object[2];
                result[0] = temp[i].trim();
                result[1] = temp[i+1].trim();
                matrixCursor.addRow(result);
            }
            return matrixCursor;

        } else if(selection.equalsIgnoreCase("@")) {
            return LDump();
        } else {
            String selectionKey = "";
            try {
                selectionKey = genHash(selection);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            if(SimpleDhtActivity.myPredeccessorId != null && SimpleDhtActivity.myNodeKey != null){
                String predKey = "";
                try {
                    predKey = genHash(SimpleDhtActivity.myPredeccessorId);
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
                if(SimpleDhtActivity.myNodeId.equalsIgnoreCase(SimpleDhtActivity.headNodeId)) {
                    if (selectionKey.compareTo(SimpleDhtActivity.myNodeKey) < 0 || selectionKey.compareTo(predKey) > 0) {
                        return getLocalMsg(selection);
                    }
                }
                if(selectionKey.compareTo(SimpleDhtActivity.myNodeKey)<0 && selectionKey.compareTo(predKey)>0) {
                    return getLocalMsg(selection);
                } else {
                    String queryResult = SimpleDhtActivity.forwardQuery("query",selection);
                    String[] columnNames = new String[2];
                    columnNames[0]=KEY_FIELD;
                    columnNames[1]=VALUE_FIELD;

                    MatrixCursor matrixCursor = new MatrixCursor(columnNames);
                    Object[] result = new Object[2];
                    result[0] = queryResult.split(":")[0].trim();
                    result[1] = queryResult.split(":")[1].trim();
                    matrixCursor.addRow(result);

                    return matrixCursor;
                }
            } else {
                return getLocalMsg(selection);
            }
        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    public String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    public MatrixCursor LDump() {

        String[] columnNames = new String[2];
        columnNames[0]=KEY_FIELD;
        columnNames[1]=VALUE_FIELD;

        MatrixCursor matrixCursor = new MatrixCursor(columnNames);
        for(int i=0; i<msgsStored.size(); i++){
            try {
                String key = msgsStored.get(i);
                if(context == null) {
                    context = getContext();
                }
                FileInputStream fileInputStream = context.openFileInput(key);
                InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
                BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                String msg = bufferedReader.readLine();

                Object[] result = new Object[2];
                result[0] = key.trim();
                result[1] = msg.trim();
                matrixCursor.addRow(result);

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return matrixCursor;
    }

    public String GDump(String initiator) {
        String GDump = "";
        if(!SimpleDhtActivity.myNodeId.equalsIgnoreCase(initiator) || (SimpleDhtActivity.myNodeId.equalsIgnoreCase(initiator)&&GDumpInit)) {
            GDumpInit = false;
            Cursor result = LDump();
            if(result.getCount()!=0) {
                result.moveToFirst();
                do {
                    GDump+=result.getString(0)+":"+result.getString(1)+":";
                }
                while (result.moveToNext());
            }
            if(SimpleDhtActivity.mySuccessorPort != null) {
                String successorDump = SimpleDhtActivity.forwardQuery("GDump", initiator);
                GDump += successorDump;
            }
        }
        return GDump;
    }

    public Cursor getLocalMsg(String selection) {
        String[] columnNames = new String[2];
        columnNames[0]=KEY_FIELD;
        columnNames[1]=VALUE_FIELD;

        MatrixCursor matrixCursor = new MatrixCursor(columnNames);
        try {
            if(context == null) {
                context = getContext();
            }
            FileInputStream fileInputStream = context.openFileInput(selection);
            InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            String msg = bufferedReader.readLine();
            Object[] result = new Object[2];
            result[0] = selection.trim();
            result[1] = msg.trim();
            matrixCursor.addRow(result);
        } catch (IOException e){
            e.printStackTrace();
        }
        return matrixCursor;
    }

    public void insertLocally(String key, String value){
        try{
            if(context == null) {
                context = getContext();
            }
            FileOutputStream fileOutputStream = context.openFileOutput(key, Context.MODE_PRIVATE);
            msgsStored.add(key);
            fileOutputStream.write(value.trim().getBytes());
            fileOutputStream.close();
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    public void deleteLocal() {
        for(int i=0; i<msgsStored.size(); i++){
            if(context == null) {
                context = getContext();
            }
            context.deleteFile(msgsStored.get(i));
        }
        msgsStored.clear();
    }

    public void deleteGlobal(String initiator) {
        if(!SimpleDhtActivity.myNodeId.equalsIgnoreCase(initiator) || (SimpleDhtActivity.myNodeId.equalsIgnoreCase(initiator)&&GlobalDeleteInit)) {
            GlobalDeleteInit = false;
            deleteLocal();
            SimpleDhtActivity.forwardQuery("GlobalDelete",initiator);
        }
    }
}
