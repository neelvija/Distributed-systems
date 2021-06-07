package edu.buffalo.cse.cse486586.simpledynamo;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;

public class SimpleDynamoProvider extends ContentProvider {

	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	public static HashMap<String,String> Messages = new HashMap<String, String>();
	public static HashMap<String,String> GlobalMessages = new HashMap<String, String>();
	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";
	final String authority = "edu.buffalo.cse.cse486586.simpledht.provider";
	final String scheme = "content";

	@Override
	public synchronized int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		if(selection.equalsIgnoreCase("@")) {
			synchronized (this){
				Messages.clear();
			}
		} else if(selection.equalsIgnoreCase("*")) {
			synchronized (this) {
				Messages.clear();
			}
			for(Node node : SimpleDynamoActivity.connectedNodes) {
				if(node.getNodeId().equalsIgnoreCase(SimpleDynamoActivity.myNode.getNodeId())){
					continue;
				}
				String response = SimpleDynamoActivity.forwardQuery("GlobalDelete",selection,node.getNodePort());
			}
		} else {
			String location = getLocationOfMsg(selection);
			for(String node : location.split(":")) {
				if(node.equalsIgnoreCase(SimpleDynamoActivity.myNode.getNodePort())) {
					synchronized (this) {
						Messages.remove(selection);
					}
				} else {
					SimpleDynamoActivity.forwardQuery("delete",selection,node);
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
	public synchronized Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		String key = (String) values.get(KEY_FIELD);
		String value = (String) values.get(VALUE_FIELD);

		String location = getLocationOfMsg(key);
		for(String node : location.split(":")) {
			if(node.equalsIgnoreCase(SimpleDynamoActivity.myNode.getNodePort())) {
				synchronized (this) {
					Messages.put(key, value);
				}
			} else {
				String contentValues = key+":"+value;
				SimpleDynamoActivity.forwardQuery("insert",contentValues,node);
			}
		}
		return uri;
	}

	public String getLocationOfMsg(String key) {
		String location = "";
		try {
		for(Node node : SimpleDynamoActivity.connectedNodes) {
			if(node.getNodeId().equalsIgnoreCase(SimpleDynamoActivity.connectedNodes.get(0).getNodeId()) && (genHash(key).compareTo(node.getNodeKey()) <= 0 || genHash(key).compareTo(node.getPredecessor1().getNodeKey()) > 0 )) {
				location = node.getNodePort()+":"+node.getSuccessor1().getNodePort()+":"+node.getSuccessor2().getNodePort();
			}
			else if(genHash(key).compareTo(node.getNodeKey()) <= 0 && genHash(key).compareTo(node.getPredecessor1().getNodeKey()) > 0 ) {
				location = node.getNodePort()+":"+node.getSuccessor1().getNodePort()+":"+node.getSuccessor2().getNodePort();
			}
		}
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return location;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public synchronized Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		if(selection.equalsIgnoreCase("@")) {
			return LDump();
		} else if(selection.equalsIgnoreCase("*")) {
			GlobalMessages.clear();
			GlobalMessages.putAll(Messages);
			for(Node node : SimpleDynamoActivity.connectedNodes) {
				if(node.getNodeId().equalsIgnoreCase(SimpleDynamoActivity.myNode.getNodeId())){
					continue;
				}
				String response = SimpleDynamoActivity.forwardQuery("GDump",selection,node.getNodePort());
				if(response != null ) {
					if(response.equalsIgnoreCase("nill")){
						continue;
					} else {
						String[] temp = response.split(":");
						for(int i=0;i<temp.length-1;i+=2) {

							String key = temp[i].trim();
							String value = temp[i+1].trim();
							GlobalMessages.put(key,value);
						}
					}
				}
			}
			String[] columnNames = new String[2];
			columnNames[0]=KEY_FIELD;
			columnNames[1]=VALUE_FIELD;

			MatrixCursor matrixCursor = new MatrixCursor(columnNames);
			synchronized (this) {
				for (Map.Entry<String, String> entry : GlobalMessages.entrySet()) {
					String key = entry.getKey();
					String value = entry.getValue();

					Object[] result = new Object[2];
					result[0] = key.trim();
					result[1] = value.trim();
					matrixCursor.addRow(result);
				}
			}
			return matrixCursor;

		} else {
			String location = getLocationOfMsg(selection);
			String nodeToSearch = location.split(":")[2];
			/*if(nodeToSearch.equalsIgnoreCase(SimpleDynamoActivity.myNode.getNodePort())) {
				String[] columnNames = new String[2];
				columnNames[0]=KEY_FIELD;
				columnNames[1]=VALUE_FIELD;

				MatrixCursor matrixCursor = new MatrixCursor(columnNames);

				String key = selection;
				String value = Messages.get(selection);
				if(value != null){
					Object[] result = new Object[2];
					result[0] = key.trim();
					result[1] = value.trim();
					matrixCursor.addRow(result);

					return matrixCursor;
				}
			} else {*/
				String ack = SimpleDynamoActivity.forwardQuery("query",selection,nodeToSearch);
				if(ack == null) {
					nodeToSearch = location.split(":")[1];
					ack = SimpleDynamoActivity.forwardQuery("query",selection,nodeToSearch);
					if(ack == null) {
						nodeToSearch = location.split(":")[0];
						ack = SimpleDynamoActivity.forwardQuery("query",selection,nodeToSearch);
					} else if(ack.equalsIgnoreCase("nill")){
						nodeToSearch = location.split(":")[0];
						ack = SimpleDynamoActivity.forwardQuery("query",selection,nodeToSearch);
					}
				} else if(ack.equalsIgnoreCase("nill") ) {
					nodeToSearch = location.split(":")[1];
					ack = SimpleDynamoActivity.forwardQuery("query",selection,nodeToSearch);
					if(ack == null) {
						nodeToSearch = location.split(":")[0];
						ack = SimpleDynamoActivity.forwardQuery("query",selection,nodeToSearch);
					} else if(ack.equalsIgnoreCase("nill")){
						nodeToSearch = location.split(":")[0];
						ack = SimpleDynamoActivity.forwardQuery("query",selection,nodeToSearch);
					}
				}
				String[] columnNames = new String[2];
				columnNames[0]=KEY_FIELD;
				columnNames[1]=VALUE_FIELD;

				MatrixCursor matrixCursor = new MatrixCursor(columnNames);

				String key = ack.split(":")[0];
				String value = ack.split(":")[1];

				Object[] result = new Object[2];
				result[0] = key.trim();
				result[1] = value.trim();
				matrixCursor.addRow(result);

				return matrixCursor;
			//}
		}
	}

	public MatrixCursor LDump() {

		String[] columnNames = new String[2];
		columnNames[0]=KEY_FIELD;
		columnNames[1]=VALUE_FIELD;

		MatrixCursor matrixCursor = new MatrixCursor(columnNames);
		synchronized (this) {
			for (Map.Entry<String, String> entry : Messages.entrySet()) {
				String key = entry.getKey();
				String value = entry.getValue();

				Object[] result = new Object[2];
				result[0] = key.trim();
				result[1] = value.trim();
				matrixCursor.addRow(result);
			}
		}
		return matrixCursor;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
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
}
