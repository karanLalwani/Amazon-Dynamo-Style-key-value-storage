package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Formatter;
import java.util.List;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.text.TextUtils;
import android.util.Log;

import static android.content.ContentValues.TAG;

public class SimpleDynamoProvider extends ContentProvider {

	static final String[] remotePorts = {"11108", "11112", "11116", "11120", "11124"};
	static final int SERVER_PORT = 10000;
	static int count = 0;
	List<Node> nodeList = new ArrayList<Node>();
	Node myNode = null;

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}
	Uri providerUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");


	public void deleteValue(String selection){
		Context context = getContext();
		File key = new File(context.getFilesDir(),selection);
		if(key.exists()){
			key.delete();
		}
	}

	public void deleteAll(){
		Context context = getContext();
		File dirFiles = context.getFilesDir();
		for(File key : dirFiles.listFiles()){
			key.delete();
		}
	}

	public void deleteCurrent(String portNum){
		Context context = getContext();
		File dirFiles = context.getFilesDir();

		for(File key : dirFiles.listFiles()){
			try{
				InputStreamReader isr = new InputStreamReader(context.openFileInput(String.valueOf(key)));
				BufferedReader br = new BufferedReader(isr);
				String value = br.readLine();
				String[] newV = value.split(";", 3);
				if(newV[1].equals(portNum)){
					key.delete();
				}
				br.close();
				isr.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		Node dNode = null;
		String msg = null;
		if(selection.equals("@")){
			msg = "DeletePred;" + myNode.getPort_num();
			deleteCurrent(myNode.getPort_num());
			//send to Node's successors
			sendMsgToNode(myNode.getSuccessor_1_port_num(), msg);
			sendMsgToNode(myNode.getSuccessor_2_port_num(), msg);
		}else if(selection.equals("*")){
			msg = "DeleteAll;*";
			for (int i = 0; i < nodeList.size(); i++) {
				Log.d(TAG, "Delete All : " + i);
				Node node = nodeList.get(i);
				sendMsgToNode(node.getPort_num(), msg);
			}
		}else{
			String selectionHash = null;
			try {
				selectionHash = genHash(selection);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
			dNode = findNode(selectionHash);
			msg = "Delete;" + selection;
			//send to Node
			sendMsgToNode(dNode.getPort_num(), msg);
			//send to Node's successors
			sendMsgToNode(dNode.getSuccessor_1_port_num(), msg);
			sendMsgToNode(dNode.getSuccessor_2_port_num(), msg);
		}
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	public Node findNode(String key){
		int n = nodeList.size();
		Log.d(TAG, "doInBackground: length of node list " + n);
		for (int i = 0; i < nodeList.size(); i++) {
			Log.d(TAG, "finding node: " + i);
			Node node = nodeList.get(i);
			if(key.compareTo(node.getNode_id()) < 0){
				return node;
			}else if(i == nodeList.size()-1){
				return nodeList.get(0);
			}
		}
		return null;
	}

	public void insertValue(String msg){
		Context context = getContext();

		if(msg.equals("")){
			return;
		}

		Log.d(TAG, "insertValue: " + msg);
		String[] store = msg.split(":", 2);
		File key = new File(context.getFilesDir(),store[0]);

		Log.d(TAG, "insertValue fileName " + store[0]);
		Log.d(TAG, "insertValue: valueToStore " + store[1]);
		try{
			if(key.exists()){
				InputStreamReader isr = new InputStreamReader(context.openFileInput(store[0]));
				BufferedReader br = new BufferedReader(isr);
				String value = br.readLine();
				String[] oldV = value.split(";", 3);
				String[] newV = store[1].split(";", 3);
 				if(newV[0].compareTo(oldV[0]) > 0) {
					OutputStreamWriter osw = new OutputStreamWriter(context.openFileOutput(store[0], Context.MODE_PRIVATE));
					osw.write(store[1]);
					osw.close();
					Log.d(TAG, "insert new value: "+ store[1]);
				}else{
					Log.d(TAG, "insert old value: "+ value);
				}
				br.close();
 				isr.close();
			}else{
				OutputStreamWriter osw = new OutputStreamWriter(context.openFileOutput(store[0], Context.MODE_PRIVATE));
				osw.write(store[1]);
				osw.close();
				Log.d(TAG, "insert new value: " + store[1]);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public String sendMsgToNode(String portNumber, String msg){
		// send message to server
		Socket socket = null;
		Log.d(TAG, "sendMsgToNode: " + msg);
		try {
			socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					Integer.parseInt(portNumber));
			DataOutputStream out = new DataOutputStream(socket.getOutputStream());
			out.writeUTF(msg);
			out.flush();

			DataInputStream in = new DataInputStream(socket.getInputStream());
			String recStr = in.readUTF();
			Log.d(TAG, "doInBackground: string received from server " + recStr);
			out.close();
			in.close();
			socket.close();
			return recStr;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return "";
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		Date date = new Date();
		long timeMilli = date.getTime();
		Log.d(TAG, "insert: current time " + timeMilli);

		String fileName = values.getAsString("key");
		String valueToStore = values.getAsString("value");

		String fileNameHash = null;
		try {
			fileNameHash = genHash(fileName);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		Node iNode = findNode(fileNameHash);
		String msg = "Insert;" + fileName + ":" + timeMilli + ";" + iNode.getPort_num() + ";" + valueToStore;
		Log.d(TAG, "insert: message " + msg);

		//send to Node
		sendMsgToNode(iNode.getPort_num(), msg);
		//send to Node's successors
		sendMsgToNode(iNode.getSuccessor_1_port_num(), msg);
		sendMsgToNode(iNode.getSuccessor_2_port_num(), msg);

		return null;
	}

	public String getHash(String emulNumber){
		String hash = null;
		try {
			hash = genHash(emulNumber);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return hash;
	}

	public void createRing(String myPortNum){
		String hash = null;
		Node tempNode = null;
		for(String rp : remotePorts){
			String emulNum = String.valueOf((Integer.parseInt(rp)/2));
			hash = getHash(emulNum);
			tempNode = new Node(hash, rp, emulNum, rp, hash, rp, hash, rp, hash, rp, hash);
			nodeList.add(tempNode);
			if(rp.equals(myPortNum)){
				myNode = tempNode;
				Log.d(TAG, "My Node : " + myNode.getPort_num());
				Log.d(TAG, "My Node : " + myNode.getNode_id());
			}
		}

		Collections.sort(nodeList);
		int n = nodeList.size();
		Log.d(TAG, "doInBackground: length of node list " + n);
		for (int i = 0; i < nodeList.size(); i++) {
			Log.d(TAG, "createRing: " + i);
			Node node = nodeList.get(i);

			node.setPredecessor_2_port_num(nodeList.get((n + i - 2) % n).getPort_num());
			String emul = String.valueOf((Integer.parseInt(node.getPredecessor_2_port_num())/2));
			node.setPredecessor_2_node_id(getHash(emul));

			node.setPredecessor_1_port_num(nodeList.get((n + i - 1) % n).getPort_num());
			emul = String.valueOf((Integer.parseInt(node.getPredecessor_1_port_num())/2));
			node.setPredecessor_1_node_id(getHash(emul));

			node.setSuccessor_1_port_num(nodeList.get((n + i + 1) % n).getPort_num());
			emul = String.valueOf((Integer.parseInt(node.getSuccessor_1_port_num())/2));
			node.setSuccessor_1_node_id(getHash(emul));

			node.setSuccessor_2_port_num(nodeList.get((n + i + 2) % n).getPort_num());
			emul = String.valueOf((Integer.parseInt(node.getSuccessor_2_port_num())/2));
			node.setSuccessor_2_node_id(getHash(emul));

		}

		Log.d(TAG, "createRing: created");

		for (int i = 0; i < nodeList.size(); i++) {
			Node node = nodeList.get(i);
			Log.d(TAG, "doInBackground: " + "Node : " + node.getPort_num());
			Log.d(TAG, "doInBackground: " + "Node P_2: " + node.getPredecessor_2_port_num());
			Log.d(TAG, "doInBackground: " + "Node P_1: " + node.getPredecessor_1_port_num());
			Log.d(TAG, "doInBackground: " + "Node S_1: " + node.getSuccessor_1_port_num());
			Log.d(TAG, "doInBackground: " + "Node S_2: " + node.getSuccessor_2_port_num());
			Log.d(TAG, "doInBackground: " + "Node nodeId: " + node.getNode_id());
			Log.d(TAG, "doInBackground: " + "Node P_2 nodeId: " + node.getPredecessor_2_node_id());
			Log.d(TAG, "doInBackground: " + "Node P_1 nodeId: " + node.getPredecessor_1_node_id());
			Log.d(TAG, "doInBackground: " + "Node S_1 nodeId: " + node.getSuccessor_1_node_id());
			Log.d(TAG, "doInBackground: " + "Node S_2 nodeId: " + node.getSuccessor_2_node_id());
		}

	}

	public void recover(){
		deleteAll();
		String recoverMsg = "";
		Log.d(TAG, "recover: All messages deleted");

		// recover My node messages
		String msg = "Recover;" + myNode.getPort_num();
		recoverMsg = sendMsgToNode(myNode.getSuccessor_1_port_num(), msg);
		if(recoverMsg.equals("")){
			recoverMsg = sendMsgToNode(myNode.getSuccessor_2_port_num(), msg);
		}
		recoveryInsert(recoverMsg);

		// recover Replication Messages
		msg = "Recover;" + myNode.getPredecessor_1_port_num();
		recoverMsg = sendMsgToNode(myNode.getPredecessor_1_port_num(), msg);
		if(recoverMsg.equals("")){
			recoverMsg = sendMsgToNode(myNode.getSuccessor_1_port_num(), msg);
		}
		recoveryInsert(recoverMsg);

		// recover Replication Messages
		msg = "Recover;" + myNode.getPredecessor_2_port_num();
		recoverMsg = sendMsgToNode(myNode.getPredecessor_2_port_num(), msg);
		if(recoverMsg.equals("")){
			recoverMsg = sendMsgToNode(myNode.getPredecessor_1_port_num(), msg);
		}
		recoveryInsert(recoverMsg);
        Log.d(TAG, "recovery done!!");
	}

	public void recoveryInsert(String recoverMsg){
		String[] records = recoverMsg.split("-");
		for(String r : records){
			insertValue(r);
			Log.d(TAG, "recover: insert value " + r);
		}
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub

		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        Log.d(TAG, "onCreate: Started ");
		
		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
			return false;
		}
		Log.d(TAG, "onCreate: emul " + portStr);
		Log.d(TAG, "onCreate: myport " + myPort);
		createRing(myPort);

        Log.d(TAG, "onCreate: clientTask recovery");
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "callRecovery", myPort);
        Log.d(TAG, "onCreate: clientTask recovery");
		return true;
	}

	public String queryNode(String selection){
		String value = "";
		String msg = null;
		Context context = getContext();
		try {
			InputStreamReader isr = new InputStreamReader(context.openFileInput(selection));
			BufferedReader br = new BufferedReader(isr);
			msg = br.readLine();
//			String[] m = msg.split(";", 3);
			value = selection + ":" + msg;
			Log.d(TAG, "query: " + selection);
			Log.d(TAG, "queryNode: " + msg);
			br.close();
			isr.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return value;
	}


	public Cursor getCursor(String curStr){
		MatrixCursor cursor = new MatrixCursor(new String[] {"key", "value"});
		if(curStr.equals("")){
			return cursor;
		}
		String[] record = curStr.split("-");
		for(String r : record){
			String[] v = r.split(":");
			String[] m = v[1].split(";");
			cursor.addRow(new String[]{v[0], m[2]});
		}
		return cursor;
	}

	public String localKeys(String portNum){
		List<String> finalStr = new ArrayList<String>();
		String msg = null;
		Context context = getContext();
		try {
			File dirFiles = context.getFilesDir();
			for (String strFile : dirFiles.list())
			{
				InputStreamReader isr = new InputStreamReader(context.openFileInput(strFile));
				BufferedReader br = new BufferedReader(isr);
				msg = br.readLine();
				String[] m = msg.split(";", 3);
				if(m[1].equals(portNum) || portNum == null){
					finalStr.add(strFile+":"+msg);
				}
				br.close();
				isr.close();
				Log.d(TAG, "query: " + strFile);
				Log.d(TAG, "value: " + msg);
			}
			return TextUtils.join("-", finalStr);
		} catch (IOException e) {
			e.printStackTrace();
		}
		Log.d(TAG, "localKeys: final string " + finalStr);
		return "";
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
        Log.d(TAG, "query is " + selection);

		String msg = null;
		if(selection.equals("@")){
			return getCursor(localKeys(null));
		}else if(selection.equals("*")){
			String a = "";
			for (int i = 0; i < nodeList.size(); i++) {
				Log.d(TAG, "queryAll: " + i);
				Node node = nodeList.get(i);
				msg = "QueryAll;"+selection;
				String str = sendMsgToNode(node.getPort_num(), msg);
				if(str.equals("")){
					str = sendMsgToNode(node.getSuccessor_1_port_num(), msg);
					if(str.equals("")){
						str = sendMsgToNode(node.getSuccessor_2_port_num(), msg);
					}
				}
				a += str + "-";
			}
			return getCursor(a);
		}else{
			String selectionHash = null;
			try {
				selectionHash = genHash(selection);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
			Node iNode = findNode(selectionHash);
			msg = "Query;"+selection;
			String str = sendMsgToNode(iNode.getPort_num(), msg);
			if(str.equals("")){
				str = sendMsgToNode(iNode.getSuccessor_1_port_num(), msg);
				if(str.equals("")){
					str = sendMsgToNode(iNode.getSuccessor_2_port_num(), msg);
				}
			}
			return getCursor(str);
		}
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

	private class ServerTask extends AsyncTask<ServerSocket, String, Void>{

		@Override
		protected Void doInBackground(ServerSocket... sockets){
			ServerSocket serverSocket = sockets[0];
			Socket s = null;
			DataInputStream in = null;
			String str = null;
			try {
				do{
					s = serverSocket.accept();
					in = new DataInputStream(s.getInputStream());
					str = in.readUTF();
					Log.d(TAG, "Server Received string is " + str);
					String[] messageReceived = str.split(";", 2);
					String msgToSend = null;
					if (messageReceived[0].equals("Insert")) {
						insertValue(messageReceived[1]);
						msgToSend = "Received";
					} else if(messageReceived[0].equals("Delete")){
						deleteValue(messageReceived[1]);
						msgToSend = "Received";
					} else if(messageReceived[0].equals("DeletePred")){
						deleteCurrent(messageReceived[1]);
						msgToSend = "Deleted";
					} else if(messageReceived[0].equals("DeleteAll")){
						deleteAll();
						msgToSend = "Deleted";
					} else if(messageReceived[0].equals("Query")){
						msgToSend = queryNode(messageReceived[1]);
					} else if(messageReceived[0].equals("QueryAll")){
						msgToSend = localKeys(null);
					} else if(messageReceived[0].equals("callRecover")){
						recover();
						msgToSend = "Received";
					} else if(messageReceived[0].equals("Recover")){
						msgToSend = localKeys(messageReceived[1]);
					}
					DataOutputStream out = new DataOutputStream(s.getOutputStream());
					Log.d(TAG, "doInBackground: " + msgToSend);
					out.writeUTF(msgToSend);
					out.flush();
				}while(str != null);
			}catch (IOException e) {
				e.printStackTrace();
				Log.d(TAG, "doInBackground: its io error");
			}

			return null;
		}
	}

	private class ClientTask extends AsyncTask<String, Void, Void>{

		@Override
		protected Void doInBackground(String... strings) {
            Log.d(TAG, "doInBackground: client started");
            Log.d(TAG, "doInBackground: ff" + strings[0]);
			if(strings[0].equals("callRecovery")){
				Log.d(TAG, "doInBackground: calling recovery");
				recover();
				Log.d(TAG, "doInBackground: recovered");
			}
//			try{
//				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
//						Integer.parseInt(myNode.getPort_num()));
//				String msgToSend = strings[0];
//				Log.d(TAG, "client msgToSend " + msgToSend);
//				DataOutputStream out = new DataOutputStream(socket.getOutputStream());
//				out.writeUTF(msgToSend);
//				out.flush();
//
//				DataInputStream in = new DataInputStream(socket.getInputStream());
//				String recStr = in.readUTF();
//
//				socket.close();
//			} catch (UnknownHostException e) {
//				Log.e(TAG, "ClientTask UnknownHostException");
//			} catch (IOException e) {
//				Log.e(TAG, "ClientTask socket IOException");
//			}
			return null;
		}
	}

}
