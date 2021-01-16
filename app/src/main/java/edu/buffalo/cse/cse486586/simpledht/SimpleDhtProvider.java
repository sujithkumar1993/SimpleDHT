package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.os.CancellationSignal;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import org.json.JSONException;
import org.json.JSONObject;
import org.w3c.dom.Node;

//References
//https://stackoverflow.com/questions/8502542/find-element-position-in-a-java-treemap
//https://developer.android.com/training/data-storage/app-specific#java
//https://stackoverflow.com/questions/11239202/list-of-files-of-saved-to-the-internal-storage
//https://stackoverflow.com/questions/31750269/intentservice-class-not-running-asynctask-on-main-ui-thread-method-execute-must
//https://stackoverflow.com/questions/25191512/async-await-return-task
//https://stackoverflow.com/questions/57036093/method-publishprogress-must-be-called-from-the-worker-thread-currently-inferred
//https://stackoverflow.com/questions/6792835/how-do-you-set-a-timeout-on-bufferedreader-and-printwriter-in-java-1-4
//https://stackoverflow.com/questions/28765024/return-string-from-asynctask
///https://stackoverflow.com/questions/27305164/java-socket-bufferedreader-read-not-reading
//https://stackoverflow.com/questions/2810615/how-to-retrieve-data-from-cursor-class
//https://stackoverflow.com/questions/4235996/viewing-an-android-database-cursor
//https://stackoverflow.com/questions/27629311/method-invocation-may-produce-java-nullpointerexception
//https://stackoverflow.com/questions/16007137/properly-using-asynctask-get

public class SimpleDhtProvider extends ContentProvider {
    static final int SERVER_PORT = 10000;
    final String rootPort = "11108";
    String predPort = "null";
    String succPort = "null";
    String predPortHash = "null";
    String succPortHash = "null";
    final String[] AVD_PORTS = {"11108", "11112", "11116", "11120", "11124"};
    String myPort;
    String myPortHash;
    //porthashvalue:port
    TreeMap<String, String> nodeAndHashMap = new TreeMap<String, String>();
    ArrayList<String> nodeIdList = new ArrayList<String>();
    TreeMap<String, String> nodeIDLookup = new TreeMap<String, String>();

    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final String URI = "edu.buffalo.cse.cse486586.simpledht.provider";
    static final String JOIN = "JOIN";
    static final String INSERT = "INSERT";
    static final String PREDSUCCINFO = "PREDSUCCINFO";
    static final String DELETE = "DELETE";
    static final String QUERY = "QUERY";


    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        Log.e(TAG, "Inside OnCreate...");

        try {

            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            Log.e(TAG, "Server thread created...");


        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");

        }

        TelephonyManager tel = (TelephonyManager) getContext().getApplicationContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        try {
            nodeIDLookup.put(genHash("5554"),"5554");
            nodeIDLookup.put(genHash("5556"),"5556");
            nodeIDLookup.put(genHash("5558"),"5558");
            nodeIDLookup.put(genHash("5560"),"5560");
            nodeIDLookup.put(genHash("5562"),"5562");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        Log.e("ONCREATE",""+nodeIDLookup);


        try {
            myPortHash = genHash(portStr);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, JOIN, rootPort);

        return false;
    }



    public void delete_local_files() {

        String[] files = getContext().fileList();
        for (String filetoread : files) {
            Log.e("DELETE", "File to delete ****: " + filetoread);
            try {
                getContext().deleteFile(filetoread);
                Log.e("DELETE", "File DELETE SUCCESS: " + filetoread);
            } catch (Exception e) {
                Log.e("DELETE", "File DELETE FAILURE: " + filetoread);
                e.printStackTrace(); } } }

//    public void sendDelReq(){
//        Log.e("DELETE", "****INSIDE DELETE REQUEST**** ");
//        for (int i =0;i <=4;++i){
//            try {
//                if(!AVD_PORTS[i].equals(myPort))
//                {
//                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(AVD_PORTS[i]));
//                PrintWriter textOutput = new PrintWriter(socket.getOutputStream(),true);
//                String text = DELETE+"@"+myPort;
//                textOutput.println(text);
//                    Log.e("DELETE", "DELETE REQUEST SENT TO: " +AVD_PORTS[i]);
//                socket.close();
//                }
//            }
//            catch (Exception e) {
//                Log.e("DELETE", "Exception while sending DELETE req to "+ AVD_PORTS[i]);
//                e.printStackTrace();
//            }
//        }
//    }


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        Log.e("QUERY","****Inside delete****");
        // TODO Auto-generated method stub
        if ((predPortHash.equals("null") && succPortHash.equals("null"))||(predPortHash.compareTo(myPortHash) == 0 && succPortHash.compareTo(myPortHash) == 0)) {
            Log.e("QUERY","Only one AVD, so query them all");

            if(selection.equals("*")||selection.equals("@")){
                delete_local_files();
        }

            else{
                try {
                    getContext().deleteFile(selection);
                    Log.e("DELETE", "File DELETE SUCCESS: " + selection);
                } catch (Exception e) {
                    Log.e("DELETE", "File DELETE FAILURE: " + selection);
                    e.printStackTrace(); }
            }
        }

        else if(selection.equals("@")){
            Log.e("DELETE", "selection " + selection);
            delete_local_files();
        }

        else if(selection.equals("*")){
            Log.e("DELETE", "selection " + selection);
            delete_local_files();
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, DELETE, succPort,"!"+myPort);
        }

        else if(selection.substring(0,1).equals("!")){
            Log.e("DELETE", "selection " + selection);
            delete_local_files();
            if(!succPort.equals(selection.substring(1,selection.length())))
            {new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, DELETE, succPort,selection);}


        }
        else {
            Log.e("DELETE", "KEY DELECTION, KEY: " + selection);
            String key = selection;
            String hashedKey = null;
            try {
                hashedKey = genHash(key);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            // 2 avds
        if (predPortHash != null && succPortHash != null && predPortHash.compareTo(succPortHash) == 0) {
                Log.e("DELETE","Only two AVD");

                String low = null;
                String high = null;
                if (myPortHash.compareTo(succPortHash) < 0){
                    low = myPortHash;
                    high = succPortHash; }
                else{
                    low = succPortHash;
                    high = myPortHash; }

                if (hashedKey.compareTo(low) > 0 && hashedKey.compareTo(high) < 0) {
                    //insert high
                    if(high.compareTo(myPortHash) ==0){
                        Log.e("DELETE","here");
                        getContext().deleteFile(selection);
                    }
                    else{
                        Log.e("DELETE","here1");
                        Log.e("DELETE","FORWARD KEY TO SUCC PORT = ***** "+succPort);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, DELETE, succPort, key); } }

                else{
                    //insert small
                    if(low.compareTo(myPortHash) == 0){
                        Log.e("DELETE","here2");
                        getContext().deleteFile(selection);
                    }

                    else{
                        Log.e("DELETE","here1");
                        Log.e("INSERT","FORWARD KEY TO SUCC PORT = ***** "+succPort);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, DELETE, succPort, key);
                    } } }

            else if (hashedKey.compareTo(predPortHash) > 0 && hashedKey.compareTo(myPortHash) <= 0) {
                            Log.e("DELETE","here3");
                            getContext().deleteFile(selection);
            }
            else if (hashedKey.compareTo(predPortHash) > 0 && hashedKey.compareTo(myPortHash) > 0) {
                if (myPortHash.compareTo(predPortHash) < 0) {
                    Log.e("DELETE","here4");
                    getContext().deleteFile(selection);
                }
                else {
                    Log.e("DELETE","here5");
                    Log.e("DELETE","FORWARD KEY TO SUCC PORT = ***** "+succPort);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, DELETE, succPort, key);
                }
            }

            else if(hashedKey.compareTo(predPortHash) < 0 && hashedKey.compareTo(myPortHash) < 0){
                if (myPortHash.compareTo(predPortHash) < 0) {
                    Log.e("DELETE","here6");
                    getContext().deleteFile(selection);
                }
                else {
                    Log.e("DELETE","here7");
                    Log.e("DELETE","FORWARD KEY TO SUCC PORT = ***** "+succPort);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, DELETE, succPort, key);
                }

            }
            else {
                    Log.e("DELETE","here8");
                    Log.e("DELETE","FORWARD KEY TO SUCC PORT = ***** "+succPort);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, DELETE, succPort, key);
            }
        }

        //use only uri and selection params
        return 0;
    }


    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }


    public void insert_file_local(Uri uri, ContentValues values) {
        String key = values.getAsString("key");
        String value = values.getAsString("value");
        FileOutputStream outputStream;
        try {

            outputStream = getContext().getApplicationContext().openFileOutput(key, Context.MODE_PRIVATE);
            outputStream.write(value.getBytes());
            Log.e(TAG, "File write success in " + myPort);
            outputStream.close();
        } catch (Exception e) {
            Log.e(TAG, "File write failed in " + myPort);
        }
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub

        Log.e("INSERT","INSIDE INSERT ***** ");
        Log.e("INSERT","SUCC PORT HASH***** "+succPortHash);
        Log.e("INSERT","PRED PORT HASH ***** "+predPortHash);

        Log.e("INSERT","Inside Insert method");
        String key = values.getAsString("key");
        String value = values.getAsString("value");
//        FileOutputStream outputStream;

        Log.e("INSERT","Key: "+key+" Value: "+value);
        String hashedKey = "";
        try {

            hashedKey = genHash(key);
        } catch (NoSuchAlgorithmException e) {
        }

        Log.e("INSERT",key+" compare to "+hashedKey+" "+hashedKey.compareTo(myPortHash));

        Log.e("INSERT",key+" ONE AVD CHECK, predport--- "+predPortHash);
        Log.e("INSERT",key+" ONE AVD CHECK, succport--- "+succPortHash);
//        Log.e("INSERT",key+" ONE AVD CHECK, predport--- "+String.valueOf(predPortHash == null));
//        Log.e("INSERT",key+" ONE AVD CHECK, succport--- "+String.valueOf(succPortHash == null));
//        Log.e("INSERT",key+" ONE AVD CHECK, both--- "+String.valueOf(predPortHash == null && succPortHash == null));



        //only one avd
        if ((predPortHash.equals("null") && succPortHash.equals("null"))||(predPortHash.compareTo(myPortHash) == 0 && succPortHash.compareTo(myPortHash) == 0)){
            Log.e("INSERT","Only one AVD, so local insert");
            insert_file_local(uri, values);

        }

        // 2 avds
        else if (predPortHash != null && succPortHash != null && predPortHash.equals(succPortHash)) {
            Log.e("INSERT","Only two AVD");


            String low = null;
            String high = null;
            if (myPortHash.compareTo(succPortHash) < 0){
                low = myPortHash;
                high = succPortHash; }
            else{
                low = succPortHash;
                high = myPortHash; }

            if (hashedKey.compareTo(low) > 0 && hashedKey.compareTo(high) < 0) {
                //insert high
                if(high.compareTo(myPortHash) ==0){
                    insert_file_local(uri, values); }
                else{
                    Log.e("INSERT","FORWARD KEY TO SUCC PORT = ***** "+succPort);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, INSERT, succPort, key, value,myPort); } }

            else{
                //insert small
                if(low.compareTo(myPortHash) == 0){
                    insert_file_local(uri,values);
                }

                else{
                    Log.e("INSERT","FORWARD KEY TO SUCC PORT = ***** "+succPort);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, INSERT, succPort, key, value,myPort);
                } } }

        else if (hashedKey.compareTo(predPortHash) > 0 && hashedKey.compareTo(myPortHash) <= 0) {
            insert_file_local(uri, values);
            Log.e("INSERT","here1");
        }
        else if (hashedKey.compareTo(predPortHash) > 0 && hashedKey.compareTo(myPortHash) > 0) {
            if (myPortHash.compareTo(predPortHash) < 0) {
                insert_file_local(uri, values);
                Log.e("INSERT","here2");
            }
            else {
                Log.e("INSERT","here3");
                Log.e("INSERT","FORWARD KEY TO SUCC PORT = ***** "+succPort);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, INSERT, succPort, key, value,myPort);
            }
        }

        else if(hashedKey.compareTo(predPortHash) < 0 && hashedKey.compareTo(myPortHash) < 0){
            if (myPortHash.compareTo(predPortHash) < 0) {
                insert_file_local(uri, values);
                Log.e("INSERT","here4");
            }
            else {
                Log.e("INSERT","here5");
                Log.e("INSERT","FORWARD KEY TO SUCC PORT = ***** "+succPort);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, INSERT, succPort, key, value,myPort);
            }

        }
        else {
            Log.e("INSERT","here6");
            Log.e("INSERT","FORWARD KEY TO SUCC PORT = ***** "+succPort);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, INSERT, succPort, key, value,myPort);
        }


//        else if(hashedKey.compareTo(myPortHash) > 0 && hashedKey.compareTo(predPortHash) <= 0){
//            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, INSERT, succPort);
//        }
//
//        else if(hashedKey.compareTo(predPortHash) > 0 && hashedKey.compareTo(succPortHash) > 0){
//            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, INSERT, succPort);
//        }
//
//        else if(hashedKey.compareTo(predPortHash) < 0 && hashedKey.compareTo(succPortHash) >= 0){
//            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, INSERT, succPort);
//        }
//
//
//
//
    return uri;
    }



    public Cursor query_local_key(String selection) {
        Log.e("QUERY","*****INSIDE LOCAL KEY-WISE QUERY****");

        String filetoread = selection;
        Log.e("QUERY","FILE TO QUERY: "+filetoread);

        try{

            FileInputStream fileInput = getContext().getApplicationContext().openFileInput(filetoread);
            BufferedReader br = new BufferedReader(new InputStreamReader(fileInput));

            String[] row = new String[2];
            row[0] = selection;
            row[1] = br.readLine();
            String[] columns = {"key","value"};
            MatrixCursor mc = new MatrixCursor(columns);
            mc.addRow(row);
            Log.e("QUERY","query success");
            Log.e("QUERY","key: "+row[0]+" value: "+row[1]);
            return mc;
        }
        catch (Exception e){
            Log.e("QUERY","ERROR WHILE LOCAL QUERY");
            e.printStackTrace();

        }
        return null;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub
        Log.e("QUERY","******INSIDE QUERY******");
        Log.e("QUERY","******SELECTION: "+selection);

        if ((predPortHash.equals("null") && succPortHash.equals("null"))||(predPortHash.compareTo(myPortHash) == 0 && succPortHash.compareTo(myPortHash) == 0)) {
            Log.e("QUERY","Only one AVD, so query them all");

            if(selection.equals("*")||selection.equals("@")){

                Log.e("QUERY","selection parameter: "+selection+" <<<<<");

                String[] columns = {"key","value"};
                MatrixCursor mc = new MatrixCursor(columns);
//                Cursor resultCursor = getContext().getContentResolver().query(uri, null, selection, null, null);
                String[] files = getContext().fileList();
                for(String filetoread:files){
                    Log.e("QUERY","File name: "+filetoread);
                    try {
                        FileInputStream fileInput = getContext().getApplicationContext().openFileInput(filetoread);
                        BufferedReader br = new BufferedReader(new InputStreamReader(fileInput));
                        String[] row = new String[2];
                        row[0] = filetoread;
                        row[1] = br.readLine();
                        Log.e("QUERY","****mc is"+mc+" key: "+row[0]+" value: "+row[1]);
                        mc.addRow(row);

                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }


                }
                return mc;

                }

            else {
                return query_local_key(selection);
            }

            }

        else if(selection.equals("@")){
            Log.e("QUERY","selection parameter: "+selection+"  <<<<<");

            String[] columns = {"key","value"};
            MatrixCursor mc = new MatrixCursor(columns);
//                Cursor resultCursor = getContext().getContentResolver().query(uri, null, selection, null, null);
            String[] files = getContext().fileList();
            for(String filetoread:files){
                Log.e("QUERY","File name: "+filetoread);
                try {
                    FileInputStream fileInput = getContext().getApplicationContext().openFileInput(filetoread);
                    BufferedReader br = new BufferedReader(new InputStreamReader(fileInput));
                    String[] row = new String[2];
                    row[0] = filetoread;
                    row[1] = br.readLine();
                    Log.e("QUERY","****mc is"+mc+" key: "+row[0]+" value: "+row[1]);
                    mc.addRow(row);

                }
                catch (Exception e) {
                    e.printStackTrace();
                    Log.e("QUERY","ERROR WHILE QUERYING "+filetoread);
                }


            }
            return mc;
        }

        else if(selection.equals("*")){
            Log.e("QUERY","selection parameter: "+selection+"  <<<<<");

            String[] columns = {"key","value"};
            MatrixCursor mc = new MatrixCursor(columns);
            String[] files = getContext().fileList();
            for(String filetoread:files){
                Log.e("QUERY","File name: "+filetoread);
                try {
                    FileInputStream fileInput = getContext().getApplicationContext().openFileInput(filetoread);
                    BufferedReader br = new BufferedReader(new InputStreamReader(fileInput));
                    String[] row = new String[2];
                    row[0] = filetoread;
                    row[1] = br.readLine();
                    Log.e("QUERY","****mc is"+mc+" key: "+row[0]+" value: "+row[1]);
                    mc.addRow(row);

                }
                catch (Exception e) {
                    e.printStackTrace();
                }

            }

            for (String i : AVD_PORTS){
                try {
                    if(!i.equals(myPort))
                    {
                        String response = new queryClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, QUERY, i,"@").get();
                        JSONObject js = new JSONObject(response);
                        Iterator<String> keyiter= js.keys();
                        while (keyiter.hasNext()){
                            String key = keyiter.next();
                            try {
                                String[] row = new String[2];
                                String value = js.getString(key);
                                row[0] = key;
                                row[1] = value;
                                mc.addRow(row);
                            } catch (JSONException e) {
                                Log.e("QUERY", "EXCEPTION PARSING JSON Object");
                            }
                        }

                    }
                }
                catch (Exception e) {
                    Log.e("QUERY", "Exception while sending QUERY req to "+ i);
                    e.printStackTrace();
                }
            }
            return mc;




        }

//        else if(selection.substring(0,1).equals("!")){
//            delete_local_files();
//            if(!succPort.equals(selection.substring(1,selection.length()))) {
//                new queryClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, QUERY, succPort, selection);
//            }}


        else {
            String[] columns = {"key","value"};
            MatrixCursor mc = new MatrixCursor(columns);

            Log.e("QUERY", "KEY: " + selection);
//            String key = selection;
            String hashedKey = null;
            try {
                hashedKey = genHash(selection);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            // 2 avds
            if (predPortHash != null && succPortHash != null && predPortHash.compareTo(succPortHash) == 0) {
                Log.e("QUERY","Only two AVD");

                String low = null;
                String high = null;
                if (myPortHash.compareTo(succPortHash) < 0){
                    low = myPortHash;
                    high = succPortHash; }
                else{
                    low = succPortHash;
                    high = myPortHash; }

                if (hashedKey.compareTo(low) > 0 && hashedKey.compareTo(high) < 0) {
                    //insert high
                    if(high.compareTo(myPortHash) ==0){
                        Log.e("QUERY","here");
                        return query_local_key(selection);
                    }
                    else{
                        Log.e("QUERY","here1");
                        Log.e("QUERY","FORWARD KEY TO SUCC PORT = ***** "+succPort);
//                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, DELETE, succPort, key);
                        try {
                            String response = new queryClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,QUERY,succPort,selection).get();
                            JSONObject js = new JSONObject(response);
                            Iterator<String> keyiter= js.keys();
                            while (keyiter.hasNext()){
                                String key = keyiter.next();
                                try {
                                    String[] row = new String[2];
                                    String value = js.getString(key);
                                    row[0] = key;
                                    row[1] = value;
                                    mc.addRow(row);
                                } catch (JSONException e) {
                                    Log.e("QUERY", "EXCEPTION PARSING JSON Object. here1"); } }
                            return mc;

                        } catch (Exception e) {
                            Log.e("QUERY", "here1 Exception while sending QUERY req to "+ succPort);
                            e.printStackTrace();
                        } }}

                else{
                    //insert small
                    if(low.compareTo(myPortHash) == 0){
                        Log.e("QUERY","here2");
                        return query_local_key(selection);
                    }

                    else{
                        Log.e("QUERY","here3");
                        Log.e("QUERY","FORWARD KEY TO SUCC PORT = ***** "+succPort);
//                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, DELETE, succPort, key);
                        try {
                            String response = new queryClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,QUERY,succPort,selection).get();
                            JSONObject js = new JSONObject(response);
                            Iterator<String> keyiter= js.keys();
                            while (keyiter.hasNext()){
                                String key = keyiter.next();
                                try {
                                    String[] row = new String[2];
                                    String value = js.getString(key);
                                    row[0] = key;
                                    row[1] = value;
                                    mc.addRow(row);
                                } catch (JSONException e) {
                                    Log.e("QUERY", "EXCEPTION PARSING JSON Object. here1"); } }
                            return mc;

                        } catch (Exception e) {
                            Log.e("QUERY", "here3 Exception while sending QUERY req to "+ succPort);
                            e.printStackTrace();
                        }
                    } } }

            else if (hashedKey.compareTo(predPortHash) > 0 && hashedKey.compareTo(myPortHash) <= 0) {
                Log.e("QUERY","here4");
                return query_local_key(selection);
            }
            else if (hashedKey.compareTo(predPortHash) > 0 && hashedKey.compareTo(myPortHash) > 0) {
                if (myPortHash.compareTo(predPortHash) < 0) {
                    Log.e("QUERY","here5");
                    return query_local_key(selection);
                }
                else {
                    Log.e("QUERY","here6");
                    Log.e("QUERY","FORWARD KEY TO SUCC PORT = ***** "+succPort);
//                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, DELETE, succPort, key);
                    try {
                        String response = new queryClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,QUERY,succPort,selection).get();
                        JSONObject js = new JSONObject(response);
                        Iterator<String> keyiter= js.keys();
                        while (keyiter.hasNext()){
                            String key = keyiter.next();
                            try {
                                String[] row = new String[2];
                                String value = js.getString(key);
                                row[0] = key;
                                row[1] = value;
                                mc.addRow(row);
                            } catch (JSONException e) {
                                Log.e("QUERY", "EXCEPTION PARSING JSON Object. here6"); } }
                        return mc;

                    } catch (Exception e) {
                        Log.e("QUERY", "here6 Exception while sending QUERY req to "+ succPort);
                        e.printStackTrace();
                    }
                }
            }

            else if(hashedKey.compareTo(predPortHash) < 0 && hashedKey.compareTo(myPortHash) < 0){
                if (myPortHash.compareTo(predPortHash) < 0) {
                    Log.e("QUERY","here7");
                    return query_local_key(selection);
                }
                else {
                    Log.e("QUERY","here8");
                    Log.e("QUERY","FORWARD KEY TO SUCC PORT = ***** "+succPort);
//                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, DELETE, succPort, key);
                    try {
                        String response = new queryClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,QUERY,succPort,selection).get();
                        JSONObject js = new JSONObject(response);
                        Iterator<String> keyiter= js.keys();
                        while (keyiter.hasNext()){
                            String key = keyiter.next();
                            try {
                                String[] row = new String[2];
                                String value = js.getString(key);
                                row[0] = key;
                                row[1] = value;
                                mc.addRow(row);
                            } catch (JSONException e) {
                                Log.e("QUERY", "EXCEPTION PARSING JSON Object. here8"); } }
                        return mc;

                    } catch (Exception e) {
                        Log.e("QUERY", "here8 Exception while sending QUERY req to "+ succPort);
                        e.printStackTrace();
                    }
                }

            }
            else {
                Log.e("QUERY","here9");
                Log.e("QUERY","FORWARD KEY TO SUCC PORT = ***** "+succPort);
                try {
                    String response = new queryClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,QUERY,succPort,selection).get();
                    JSONObject js = new JSONObject(response);
                    Iterator<String> keyiter= js.keys();
                    while (keyiter.hasNext()){
                        String key = keyiter.next();
                        try {
                            String[] row = new String[2];
                            String value = js.getString(key);
                            row[0] = key;
                            row[1] = value;
                            mc.addRow(row);
                        } catch (JSONException e) {
                            Log.e("QUERY", "EXCEPTION PARSING JSON Object. here9"); } }
                    return mc;

                } catch (Exception e) {
                    Log.e("QUERY", "here9 Exception while sending QUERY req to "+ succPort);
                    e.printStackTrace();
                }
            }
        }

        return null;
    }









    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
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




    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            Log.e(TAG, "*** INSIDE ServerTask METHOD ***");
            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */
            try{

                while (true) {
                    Log.e(TAG, "*** WAITING TO ACCEPT ***");
                        Socket socket = serverSocket.accept();
                        Log.e("ST", "***ServerTask --------Connection accepted by server***");
                        BufferedReader brIncomingMsg = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        PrintWriter outputWriter = new PrintWriter(socket.getOutputStream(), true);
                        String incomingString = brIncomingMsg.readLine();
                    try {

                        String[] a = incomingString.split("@");
                        String requestType = a[0];
                        Log.e("ST", "***request PAYLOAD*** " + incomingString);
                        Log.e("ST", "***request type IN SERVER*** " + requestType);


                        if (requestType.equals(JOIN)) {
                            String fromPort = a[1];
                            int port = Integer.parseInt(fromPort);
                            Log.e("ST", "JOIN received in SERVER TASK FROM " + fromPort);
                            String hashVal = genHash(String.valueOf(port / 2));
                            nodeAndHashMap.put(hashVal, String.valueOf(port));//(hash(11108/2),"11108")

                            Log.e("ST", "nodeAndHashMap->>>" + nodeAndHashMap);
                            nodeIdList.clear();
                            for (String key : nodeAndHashMap.keySet()) {
                                nodeIdList.add(key);
                            }
                            Log.e("ST", "nodeIdList->>>" + nodeIdList);
                            int nodeIdPos = nodeIdList.indexOf(hashVal);


//update pred and succ values for every node join

                            for (int i = 0; i <= nodeIdList.size() - 1; i++) {

                                if (nodeIdList.size() == 1) {
                                    Log.e("ST", "Only one avd, so pred=succ=null");
                                    predPortHash = "null";
                                    succPortHash = "null";

                                } else {
                                    Log.e("ST", "***" + nodeIdList.size() + " AVDS and i is " + i);
                                    if (i == 0) {
                                        predPortHash = nodeIdList.get(nodeIdList.size() - 1);
                                        succPortHash = nodeIdList.get(i + 1);

                                    } else if (i == nodeIdList.size() - 1) {
                                        predPortHash = nodeIdList.get(i - 1);
                                        succPortHash = nodeIdList.get(0);

                                    } else {
                                        predPortHash = nodeIdList.get(i - 1);
                                        succPortHash = nodeIdList.get(i + 1);
                                    }
                                }
                                String portToSendTo = nodeAndHashMap.get(nodeIdList.get(i));


                                if (predPortHash != null && succPortHash != null) {


                                    String predsuccinfo = "PREDSUCCINFO@" + portToSendTo + "@" + predPortHash + "@" + nodeAndHashMap.get(predPortHash) + "@" + succPortHash + "@" + nodeAndHashMap.get(succPortHash);
                                    Log.e("ST", "NODEINFO TO CLIENT--> hasval@portno@pred@succ: " + predsuccinfo);
                                    publishProgress(predsuccinfo);
                                    Log.e("ST", "PRED AND SUCC UPDATED VALUES SENT TO " + portToSendTo);
                                } else {
                                    String predsuccinfo = "PREDSUCCINFO@" + portToSendTo + "@" + predPortHash + "@" + "null" + "@" + succPortHash + "@" + "null";
                                    Log.e("ST", "NODEINFO TO CLIENT--> hasval@portno@pred@succ: " + predsuccinfo);
                                    publishProgress(predsuccinfo);
                                    Log.e("ST", "PRED AND SUCC UPDATED VALUES SENT TO " + portToSendTo);
                                }

                            }

                        } else if (requestType.equals(PREDSUCCINFO)) {
//                        String textToSend = requestType + "@" + toPort + "@" + predHash + "@" + pPort+"@"+succHash+"@"+sPort;
                            String aboutPort = a[1];
                            Log.e("ST", "*****PREDSUCCINFO REQUEST RECEIVED IN SERVER TASK IN***** " + myPort + " about " + aboutPort);
                            predPortHash = a[2];
                            predPort = a[3];
                            succPortHash = a[4];
                            succPort = a[5];
                            Log.e("ST", "UPDATED PREDSUCCINFO OF " + myPort + "-> predPort: " + predPort + "succPort: " + succPort);


                        } else if (requestType.equals(INSERT)) {
                            Log.e("ST", "*****INSERT REQUEST RECEIVED IN SERVER TASK IN***** " + myPort);

//                        String textToSend =requestType+"@"+toPort+"@"+key+"@"+value;
                            Uri uri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledht.provider”");
                            ContentValues cv = new ContentValues();
                            cv.put("key", a[2]);
                            cv.put("value", a[3]);
                            insert(uri, cv);


                        }

                        else if (requestType.equals(DELETE)){
//                            String textToSend = requestType + "@" + toPort + "@" + selection;
                            Uri uri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledht.provider”");
                            String selection = a[2];
                            delete(uri,selection,null);
                            }

                        else if (requestType.equals(QUERY)){
                            //                        QUERY+"@"+selection;

                            Uri uri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledht.provider”");
                            String fromPort = a[1];
                            String selection = incomingString.substring(12,incomingString.length());
                            Cursor cursor = query(uri,null,selection,null,null);
                            JSONObject data = new JSONObject();
//                            DatabaseUtils.dumpCursor(cursor);
                            if (cursor !=null && cursor.moveToFirst()){
                                while(!cursor.isAfterLast()){
                                    String key = cursor.getString(cursor.getColumnIndex("key"));
                                    String value = cursor.getString(cursor.getColumnIndex("value"));
                                    data.put(key,value);
                                    cursor.moveToNext();
                                }
                            }
                            DataOutputStream cout = new DataOutputStream(socket.getOutputStream());
                            cout.writeUTF(data.toString());
                            cout.flush();
                            cursor.close();


                        }



                    socket.close();
                    } catch (NullPointerException e) {
                        Log.e("ST","NullPointer Exception while read line from Client");
                        continue;
                    }


                }
            }
            catch(Exception e){
                e.printStackTrace();
                Log.e("ST", "Error while socket connection in Server ",e);
            }
            return null;
        }


        protected void onProgressUpdate(String...strings) {
//            String predsuccinfo = "PREDSUCCINFO@" + portToSendTo + "@" + predPortHash +"@"+nodeAndHashMap.get(predPortHash)+ "@" + succPortHash+"@"+nodeAndHashMap.get(succPortHash);
            String ss[] = strings[0].split("@");
            String portToSendTo = ss[1];
            String predHash = ss[2];
            String pPort = ss[3];
            String succHash = ss[4];
            String sPort = ss[5];
            Log.e("ST", "ONPROGRESSUPDATE********** "+portToSendTo+"@"+pPort+"@"+sPort);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, PREDSUCCINFO, portToSendTo,predHash,pPort,succHash,sPort);



            return;
        }
    }



    private  class queryClientTask extends AsyncTask<String,Void, String>{

        @Override
        protected String doInBackground(String... msgs){

//            new queryClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,QUERY,succPort,selection);
            Log.e("QCT", "******INSIDE QUERY CLIENT TASK****** ");
            String requestType = msgs[0];
            String toPort = msgs[1];
            String selection = msgs[2];
            Log.e("QCT", "Request Type: " + requestType);
            Log.e("QCT", " TO: " + toPort+"**");
            Log.e("QCT", " FROM: " + myPort);
            Log.e("QCT", " selection: " + selection);

            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(toPort));
                socket.setSoTimeout(3000);
                PrintWriter outmessage = new PrintWriter(socket.getOutputStream(),true);
                String textToSend = QUERY+"@"+myPort+"@"+selection;
                outmessage.println(textToSend);
                DataInputStream brIncomingMsg = new DataInputStream(socket.getInputStream());
                String response = brIncomingMsg.readUTF();
                Log.e("QCT", "******RESPONSE ****** "+response);
                return response;


            }
            catch (SocketException e) {
                e.printStackTrace();
                return null;
            }
            catch (IOException e){
                e.printStackTrace();
                return null;
            }

        }
    }



    private class ClientTask extends AsyncTask<String, Void, Void> {


        @Override
        protected Void doInBackground(String... msgs) {
            try {
                String requestType = msgs[0];
                String toPort = msgs[1];
                Log.e("CT", "Request Type: " + requestType);
                Log.e("CT", " TO: " + toPort+"**");
                Log.e("CT", " FROM: " + myPort);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(toPort));
                socket.setSoTimeout(3000);
                PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader brIncomingMsg = new BufferedReader(new InputStreamReader(socket.getInputStream()));
//                String msgfromServer = brIncomingMsg.readLine();
//                if(msgfromServer!=null){
//                Log.e("CT", "msgfromServer: " + msgfromServer);}


                //                    Reading from server

//                    String proposal = "PREDSUCCINFO@" +hashVal +"@"+ port/2 + "@" + predPort + "@" + succPort;
//                String ss[] = msgfromServer.split("@");
//                String responseType = ss[0];
//                Log.e("CT","****response-type**** "+responseType);

                if (requestType.equals(JOIN)) {
                    try {
                        Log.e("CT", "INSIDE JOIN request");
//              #Sending request to join in the chord ring
                        String textToSend = requestType + "@" + myPort;
                        Log.e("CT", "" + textToSend + " from " + myPort);
                        pw.println(textToSend);
                        pw.flush();
                        Log.e("CT", "JOIN request SENT");
                    } catch (NullPointerException e) {
                        Log.e("CT", "Null pointer while JOIN in client task in " + myPort);
                    }

                } else if (requestType.equals(INSERT)) {
                    try {

//                        (AsyncTask.SERIAL_EXECUTOR, INSERT, succPort,key,value)
                        String key = msgs[2];
                        String value = msgs[3];
                        String originPort = msgs[4];
                        String textToSend = requestType + "@" + toPort + "@" + key + "@" + value+"@"+originPort;
                        Log.e("CT", "" + textToSend);
                        pw.println(textToSend);
                        pw.flush();
                        Log.e("CT", "INSERT request SENT");
                    } catch (Exception e) {
                        Log.e("CT", "Exception while INSERT in client task" + myPort);
                    }
                }


             else if (requestType.equals(PREDSUCCINFO)) {
//                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, PREDSUCCINFO, portToSendTo,predHash,predPort,succHash,succPort);
                    String predHash = msgs[2];
                    String pPort = msgs[3];
                    String succHash = msgs[4];
                    String sPort = msgs[5];
                    String textToSend = requestType + "@" + toPort + "@" + predHash + "@" + pPort+"@"+succHash+"@"+sPort;
                    Log.e("CT", "" + textToSend);
                    pw.println(textToSend);
                    pw.flush();
                    Log.e("CT", "PREDSUCCINFO SENDING FROM "+myPort+" TO " + toPort + "-> predPort: " + pPort + "succPort: " + sPort);

                }


             else if(requestType.equals(DELETE)){
                 String selection =msgs[2];
                    String textToSend = requestType + "@" + toPort + "@" + selection;
                    Log.e("CT", "" + textToSend);
                    pw.println(textToSend);
                    pw.flush();
                    Log.e("CT", "DELETE SENDING FROM "+myPort+" TO " + toPort+ "AND SELECTION: "+selection);
                }

            } catch (Exception e) {
                Log.e("CT", "ClientTask Exception in " + myPort);
                    e.printStackTrace();
            }


            return null;


        }

    }
}





