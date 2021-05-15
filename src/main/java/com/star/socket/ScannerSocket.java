package com.star.socket;

import java.io.*;
import java.net.Socket;

public class ScannerSocket {

    public static void main(String[] args) {

        try (Socket socket = new Socket("10.191.107.42", 7777);
             BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
             BufferedReader br = new BufferedReader(new InputStreamReader(System.in));){
            socket.setKeepAlive(true);
            String line=null;
            while ((line=br.readLine())!=null){
                bw.write(line);
                bw.flush();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
