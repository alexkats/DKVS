package com.katsman.dkvs.server;

import com.katsman.dkvs.common.utils.ParseException;
import com.katsman.dkvs.server.utils.Configuration;
import com.katsman.dkvs.server.workers.Server;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * @author Alexey Katsman
 * @since 15.06.16
 */

public class Main {
    private static final String fileName = "dkvs.properties";

    public static void main(String[] args) {
        String s;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(System.in))) {
            s = br.readLine();

            if (s == null || s.equals("")) {
                try {
                    new Server(new Configuration(fileName, 0)).start();
                    new Server(new Configuration(fileName, 1)).start();
                    new Server(new Configuration(fileName, 2)).start();
                    new Server(new Configuration(fileName, 3)).start();
                    new Server(new Configuration(fileName, 4)).start();
                } catch (ParseException | FileNotFoundException e) {
                    System.out.println("Can't read properties file: " + e.getMessage());
                } catch (IOException e) {
                    System.out.println("Can't initialize!");
                }
            } else {
                try {
                    new Server(new Configuration(fileName, Integer.parseInt(s))).start();
                } catch (NumberFormatException e) {
                    System.out.println("Can't read node number!");
                } catch (ParseException | FileNotFoundException e) {
                    System.out.println("Can't read properties file: " + e.getMessage());
                } catch (IOException e) {
                    System.out.println("Can't initialize!");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
