import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;

public class Dstore {

  private static ArrayList<String> files;


  public static void main(String[] args) throws Exception {

    final int port = Integer.parseInt(args[0]); //accessed by client for sending files
    final int cport = Integer.parseInt(args[1]); //for communication with controller
    int timeout = Integer.parseInt(args[2]);
    String file_folder = String.valueOf(args[3]);

    files = new ArrayList<>();

    try {

      //socket to speak with controller. We know the address and port we want to connect to.
      Socket socketCon = new Socket(InetAddress.getLocalHost(), cport);
      PrintWriter outCon = new PrintWriter(socketCon.getOutputStream()); //to send
      BufferedReader inCon = new BufferedReader(new InputStreamReader(socketCon.getInputStream())); //to receive


      //new thread for controller
      new Thread(new Runnable() {
        public void run() {
          try {

            //Join controller
            String message = Protocol.JOIN_TOKEN + " " + port ;
            outCon.println(message);
            outCon.flush();
            System.out.println("Dstore message sent:: " + message);

            //set up empty file folder. Different name to any other.
            File directory = new File(file_folder);
            try {

              if (!directory.exists()) {
                directory.mkdir();
                System.out.println("Directory created");
              } else {
                System.out.println("Directory already exits. Remove all its contents");
                File[] files = directory.listFiles();
                if (files!=null) {
                  for (File file : files) {
                    file.delete();
                  }
                }
              }

            } catch (Exception e) {
              System.out.println("Error:: directory creation failed");
            }


            for (;;) {
              String line = inCon.readLine();

              switch (getCommand(line)) {
                case Protocol.REMOVE_TOKEN:

                  System.out.println("Received from controller socket: " + line);

                  String filenameR = getFirstOption(line);
                  String ackMessage;

                  //remove
                  File removeFile = new File(file_folder + File.separator + filenameR);
                  try {
                    if (removeFile.exists()) {
                      removeFile.delete();
                      if (files.contains(filenameR)) {
                        files.remove(filenameR);
                      }
                      ackMessage = Protocol.REMOVE_ACK_TOKEN + " " + filenameR;
                      outCon.println(ackMessage);
                      outCon.flush();
                      System.out.println("Removed: " + filenameR);
                    } else {
                      ackMessage = Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + filenameR;
                      outCon.println(ackMessage);
                      outCon.flush();
                      System.out.println("Doesn't have: " + filenameR);
                    }

                  } catch (Exception e) {
                    System.out.println("Remove failed: " + filenameR);
                  }

                  break;

                case Protocol.LIST_TOKEN:
                  System.out.println("Received from controller: " + line);

                  String response = Protocol.LIST_TOKEN;
                  for (String file : files) {
                    response += " " + file;
                  }
                  System.out.println("Sending: " + response);
                  outCon.println(response);
                  outCon.flush();

                  break;

                case Protocol.REBALANCE_TOKEN:
                  System.out.println("Received from controller " + port + ": (" + line + ")");

                  String arguments = removeCommand(line);
                  System.out.println("(" + arguments + ")");

                  if (arguments.equals(" ")) {
                    System.out.println("Nothing to do");
                    //Send ACK
                    outCon.println(Protocol.REBALANCE_COMPLETE_TOKEN);
                    outCon.flush();
                    break;
                  }

                  //Do Rebalance
                  //(1 test3.txt 1 4003 1 test3.txt)
                  ArrayList<String> info = new ArrayList<>(
                      Arrays.asList(arguments.split(" ")));
                  ArrayList<String> sendTo = new ArrayList<>();
                  ArrayList<String> remove = new ArrayList<>();
                  int splitIndex;
                  for (int i = info.size() - 1; i >= 0; i--) {
                    System.out.println(info.get(i));
                    if (!info.get(i).contains(".") && i != info.size() - 1 && i != 0) {
                      //is not file, is the count to split at
                      splitIndex = i; //index remove list starts at

                      for (int j = 0; j < splitIndex; j++) {
                        sendTo.add(info.get(j));
                      }
                      for (int j = splitIndex; j < info.size(); j++) {
                        remove.add(info.get(j));
                      }

                      i = -1;
                    } else if (!info.get(i).contains(".") && i == info.size() - 1) {
                      //none to remove
                      for (int j = 0; j < info.size(); j++) {
                        sendTo.add(info.get(j));
                      }

                      i = -1;
                    } else if ((!info.get(i).contains(".") && i == 0)) {
                      // none to send
                      for (int j = 0; j < info.size(); j++) {
                        remove.add(info.get(j));
                      }

                      i = -1;
                    }
                  }

                  //now have sendTo and remove list

                  // 3 text1.txt 2 4001 4002 text2.txt 1 4001 text2.txt 1 4002
                  if (sendTo.size() != 0) {

                    int nextFile = 1;
                    for (int i = 0; i < Integer.parseInt(sendTo.get(0)); i++) {
                      //for each file

                      String nameS = sendTo.get(nextFile);
                      int noDstore = Integer.parseInt(sendTo.get(nextFile + 1));
                      int start = nextFile + 2;
                      nextFile = start + noDstore;

                      if (!files.contains(nameS)) {
                        System.out.println(
                            "for rebalance sent, dstore does not have " + nameS);
                      } else {

                        for (int j = start; j < nextFile; j++) {
                          //for each dstore
                          try {
                            System.out.println("sending " + nameS);

                            //set up socket
                            Socket socketR = new Socket(InetAddress.getLocalHost(),
                                Integer.parseInt(sendTo.get(j)));
                            PrintWriter outR = new PrintWriter(
                                socketR.getOutputStream()); //to send
                            BufferedReader inR = new BufferedReader(
                                new InputStreamReader(socketR.getInputStream())); //to receive

                            //set up file
                            File inputFile = new File(file_folder + File.separator + nameS);
                            int fileLength = (int) inputFile.length();
                            byte[] buf = new byte[fileLength];
                            FileInputStream inf = new FileInputStream(inputFile);
                            int buflen;
                            buflen = inf.read(buf);

                            //send message
                            outR.println(Protocol.REBALANCE_STORE_TOKEN + " " + nameS + " "
                                + fileLength);
                            outR.flush();
                            socketR.setSoTimeout(timeout);
                            String ack = inR.readLine();

                            OutputStream outFR = socketR.getOutputStream();
                            outFR.write(buf);
                            inf.close();
                            outFR.close();

                            System.out.println("Sent " + nameS);

                            socketR.close();
                          } catch (SocketTimeoutException e) {
                            System.out.println("Timeout");
                          } catch (Exception e) {
                            System.out.println("Send failed: " + nameS);
                          }

                        }
                      }
                    }
                  }

                  //3 txt1 txt2 txt3
                  if (remove.size() != 0) {

                    for (int i = 0; i < Integer.parseInt(remove.get(0)); i++) {
                      String nameF = remove.get(i + 1);

                      //remove
                      File removeF = new File(file_folder + File.separator + nameF);
                      try {
                        if (removeF.exists()) {
                          removeF.delete();
                          if (files.contains(nameF)) {
                            files.remove(nameF);
                          }
                          System.out.println("Removed for rebalance: " + nameF);
                        }
                      } catch (Exception e) {
                        System.out.println("Remove failed: " + nameF);
                      }

                    }
                  }

                  //Send ACK
                  outCon.println(Protocol.REBALANCE_COMPLETE_TOKEN);
                  outCon.flush();

                  break;


              }
            }

          } catch (Exception e) {}
        }
      }).start();



      //connecting to clients
      try {
        //socket that waits for requests to come in
        ServerSocket ssClient = new ServerSocket(port);
        for (;;) {
          try {
            System.out.println("waiting for connection");
            //waits here until something connects
            Socket socketClient = ssClient.accept();
            System.out.println("connected");

            //new thread for each client.
            new Thread(new Runnable() {
              public void run() {
                try {
                  PrintWriter outClient = new PrintWriter(socketClient.getOutputStream()); //to send
                  BufferedReader inClient = new BufferedReader(new InputStreamReader(socketClient.getInputStream())); //to receive


                  String line = inClient.readLine();
                  String from = String.valueOf(socketClient.getPort());

                  switch (getCommand(line)) {
                    case Protocol.STORE_TOKEN:
                      System.out.println("Received " + from + ": " + line);

                      String filename = getFirstOption(line);
                      Integer filesize = Integer.parseInt(getSecondOption(line));

                      //send ack
                      outClient.println(Protocol.ACK_TOKEN);
                      outClient.flush();

                      //get content and store
                      try {
                        System.out.println("Begin store");
                        InputStream inF = socketClient.getInputStream();
                        byte[] buf = new byte[filesize];
                        int buflen;
                        buflen = inF.read(buf);
                        File outputFile = new File(file_folder + File.separator + filename);
                        FileOutputStream outF = new FileOutputStream(outputFile);
                        outF.write(buf);
                        inF.close();
                        outF.close();
                        System.out.println("Store Complete: " + filename + " " + port);

                        files.add(filename);
                        //ACK to controller
                        System.out.println("Store ack to controller " + filename + " " + port);
                        outCon.println(Protocol.STORE_ACK_TOKEN + " " + filename);
                        outCon.flush();

                      } catch (Exception e) {
                        System.out.println("Issue: storing file " + filename + " at " + port);
                      }

                      break;

                    case Protocol.LOAD_DATA_TOKEN:
                      System.out.println("Received " + port + ": " + line);

                      String filenameL = getFirstOption(line);
                      if (!files.contains(filenameL)) {
                        System.out.println("Dstore " + port + " does not have " + filenameL);
                        socketClient.close();
                      } else {

                        System.out.println("Load " + filenameL + " from dstore " + port);
                        try {
                          File inputFile = new File(file_folder + File.separator + filenameL);

                          byte[] buf = new byte[(int) inputFile.length()];
                          FileInputStream inf = new FileInputStream(inputFile);
                          OutputStream outFClient = socketClient.getOutputStream();
                          int buflen;
                          buflen = inf.read(buf);
                          outFClient.write(buf);
                          inf.close();
                          outFClient.close();

                          System.out.println("Loaded: " + filenameL);
                        } catch (Exception e) {
                          System.out.println("Load failed: " + filenameL);
                        }

                      }

                      break;

                    case Protocol.LIST_TOKEN:

                        /*System.out.println("Received " + port + ": " + line);

                        String response = Protocol.LIST_TOKEN;
                        for (String file : files) {
                          response += " " + file;
                        }
                        outClient.println(response);
                        outClient.flush();

                        break;*/

                    case Protocol.REBALANCE_STORE_TOKEN:
                      /*
                         Dstore i -> Dstore j: REBALANCE_STORE filename filesize
                         Dstore j -> Dstore i: ACK
                         Dstore i -> Dstore j: file_content
                         */

                      System.out.println("Received: " + line);
                      String filenameSR = getFirstOption(line);
                      Integer filesizeSR = Integer.parseInt(getSecondOption(line));
                      System.out.println(filenameSR);
                      System.out.println(filesizeSR);

                      //send ack
                      outClient.println(Protocol.ACK_TOKEN);
                      outClient.flush();

                      //get content and store
                      try {
                        System.out.println("Begin rebalance store");
                        InputStream inFR = socketClient.getInputStream();
                        System.out.println("1");
                        byte[] buf = new byte[filesizeSR];
                        System.out.println("2");
                        int buflen;
                        buflen = inFR.read(buf);
                        System.out.println("3");
                        File outputFileR = new File(file_folder + File.separator + filenameSR);
                        System.out.println("4");
                        FileOutputStream outFR = new FileOutputStream(outputFileR);
                        System.out.println("5");
                        outFR.write(buf);
                        inFR.close();
                        outFR.close();
                        System.out.println("rebalance store Complete: " + filenameSR);

                        files.add(filenameSR);

                      } catch (Exception e) {
                        System.out.println(
                            "Issue: storing rebalance file " + filenameSR + " at " + port);
                      }

                      break;

                    default:
                      System.out.println("Malformed Message to " + port + ": " + line);
                  }

                  socketClient.close();

                } catch (Exception e){}
              }
            }).start();
          } catch(Exception e){}
        }
      } catch(Exception e){}



    } catch(Exception e) { System.out.println("error"+e); }
  }



  public static String getCommand(String line) {
    if (line.contains(" ")) {
      return line.substring(0, line.indexOf(" "));
    }
    return line;
  }

  public static String removeStartWord(String line) {
    if (line.contains(" ")) {
      return line.substring(line.indexOf(" ") + 1);
    }
    return line;
  }

  public static String getFirstOption(String line) {
    return getCommand(removeStartWord(line));
  }

  public static String getSecondOption(String line) {
    return getCommand(removeStartWord(removeStartWord(line)));
  }

  public static String removeCommand(String line) {
    if (line.contains(" ")) {
      return line.substring(line.indexOf(" ") + 1);
    }
    return line;
  }

}