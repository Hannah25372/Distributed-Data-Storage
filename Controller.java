import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class Controller {

  static int R;
  static int timeout;
  static int rebalance_period;

  static ArrayList<Entry> index;
  static ArrayList<Integer> DstoresPort; //but this doesnt go down if DStore crash
  static Map<Integer,Socket> dstoresSockets;
  static ArrayList<String> commandQueue;

  static Timer rebalanceTimer;
  static int time;

  static volatile AtomicBoolean rebalanceOccuring;

  static Map<Integer,String> dstoreFileLists;
  static CountDownLatch listResponseLatch;

  static CountDownLatch latch;


  public static void main(String[] args) throws Exception {

    final int cport = Integer.parseInt(args[0]);
    R = Integer.parseInt(args[1]);
    timeout = Integer.parseInt(args[2]);
    rebalance_period = Integer.parseInt(args[3]);

    index = new ArrayList<>();
    DstoresPort = new ArrayList<>();
    commandQueue = new ArrayList<>();
    dstoresSockets = new HashMap<>();

    rebalanceOccuring = new AtomicBoolean();
    rebalanceOccuring.set(false);
    rebalanceTimer = new Timer();
    time = rebalance_period;
    rebalanceTimer.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        countDownLoop(rebalance_period, timeout, R);
      }
    } , rebalance_period, 250);
    //gameLoop called after delay, every quarter of a second,


    try {

      //socket that waits for requests to come in
      ServerSocket ss = new ServerSocket(cport);

      for(;;) {

        try {
          System.out.println("waiting for connection");
          //waits here until something connects, either client or Dstore
          Socket recipient = ss.accept();
          System.out.println("connected");

          new Thread(new connectionThread(recipient)).start();


        } catch(Exception e){ System.out.println("error "+e); } //client connecting to socket
      }
    } catch(Exception e){ System.out.println("error "+e); }  //making socket
    System.out.println();

  }


  static class connectionThread implements Runnable {

    Socket socket;
    public connectionThread(Socket recipient) {
      socket = recipient;
    }

    @Override
    public void run() {

      try {
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        String line = in.readLine();
        System.out.println("First message received: " + line);

        String command = getCommand(line);

        if (command.equals(Protocol.JOIN_TOKEN)) {
          Integer port = Integer.parseInt(line.substring(line.indexOf(" ") + 1));

          new Thread( new dstoreThread(socket, port)).start();

        } else {

          new Thread ( new clientThread(socket, line)).start();

        }

      } catch (Exception e) {}
    }
  }

  static class dstoreThread implements Runnable  {

    Socket socket;
    Integer port;
    public dstoreThread(Socket recipient, Integer _port) {
      socket = recipient;
      port = _port;
    }

    @Override
    public void run() {

      try {
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        System.out.println("Into thread " + this);

        //Re-balance
        while (!rebalanceOccuring.compareAndSet(false, true));
        //while (rebalanceOccuring.get());

        System.out.println("DStore connected; Port: " + port);
        DstoresPort.add(port);
        dstoresSockets.put(port, socket);
        System.out.println("Number of Dstores: " + DstoresPort.size());

        //rebalanceOccuring.set(true);
        System.out.println("Dstore called rebalance");
        new Thread(new Rebalance()).start();


        String input;
        while ((input = in.readLine()) != null) {

          switch (getCommand(input)) {

            case Protocol.STORE_ACK_TOKEN:
              System.out.println("Received from Dstore " + port + ": " + input);
              String storeACKfilename = getFirstOption(input);
              Integer pos = getIndexByFilename(storeACKfilename);
              System.out.println(index.get(pos).latchS.getCount());
              index.get(pos).latchS.countDown();
              System.out.println(index.get(pos).latchS.getCount());
              break;

            case Protocol.REMOVE_ACK_TOKEN:
              System.out.println("Received from Dstore " + port + ": " + input);
              String removeACKfilename = getFirstOption(input);
              Integer num = getIndexByFilename(removeACKfilename);
              index.get(num).latchR.countDown();
              break;

            case Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN:
              //The remove ACK for if the file wasn't at the dstore
              System.out.println("Received from Dstore " + port + ": " + input);
              String removeACKfilenameNot = getFirstOption(input);
              Integer numN = getIndexByFilename(removeACKfilenameNot);
              if (numN!=-1) {
                index.get(numN).latchR.countDown();
              }
              break;

            case Protocol.LIST_TOKEN:
              //for rebalance
              System.out.println("Received from Dstore " + port + ": " + input);
              String list = removeWordList(input);
              dstoreFileLists.put(port,list);
              listResponseLatch.countDown();
              System.out.println("Count down rebalance list now: " + listResponseLatch.getCount());

              break;

            case Protocol.REBALANCE_COMPLETE_TOKEN:
              System.out.println("Controller recieved RB-C from dstore");
              latch.countDown();

              break;
            default:
              System.out.println("Malformed Message (from dstore): " + input);
              break;
          }

          System.out.println("Controller waiting for next command from dstore");
        }


        //connection failed
        System.out.println("Dstore failed: " + port);
        DstoresPort.remove(port);
        dstoresSockets.remove(port,socket);


      } catch (Exception e) {}

    }
  }

  static class clientThread implements Runnable {

    Socket socket;
    String firstLine;
    public clientThread(Socket recipient, String line) {
      socket = recipient;
      firstLine = line;
    }

    @Override
    public void run() {

      try {

        System.out.println("New client Connected: " + this);

        PrintWriter out = new PrintWriter(socket.getOutputStream()); //to send
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        ArrayList<ArrayList<String>> loadAttempts = new ArrayList<>();

        String input = firstLine;

        do {

          while (rebalanceOccuring.get());

          //Handle other messages coming in here.
          switch (getCommand(input)) {

            case Protocol.LIST_TOKEN:
              System.out.println("Received from client " + this + ": " + input);
              //send back a list of file names
              String result;
              if (DstoresPort.size() < R) {
                result = Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN;
              } else {
                result = Protocol.LIST_TOKEN;
                //create the list - do not including files being stored/removed
                for (Entry file : index) {
                  if (file.getCurrentState() == CurrentState.store_complete) {
                    result += " " + file.getFileName();
                  }

                }
              }
              out.println(result);
              out.flush();
              System.out.println("Sent to " + this + ": " + result);
              break;

            case Protocol.STORE_TOKEN:
              System.out.println("Received from client " + this.toString() + ": " + input);

              String reply = "";
              String filename = getFirstOption(input);
              Integer filesize = Integer.parseInt(getSecondOption(input));

              if (DstoresPort.size() < R) {
                reply = Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN;
                System.out.println("Sent: " + reply);
                //send back to client
                out.println(reply);
                out.flush();
                break;

              } else {

                for (Entry files : index) {
                  if (files.getFileName().equals(filename)) {
                    //For store: file already exists if process of stored/removed
                    if (files.getCurrentState() != CurrentState.remove_complete) {
                      reply = Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN;
                      System.out.println("Sent: " + reply);
                      //send back to client
                      out.println(reply);
                      out.flush();
                      break;
                    }
                  }
                }

              }

              //no error messages. Go ahead with Store
              //get Dstores to store to and send to client
              if (reply.equals("")) {
                ArrayList<Integer> dstores = getRDstoresWithLeast(R);
                index.add(new Entry(filename, CurrentState.store_in_progress, dstores, filesize));
                reply = Protocol.STORE_TO_TOKEN;
                for (Integer port : dstores) {
                  reply += " " + port;
                }
                System.out.println("Sent: " + reply);
                //send back to client
                out.println(reply);
                out.flush();

                Integer pos = getIndexByFilename(filename);
                index.get(pos).latchS = new CountDownLatch(R);

                Boolean answer = index.get(pos).latchS.await(timeout, TimeUnit.MILLISECONDS);
                System.out.println("Awaited complete");

                if (answer) {
                  out.println(Protocol.STORE_COMPLETE_TOKEN);
                  out.flush();
                  System.out.println("Sent: STORE COMPLETE");
                  //updateState(CurrentState.store_complete, pos);
                  index.get(pos).setCurrentState(CurrentState.store_complete);
                } else {
                  //timeout
                  System.out.println(
                      "ACK timeout for all store " + filename + ". Removed from index");
                  index.remove(pos);
                }
              }

              break;

            case Protocol.LOAD_TOKEN:

              System.out.println("Received from client " + this.toString() + ": " + input);

              String filenameLoad = getFirstOption(input);
              Integer numL = getIndexByFilename(filenameLoad);

              if (DstoresPort.size() < R) {
                reply = Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN;
                out.println(reply);
                out.flush();
                System.out.println("Send to client " + this + ": <R");

              } else if (numL == -1) {
                reply = Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN;
                out.println(reply);
                out.flush();
                System.out.println("Send to client " + this + ": File not exist");
              } else if (index.get(numL).getCurrentState() != CurrentState.store_complete) {
                reply = Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN;
                out.println(reply);
                out.flush();
                System.out.println(
                    "Send to client " + this + ": File not exist (concurrent store/remove)");
              } else {

                //make list for ports related to filename incase many
                Integer listPos = getListByFilename(filenameLoad, loadAttempts);
                if (listPos != -1) {
                  //already in list
                  loadAttempts.remove(listPos);
                }

                ArrayList<String> portList = new ArrayList<>();
                portList.add(filenameLoad);
                for (int i = 0; i < R; i++) {
                  portList.add(String.valueOf(index.get(numL).getDstores().get(i)));
                }

                //start with the first Dstore in list
                Integer portFile = Integer.parseInt(portList.get(1));
                reply = Protocol.LOAD_FROM_TOKEN + " " + portFile + " " + index.get(numL)
                    .getFilesize();

                out.println(reply);
                out.flush();
                System.out.println("Send to client " + this + ": " + reply);

                portList.remove(1);
                loadAttempts.add(portList);
              }
              break;

            case Protocol.RELOAD_TOKEN:

              System.out.println("Received from client " + this.toString() + ": " + input);

              String filenameReload = getFirstOption(input);
              Integer numRL = getIndexByFilename(filenameReload);
              String response;
              if (DstoresPort.size() < R) {
                response = Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN;
                out.println(response);
                out.flush();
                System.out.println("Send to client " + this + ": " + response);
              } else if (numRL == -1) {
                response = Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN;
                out.println(response);
                out.flush();
                System.out.println("Send to client " + this + ": " + response);
              } else if (index.get(numRL).getCurrentState() != CurrentState.store_complete) {
                reply = Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN;
                out.println(reply);
                out.flush();
                System.out.println(
                    "Send to client " + this + ": File not exist (concurrent store/remove)");
              } else {

                //SELECT DIFFERENT DSTORE
                //IF ALL TRIED< SEND BACK ERROR LOAD TO CLIENT
                Integer listNum = getListByFilename(filenameReload, loadAttempts);

                if (loadAttempts.get(listNum).size() > 1) {
                  //has more ports to try
                  Integer portRL = Integer.parseInt(loadAttempts.get(listNum).get(1));
                  response =
                      Protocol.LOAD_FROM_TOKEN + " " + portRL + " " + index.get(numRL)
                          .getFilesize();
                  loadAttempts.get(listNum).remove(1);
                } else {
                  //tried all
                  response = Protocol.ERROR_LOAD_TOKEN;
                  loadAttempts.remove(listNum);
                }

                out.println(response);
                out.flush();

                System.out.println("Send to client " + this + ": " + response);

              }
              break;

            case Protocol.REMOVE_TOKEN:

              System.out.println("Received from client " + this + ": " + input);

              String fileName = getFirstOption(input);
              //int num = getIndexByFilename(fileName);
              int num = 0;

              boolean remove = false;
              for (int i = 0; i < index.size(); i++) {
                if (index.get(i).getFileName().equals(fileName) && index.get(i).getCurrentState()==CurrentState.store_complete) {
                  //can remove
                  index.get(i).setCurrentState(CurrentState.remove_in_progress);
                  remove = true;
                  num = i;
                  break;
                }
              }

              if (DstoresPort.size() < R) {
                out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                out.flush();
                System.out.println("Send to client " + this + ": Not enough dstores");
              }

              if (remove) {
                System.out.println("Client " + this + " removing file " + fileName);

                ArrayList<Integer> dstoresRemove = new ArrayList<>(index.get(num).getDstores());

                index.get(num).latchR = new CountDownLatch(R);

                //send the remove messages
                for (int j = 0; j < R; j++) {
                  if (DstoresPort.contains(dstoresRemove.get(j))) {
                    PrintWriter outDs = new PrintWriter(
                        dstoresSockets.get(dstoresRemove.get(j)).getOutputStream()); //to send
                    outDs.println(Protocol.REMOVE_TOKEN + " " + fileName);
                    outDs.flush();
                  }

                }

                boolean ans = index.get(num).latchR.await(timeout, TimeUnit.MILLISECONDS);

                if (ans) {
                  out.println(Protocol.REMOVE_COMPLETE_TOKEN);
                  out.flush();
                  //updateState(CurrentState.remove_complete, num);
                  index.get(num).setCurrentState(CurrentState.remove_complete);
                  index.remove(num);
                  System.out.println("Sent to client " + this + ": Remove complete");
                } else {
                  //timeout
                  System.out.println(
                      "Timeout while waiting for all remove ACK. State not changed");
                }
              } else {
                out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                out.flush();
                System.out.println("Send to client " + this + ": File not exist");
              }
              break;

            default:
              //ignore command, but log it
              System.out.println("Malformed Message (from client): " + input);
          }

        } while ((input = in.readLine()) != null);


      } catch (Exception e) {
        System.out.println("Exception occur in Client thread " + this + " : " + e);

      }

    }
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

  public static String removeWordList(String line) {
    if (line.contains(" ")) {
      return line.substring(line.indexOf(" ") + 1);
    } else {
      return "";
    }
  }

  public static String getFirstOption(String line) {
    return getCommand(removeStartWord(line));
  }

  public static String getSecondOption(String line) {
    return getCommand(removeStartWord(removeStartWord(line)));
  }



  public static ArrayList<Integer> getRDstoresWithLeast(Integer R) {
    ArrayList<Integer> returning = new ArrayList<>();

    ArrayList<Integer> noFiles = new ArrayList<>();
    ArrayList<Integer> dstoresFiles = new ArrayList<>();

    for (Integer port: DstoresPort) {
      dstoresFiles.add(port);
      noFiles.add(0);
    }

    for (Entry item:index) {
      //only count the stuff that will still be there
      //if (item.getCurrentState() == CurrentState.store_complete || item.getCurrentState() == CurrentState.store_in_progress ) {
        for (Integer port : item.getDstores()) {
          for (int i = 0; i < dstoresFiles.size(); i++) {
            if (dstoresFiles.get(i) == port) {
              noFiles.set(i, noFiles.get(i)+1);
            }
          }
        }
      //}
    }

    //put the R lowest counts in a list.
    for (int i = 0; i < noFiles.size(); i++) {
      for (int j = 1; j < noFiles.size() - i; j++) {
        if (noFiles.get(j-1) > noFiles.get(j)) {
          //swap
          int temp = noFiles.get(j-1);
          noFiles.set(j-1, noFiles.get(j));
          noFiles.set(j, temp);

          int temp2 = dstoresFiles.get(j-1);
          dstoresFiles.set(j-1, dstoresFiles.get(j));
          dstoresFiles.set(j, temp2);
        }
      }
    }


    for (int i = 0; i < R; i++) {
      returning.add(dstoresFiles.get(i));
    }

    return returning;
  }



  public static ArrayList<Integer> getRDstores(Integer R) {

    Random random = new Random();
    ArrayList<Integer> availableDstores = new ArrayList<>();
    for (Integer port: DstoresPort) {
      availableDstores.add(port);
    }
    for (int i = 0; i < DstoresPort.size() - R ; i++) {
      availableDstores.remove(random.nextInt(availableDstores.size()));
    }
    return availableDstores;
  }

  enum CurrentState {
    store_in_progress,
    store_complete,
    remove_in_progress,
    remove_complete
  }

  public static class Entry {
    private String fileName;
    private Integer filesize;
    private volatile CurrentState currentState;
    private ArrayList<Integer> Dstores;
    public CountDownLatch latchR;
    public CountDownLatch latchS;

    public Entry(String fileName, CurrentState currentState, ArrayList<Integer> Dstores, Integer filesize) {
      this.fileName = fileName;
      this.currentState = currentState;
      this.Dstores = Dstores; //Will be on R Dstores
      this.filesize = filesize;
    }

    public Entry(Entry entry) {
      this.fileName = entry.getFileName();
      this.currentState = entry.getCurrentState();
      this.Dstores = entry.getDstores(); //Will be on R Dstores
      this.filesize = entry.getFilesize();
    }

    public Integer getFilesize() {
      return filesize;
    }

    public String getFileName() {
      return fileName;
    }

    public void setCurrentState(CurrentState currentState) {
      this.currentState = currentState;
    }

    public CurrentState getCurrentState() {
      return currentState;
    }

    public void setDstores(ArrayList<Integer> dstores) {
      Dstores = dstores;
    }

    public ArrayList<Integer> getDstores() {
      return Dstores;
    }
  }

  public static Integer getIndexByFilename(String filename) {
    Integer n = 0;
    for (Entry file: index) {
      if(file.getFileName().equals(filename)) {
        return n;
      } else {
        n+=1;
      }
    }
    return -1;
  }

  public static Integer getListByFilename(String filename, ArrayList<ArrayList<String>> portList) {
    Integer count = 0;
    for (ArrayList<String> list: portList) {
      if (list.get(0).equals(filename)) {
        return count;
      }
      count += 1;
    }
    return -1;
  }


  public static class Pair {
    private Integer port;
    private Integer count;

    public Pair(Integer port, Integer count){
      this.port = port;
      this.count = count;
    }

    public Integer getCount() {
      return count;
    }

    public Integer getPort() {
      return port;
    }

    public void setCount(Integer count) {
      this.count = count;
    }

    public void setPort(Integer port) {
      this.port = port;
    }
  }


  static class Rebalance implements Runnable {

    @Override
    public void run() {
      if (DstoresPort.size() >= R) {

        //Log
        System.out.println("#### REBALANCE ####");

        //if any store or remove occuring, wait for them to finish
        //go through index and check if store in progress or remove in progress there - wait for to change, or timeout seconds to pass

        listResponseLatch = new CountDownLatch(DstoresPort.size());
        dstoreFileLists = new HashMap<>();


        ArrayList<String> filesAtStore = new ArrayList<>(); //gets List of files from each Dstore
        ArrayList<Integer> change = new ArrayList<>();
        ArrayList<String> files;
        ArrayList<Integer> occurances = new ArrayList<>();
        String bluntAllFiles = "";
        double F = 0;
        double N = 0;
        double lower = 0;
        double upper = 0;


        ArrayList<Integer> failedDstores = new ArrayList<>();

        //this checks if they have gone down? bc will recieve nothing
        for (Integer port : DstoresPort) {
          System.out.println(port);
          try {
            PrintWriter out = new PrintWriter(dstoresSockets.get(port).getOutputStream()); //to send
            System.out.println("Send LIST command");
            out.println(Protocol.LIST_TOKEN);
            out.flush();

          } catch (Exception e) {
            System.out.println("Couldn't contact dstore for rebalance LIST " + port);
          }
        }

        try {

          boolean ans = listResponseLatch.await(timeout, TimeUnit.MILLISECONDS);
          if (ans) {
            System.out.println("All dstore respond to rebalance");
          } else {
            System.out.println("Timeout: not all dstore respond to rebalance");
          }
        } catch (Exception e) {
          System.out.println("Error with latch counting rebalance LIST responses: " + e);
        }

        // get the filesAtStore list. Must be in order of port numbers in port list, and remove ports that didnt get a list
        for (Integer port: DstoresPort) {
          if (dstoreFileLists.containsKey(port)) {
            filesAtStore.add(dstoreFileLists.get(port));
          } else {
            failedDstores.add(port);
          }
        }

        for (Integer port : failedDstores) {
          System.out.println("Dstore failed during rebalance: " + port);
          DstoresPort.remove(port);
          dstoresSockets.remove(port);
        }

        N = DstoresPort.size();
        if (N!=0) {

          //now have list of all files to deal with
          for (String file : filesAtStore) {
            System.out.println(" -> " + file);
            bluntAllFiles += file + " ";
          }

          //list of single files from dstores
          files = new ArrayList<>(Arrays.asList(bluntAllFiles.split(" ")));
          while (files.contains("")) {
            files.remove("");
          }
          //remove doubles
          ArrayList<String> files2 = new ArrayList<>();
          for (String f : files) {
            if (!files2.contains(f)) {
              files2.add(f);
            }
          }
          //if not in file2 but on index, remove from index
          //if on files2 but not index, remove from dstore
          ArrayList<Entry> entriesToRemove = new ArrayList<>();
          for (Entry e : index) {
            if (!files2.contains(e.getFileName())) {
              //remove from index
              entriesToRemove.add(e);
            }
            if (CurrentState.remove_in_progress==e.getCurrentState()) {
              entriesToRemove.add(e);
            }
          }
          for (Entry e : entriesToRemove) {
            index.remove(e);
          }
          //index files
          ArrayList<String> indexFiles = new ArrayList<>();
          for (Entry e : index) {
            indexFiles.add(e.getFileName());
          }
          ArrayList<String> dstoreFilesToRemove = new ArrayList<>();
          for (String fr : files2) {
            if (!indexFiles.contains(fr)) {
              //remove from files2
              dstoreFilesToRemove.add(fr);
            }
          }
          for (String fr : dstoreFilesToRemove) {
            files2.remove(fr);
          }

          //getting how many times each file is stored.
          for (String f2 : files2) {
            Integer counter = 0;
            for (String f : files) {
              if (f2.equals(f)) {
                counter++;
              }
            }
            occurances.add(counter);
          }

          F = files2.size();

          lower = Math.floor(R * F / N);
          upper = Math.ceil(R * F / N);
          System.out.println(
              "R: " + R + " F: " + F + " N: " + N + " Upper: " + upper + " Lower: " + lower);

          change = getChangeList(filesAtStore, lower, upper);

          System.out.println("Dstores:");
          for (int i = 0; i < change.size(); i++) {
            System.out.println(
                change.get(i) + "  |  " + DstoresPort.get(i) + "  |  " + filesAtStore.get(i));
          }
          System.out.println("Dstore files:");
          for (int i = 0; i < files2.size(); i++) {
            System.out.println(files2.get(i) + "  |  " + occurances.get(i));
          }
          System.out.println("Index Files:");
          for (int i = 0; i < files2.size(); i++) {
            System.out.println(indexFiles.get(i));
          }

          ArrayList<String> dstoreSendList = new ArrayList<>();
          ArrayList<String> dstoreRemoveList = new ArrayList<>();
          ArrayList<String> dstorePortText = new ArrayList<>();
          ArrayList<String> filesToAdd = new ArrayList<>();


          ///DATA COLLECTION COMPLETE - REBALANCE START

          //go through occurnaces, find what files to add there are
          //go through fileChanges / dstores port. Find out which ones need to remove a file, record it in their message
          //go through filesChanges, see which ones need a file, and find out where they can get it?
          //if a file not on files2, needs to be removed from dstores

          //will need to be updating: occurances, change, filesAtDstore

          //through occurances to see which files need to be stored more times
          for (int i = 0; i < occurances.size(); i++) {
            if (occurances.get(i) < R) {
              for (int j = 0; j < (R - occurances.get(i)); j++) {
                filesToAdd.add(files2.get(i));
              }
            }
          }
          boolean allGood = true;
          for (int num : change) {
            if (num != 0) {
              allGood = false;
            }
          }
          allGood = allGood && (filesToAdd.size() == 0);
          if (!allGood) {


            //to remove any files where there is more than R
            for (int i = 0; i < occurances.size(); i++) {
              if (occurances.get(i) > R) {
                int count = occurances.get(i);
                String file = files2.get(i);
                int noToRemove = count - R;

                //check if there is ones with change < 0 first to remove from
                for (int j = 0; j < change.size(); j++) {
                  if (change.get(j)<0 && filesAtStore.get(j).contains(file) && noToRemove>0) {
                    //this is dstore to remove from

                    String removeText = DstoresPort.get(j) + " " + file;
                    dstoreRemoveList.add(removeText);


                    //to remove the file from the remove list
                    ArrayList<String> actualList = new ArrayList<>(Arrays.asList(filesAtStore.get(j).split(" ")));
                    actualList.remove(file);
                    String newString = "";
                    for (String f: actualList) {
                      newString += f + " ";
                    }
                    if(newString.length()>0) {
                      newString = newString.substring(0, newString.length() - 1); //remove last char
                    }
                    filesAtStore.set(j,newString);

                    change.set(j, getNewChange(newString, upper, lower));


                    noToRemove -= 1;
                    count -=1;
                  }
                }
                if (noToRemove>0) {
                  //wasnt any that needed removing to take from, so just take from random ones that have it
                  for (int j = 0; j < change.size(); j++) {
                    if (filesAtStore.get(j).contains(file) && noToRemove>0) {
                      //this is dstore to remove from

                      String removeText = DstoresPort.get(j) + " " + file;
                      dstoreRemoveList.add(removeText);

                      //to remove the file from the remove list
                      ArrayList<String> actualList = new ArrayList<>(Arrays.asList(filesAtStore.get(j).split(" ")));
                      actualList.remove(file);
                      String newString = "";
                      for (String f: actualList) {
                        newString += f + " ";
                      }
                      if(newString.length()>0) {
                        newString = newString.substring(0, newString.length() - 1); //remove last char
                      }
                      filesAtStore.set(j,newString);


                      //if its zero, could stay zero or increase to 1, depending on how many files it has and the upper and lower
                      change.set(j, getNewChange(newString, upper, lower));

                      noToRemove -= 1;
                      count -= 1;
                    }
                  }
                }
                //update change, occurances, filesAtStore
                occurances.set(i,count);
              }
            }


            int changeAddTotal = 0;
            for (int num : change) {
              if (num > 0) {
                changeAddTotal += num;
              }
            }
            while (filesToAdd.size() > 0 && changeAddTotal > 0) {
              //Now look through dstores for ones with >0 these can be added too. will need to find where they can be sent from
              for (int i = 0; i < DstoresPort.size(); i++) {
                //need more
                if (change.get(i) > 0 && filesToAdd.size() > 0) {
                  //check through list to see which one from addList could add. Then check what dstore has that, and add it too that dstores send list
                  int bound = filesToAdd.size();
                  for (int j = 0; j < bound; j++) {
                    if (!filesAtStore.get(i).contains(filesToAdd.get(j))) {
                      //not at that dstore already
                      //add this file j to dstore i from ?

                      //From Text.txt To
                      String text = filesToAdd.get(j) + " " + DstoresPort.get(i);
                      for (int k = 0; k < filesAtStore.size(); k++) {
                        if (filesAtStore.get(k).contains(filesToAdd.get(j))) {
                          text = DstoresPort.get(k) + " " + text;
                        }
                      }
                      dstoreSendList.add(text);

                      //change occurrences, change and filesAtStore
                      String newFiles = filesAtStore.get(i) + " " + filesToAdd.get(j);
                      filesAtStore.set(i, newFiles);
                      change.set(i, getNewChange(newFiles, upper, lower));
                      int index = getIndexS(files2, filesToAdd.get(j));
                      occurances.set(index, occurances.get(index) + 1);

                      filesToAdd.remove(j);
                      j = bound;
                    }
                  }
                  changeAddTotal -= 1;
                  //whether one was changed or not, it had the oppurtunity too in here, so count it since needed to break the loop
                }
              }
            }
            //finished with the if there are extra files to add, can we add them somewhere
            // dstore send list contains all the  "from file to" records


            System.out.println("First step of rebalance");
            System.out.println("Dstores:");
            for (int i = 0; i < change.size(); i++) {
              System.out.println(
                  change.get(i) + "  |  " + DstoresPort.get(i) + "  |  " + filesAtStore.get(i));
            }
            System.out.println("Dstore files:");
            for (int i = 0; i < files2.size(); i++) {
              System.out.println(files2.get(i) + "  |  " + occurances.get(i));
            }
            System.out.println("Index Files:");
            for (int i = 0; i < files2.size(); i++) {
              System.out.println(indexFiles.get(i));
            }
            System.out.println("DStore send list:");
            for (int i = 0; i < dstoreSendList.size(); i++) {
              System.out.println(dstoreSendList.get(i));
            }


            //through changes to find ones that have too many, and select to move them to ones with too few
            int noFilesOverMax = 0;
            for (Integer num : change) {
              if (num < 0) {
                noFilesOverMax -= num;    //0 - -2 = 2,   2 - -3 = 5,
              }
            }
            while (noFilesOverMax > 0) {
              for (int i = 0; i < change.size(); i++) {  //go through each dstore with too many
                if (change.get(i) < 0) {
                  //this dstore has too many
                  //check if there is a dstore with too few in which it can send a file.
                  String fileListAtDstoreRemove = filesAtStore.get(i);
                  ArrayList<String> removeList = new ArrayList<>(Arrays.asList(fileListAtDstoreRemove.split(" ")));

                  for (int j = 0; j < change.size(); j++) {
                    if (change.get(j) > 0) {
                      String fileListAtDstoreAdd = filesAtStore.get(j);
                      for (int k = 0; k < removeList.size(); k++) {
                        String potentialSwap = removeList.get(k);

                        if (!fileListAtDstoreAdd.contains(potentialSwap)) {
                          //can move the file!!!!

                          //from file to
                          String storeToText = DstoresPort.get(i) + " " + potentialSwap + " " + DstoresPort.get(j);
                          dstoreSendList.add(storeToText);
                          // store file
                          String removeText = DstoresPort.get(i) + " " + potentialSwap;
                          dstoreRemoveList.add(removeText);


                          k = removeList.size();

                          //update fields. occurences stays the same. swap dstores

                          fileListAtDstoreAdd += " " + potentialSwap;
                          filesAtStore.set(j,fileListAtDstoreAdd);
                          //to remove the file from the remove list
                          removeList.remove(potentialSwap);
                          String newList = "";
                          for (String file: removeList) {
                            newList += file + " ";
                          }
                          if(newList.length()>0) {
                            newList = newList.substring(0, newList.length() - 1); //remove last char
                          }
                          filesAtStore.set(i,newList);
                          change.set(i, getNewChange(newList, upper, lower));
                          change.set(j, getNewChange(fileListAtDstoreAdd, upper, lower));

                        }
                      }

                    }
                  }


                  noFilesOverMax -= 1;
                }
              }
            }


            System.out.println("Second step of rebalance");
            System.out.println("Dstores:");
            for (int i = 0; i < change.size(); i++) {
              System.out.println(
                  change.get(i) + "  |  " + DstoresPort.get(i) + "  |  " + filesAtStore.get(i));
            }
            System.out.println("Dstore files:");
            for (int i = 0; i < files2.size(); i++) {
              System.out.println(files2.get(i) + "  |  " + occurances.get(i));
            }
            System.out.println("Index Files:");
            for (int i = 0; i < files2.size(); i++) {
              System.out.println(indexFiles.get(i));
            }
            System.out.println("DStore send list:");
            for (int i = 0; i < dstoreSendList.size(); i++) {
              System.out.println(dstoreSendList.get(i));
            }
            System.out.println("DStore remove list:");
            for (int i = 0; i < dstoreRemoveList.size(); i++) {
              System.out.println(dstoreRemoveList.get(i));
            }


            //do the message construct here for the actual file moving

            ArrayList<String> files_to_remove = new ArrayList<>(); //done in order of port numbers
            //for each dstore
            for (Integer port: DstoresPort) {
              String textToAdd = "";
              int count = 0;

              //for each removal entry
              for (String text : dstoreRemoveList) {
                if (getCommand(text).equals(String.valueOf(port))) {
                  textToAdd += " " + removeStartWord(text);
                  count += 1;
                }
              }
              if (count!=0) {
                files_to_remove.add(count + textToAdd);
              } else {
                files_to_remove.add("");
              }

            }

            //dstoreSendList: from file to
            //this is for one dstore sending::   no.filesToSend  filename, no.dstoresToSendTo, dstores
            ArrayList<String> files_to_send = new ArrayList<>();

            for (Integer port: DstoresPort) {
              ArrayList<String> fileForOneDstore = new ArrayList<>();
              String textToAdd = "";
              Integer count =  0;

              for (String text: dstoreSendList) {
                if (getCommand(text).equals(String.valueOf(port))) {
                  fileForOneDstore.add(removeStartWord(text));
                }
              }
              //get an array containing all the files sending for one dstore (may have several of one file going to diff stores)
              //   text1.txt 4001    text2.txt  4002   text1.txt 4002

              ArrayList<Integer> countsForEachFile = new ArrayList<>();
              ArrayList<String> recordForEachFile = new ArrayList<>();
              for (String sender : fileForOneDstore) {
                boolean appended = false;
                for (int i = 0; i < recordForEachFile.size(); i++) {
                  if (recordForEachFile.get(i).contains(getCommand(sender))) {
                    recordForEachFile.set(i, recordForEachFile.get(i) + " " + removeStartWord(sender));
                    countsForEachFile.set(i, countsForEachFile.get(i) + 1);
                    appended = true;
                  }
                }
                if (!appended) {
                  // new to list
                  recordForEachFile.add(sender);
                  countsForEachFile.add(1);
                }
                //now have:    text1.txt 4001 4002             text2.txt 4003
                //                     2                               1
                count = recordForEachFile.size();
                textToAdd = String.valueOf(count);
                for (int i = 0; i < count; i++) {
                  textToAdd += " " + getCommand(recordForEachFile.get(i)) + " " + countsForEachFile.get(i) + " " + removeStartWord(recordForEachFile.get(i));
                }
              }
              files_to_send.add(textToAdd);
            }

            //REBALANCE 0 if nothing to send or delete
            //REBALNCE    if only smth to send
            //REBALNCE    if only smth to delete
            ArrayList<String> rebalanceMessages = new ArrayList<>();
            System.out.println("Messages:");
            for (int i = 0; i < DstoresPort.size(); i++) {
              String message = Protocol.REBALANCE_TOKEN + " " + files_to_send.get(i) + " " + files_to_remove.get(i);
              rebalanceMessages.add(message);
              System.out.println(DstoresPort.get(i) + ": " + message);
            }

            //send REBALANCE message
            for (int i = 0; i < DstoresPort.size(); i++) {
              Integer port = DstoresPort.get(i);
              try {

                //make this send all messages first, then wait for ACKs with countdownlatch.
                //Socket socket = new Socket(InetAddress.getLocalHost(), port);
                PrintWriter out = new PrintWriter(dstoresSockets.get(port).getOutputStream()); //to send

                System.out.println("Send REBALANCE command");
                out.println(rebalanceMessages.get(i));
                out.flush();

                //int t = timeout*(int)N;
                //socket.setSoTimeout(t);
                //String ack = in.readLine();
                //System.out.println("Received: " + ack);

              } catch (Exception e) {
              }
            }



            latch = new CountDownLatch(DstoresPort.size());
            try {
              latch.await(timeout, TimeUnit.MILLISECONDS);

            } catch (Exception e){
              System.out.println("Not all rebalance ACK received in timeout");
            }



          } // end of if (!allGood)  -no need to rebalnce if all good
        } //end of if (N!=0) -no dstores, cannot rebalnce


        rebalanceOccuring.set(false);
      }
      rebalanceOccuring.set(false);
      System.out.println("Finished rebalance function");
    }
  }



  public static Integer getIndexS(ArrayList<String> list, String element) {
    for (int i = 0; i < list.size(); i++) {
      if (list.get(i)==element) {
        return i;
      }
    }
    return -1;
  }

  public static Integer getNewChange(String files, double upper, double lower){
    int fileCount;
    if (files.equals("")) {
      fileCount = 0;
    } else {
      //number of spaces plus one
      Integer spaceCount = 0;
      for (int i = 0; i < files.length(); i++) {
        if (files.charAt(i) == ' ') {
          spaceCount++;
        }
      }
      fileCount = spaceCount+1;
    }

    if (fileCount < lower) {
      return((int) lower - fileCount);
    } else if (fileCount > upper) {
      return((int) upper - fileCount);
    } else {
      return(0);
    }

  }

  public static ArrayList<Integer> getChangeList(ArrayList<String> filesAtStore, double lower, double upper) {
    ArrayList<Integer> changeList = new ArrayList<>();
    for (String files: filesAtStore) {
      changeList.add(getNewChange(files, upper, lower));
    }
    return changeList;
  }


  /**
   * Code to run every 250milliseconds
   * Counts down time, and does rebalance if it equals 0
   */
  public static void countDownLoop(int rebalance_period, int timeout, int R){

    if (rebalanceOccuring.get()) {  //if it is true
      //already happening, don't count down and reset timer
      time = rebalance_period;
      System.out.println("(timer) rebalance occuring, reset timer and no countdown");

    } else {
      if (time <= 0) {
        if (rebalanceOccuring.compareAndSet(false,true)) {
          //if some other one started, then no need for this one.
          new Thread(new Rebalance()).start();
          System.out.println("Completed rebalance from time period");
        }
        time = rebalance_period;
      } else {
        //decrease time
        time -= 250;
      }
    }

  }


}


