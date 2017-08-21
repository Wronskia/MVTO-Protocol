import java.util.*;

/**
 *  implement a (main-memory) data store with MVTO.
 *  objects are <int, int> key-value pairs.
 *  if an operation is to be refused by the MVTO protocol,
 *  undo its xact (what work does this take?) and throw an exception.
 *  garbage collection of versions is not required.
 *  Throw exceptions when necessary, such as when we try to execute an operation in
 *  a transaction that is not running; when we insert an object with an existing
 *  key; when we try to read or write a nonexisting key, etc.
 *  Keep the interface, we want to test automatically!
 *
 **/


public class MVTO {
  /* TODO -- your versioned key-value store data structure */

  private static int max_xact = 0;

  static HashMap<Integer, ArrayList<Objectversion>> objects = new HashMap();
  static HashMap<Integer, List<Objectversion>> writeFromxact = new HashMap();
  static HashMap<Integer, List<Objectversion>> readFromxact = new HashMap();
  static ArrayList<Integer> uncommited = new ArrayList<>();
  static ArrayList<Integer> commited = new ArrayList<>();
  static ArrayList<Integer> aborted = new ArrayList<>();
  // given the version returns every xacts that red the version of this object
  static HashMap<Objectversion,ArrayList<Integer>> xactred = new HashMap<>();
  // given the version returns every xacts that wrote the version of this object
  static HashMap<Objectversion,ArrayList<Integer>> xactwrote = new HashMap<>();


  private static class Objectversion {
    int key;
    int value;
    int WTS;
    int RTS;

    public Objectversion(int RTS, int WTS, int Value, int key) {
      this.RTS = RTS;
      this.WTS = WTS;
      this.value = Value;
      this.key = key;
    }
    public int getRTS() {
      return RTS;
    }

    public int getWTS() {
      return WTS;
    }

    public int getvalue() {
      return value;
    }
    public int setRTS(int RTS) {
      return this.RTS=RTS;
    }

    public int setWTS(int WTS) {
      return this.WTS=WTS;
    }
    public int setValue(int value) {
      return this.value=value;
    }
  }

  // returns transaction id == logical start timestamp
  public static int begin_transaction() {
    // You might add code here!
    int xact = ++max_xact;
    writeFromxact.put(xact, new ArrayList<>());
    readFromxact.put(xact, new ArrayList<>());
    return xact;
  }

  // create and initialize new object in transaction xact
  public static void insert(int xact, int key, int value) throws Exception {
    if (!aborted.contains(xact)) {
      System.out.println("T(" + xact + "):I(" + key + "," + value + ")");
      if (objects.containsKey(key)) {
        rollback(xact);
        throw new Exception("KEY ALREADY EXISTS IN T(" + xact + "):I(" + key + ")");
      } else {
        Objectversion v = new Objectversion(xact, xact, value, key);
        objects.put(key, new ArrayList<Objectversion>() {{
          add(v);
        }});
        writeFromxact.get(xact).add(v);
        readFromxact.get(xact).add(v);

        xactwrote.put(v, new ArrayList<Integer>(){{add(xact);}});
        xactred.put(v, new ArrayList<Integer>(){{add(xact);}});

      }
    }
  }

  // return value of object key in transaction xact
  public static int read(int xact, int key) throws Exception
  {

    if(commited.contains(xact)) {
      throw new Exception("" + " already committed");
    }

    if(!objects.containsKey(key)) {
      throw new Exception("DOES NOT EXIST IN T("+xact+"):R("+key+")");
    }

    Objectversion found = null;
    int max = -1;
    for (Objectversion v: objects.get(key)) {
      if (v.getWTS() >= max && v.getWTS() <= xact) {
        found = v;
        max = v.getWTS();
      }
    }

    if(found == null) {
      throw new Exception("not found");
    }

    if (xact > found.getRTS()) {
      found.setRTS(xact);
      readFromxact.get(xact).add(found);
      xactred.get(found).add(xact);
    }
    System.out.println("T("+ xact +"):R(" + key + ") => " + found.getvalue());
    return found.value;
  }

  // write value of existing object identified by key in transaction xact
  public static void write(int xact, int key, int value) throws Exception {
    if (!objects.containsKey(key))
    {
      throw new Exception("NO KEY");
    }
    System.out.println("T("+ xact +"):W(" + key + "," + value + ")");
    Objectversion found = null;
    int max = -1;
    for (Objectversion v: objects.get(key)) {
      if (v.WTS >= max && v.WTS <= xact) {
        found = v;
        max = v.WTS;
      }
    }

    if(found == null) {
      throw new Exception("DOES NOT EXIST IN T("+xact+"):W("+key+","+value+")");
    }

    if(xact < found.RTS) {
      rollback(xact);
      return;
    }

    if(xact >= found.RTS && xact == found.WTS) {
      found.setValue(value);
      xactwrote.get(found).add(xact);
    } else if(xact >= found.RTS && xact > found.WTS) {
      Objectversion v = new Objectversion(xact,xact,value, key);
      objects.get(key).add(v);
      writeFromxact.get(xact).add(v);
      xactwrote.put(v,new ArrayList<Integer>(){{add(xact);}});
      xactred.put(v, new ArrayList<>());

    }
    writeFromxact.get(xact).add(found);
  }

  public static void commit(int xact)   throws Exception {
    if (!uncommited.contains(xact))
    {
      System.out.println("T("+xact+"):COMMIT START ");
    }
    if (aborted.contains(xact))
    {
      throw new Exception("T("+xact+") DOES NOT EXIST");
    }
    boolean commit = true;

    for (int i = 0; i < readFromxact.get(xact).size(); i++) {
      for (int j = 0; j < xactwrote.get(readFromxact.get(xact).get(i)).size(); j++) {
        if (!commited.contains(xactwrote.get(readFromxact.get(xact).get(i)).get(j))) {
          if (xactwrote.get(readFromxact.get(xact).get(i)).get(j) != xact) {
            commit = false;
          }
        }
      }
    }



    if (commit)
    {
      if (uncommited.contains(xact))
      {
        uncommited.remove((Integer) xact);
      }
      System.out.println("T("+xact+"):COMMIT FINISH ");
      commited.add(xact);
        for (int i = 0; i < writeFromxact.get(xact).size(); i++) {
          if (xactred.containsKey(writeFromxact.get(xact).get(i))) {
            for (int j = 0; j < xactred.get(writeFromxact.get(xact).get(i)).size(); j++) {
              if (uncommited.contains(xactred.get(writeFromxact.get(xact).get(i)).get(j))) {
                commit(xactred.get(writeFromxact.get(xact).get(i)).get(j));
              }
            }
          }
        }
    }
    else
    {
      if (!uncommited.contains(xact))
      {
        uncommited.add(xact);
      }
    }

  }

  public static void rollback(int xact) throws Exception {

    System.out.println("T(" + xact + "):ROLLBACK ");

      for (int i = 0; i < writeFromxact.get(xact).size(); i++) {
        for (int j = 0; j < xactred.get(writeFromxact.get(xact).get(i)).size(); j++) {
          if (xactred.get(writeFromxact.get(xact).get(i)).get(j) > xact) {
            rollback(xactred.get(writeFromxact.get(xact).get(i)).get(j));
          }
        }
        objects.get(writeFromxact.get(xact).get(i).key).remove(writeFromxact.get(xact).get(i));
      }



    aborted.add(xact);
  }


}