package worker;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import java.sql.*;
import org.json.JSONObject;

class Worker {
  public static void main(String[] args) {
    try {
      Jedis redis = connectToRedis("redis");
      Connection dbConn = connectToDB("db");

      System.err.println("Watching vote queue");

      while (true) {
        String voteJSON = redis.blpop(0, "votes").get(1);
        JSONObject voteData = new JSONObject(voteJSON);
        String voterID = voteData.getString("voter_id");
        String vote = voteData.getString("vote");

        System.err.printf("Processing vote for '%s' by '%s'\n", vote, voterID);
        updateVote(dbConn, voterID, vote);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      System.exit(1);
    }
  }

  static void updateVote(Connection dbConn, String voterID, String vote) throws SQLException {
    PreparedStatement insert = dbConn.prepareStatement(
      "INSERT INTO votes (id, vote) VALUES (?, ?)");
    insert.setString(1, voterID);
    insert.setString(2, vote);

    try {
      insert.executeUpdate();
    } catch (SQLException e) {
      PreparedStatement update = dbConn.prepareStatement(
        "UPDATE votes SET vote = ? WHERE id = ?");
      update.setString(1, vote);
      update.setString(2, voterID);
      update.executeUpdate();
    }
  }

static Jedis connectToRedis(String host) {
  // if we are running in container or kubernetes 
  Jedis conn = new Jedis(host);
  
  // if we are running in local machine
    // Jedis conn = new Jedis("localhost", 6379);
    int retryAttempts = 10;  // Retry limit
    int attempt = 0;

    while (attempt < retryAttempts) {
        try {
            // Try a basic ping to check if Redis is responding
            if ("PONG".equals(conn.ping())) {
                break;  // Redis is available
            }
        } catch (JedisConnectionException e) {
            attempt++;
            System.err.println("Waiting for redis... attempt " + attempt + " of " + retryAttempts);
            sleep(1000);
        }
    }

    if (attempt == retryAttempts) {
        System.err.println("Unable to connect to Redis after " + retryAttempts + " attempts.");
        System.exit(1);  // Exit the application or handle appropriately
    }

    System.err.println("Connected to redis");
    return conn;
}

  static Connection connectToDB(String host) throws SQLException {
    Connection conn = null;

    try {

      Class.forName("org.postgresql.Driver");
      // this linkn for the container and  kubernetes
      String url = "jdbc:postgresql://" + host + "/postgres";
      
      // this link for the local machine
      // String url = "jdbc:postgresql://localhost:5432/postgres";

      while (conn == null) {
        try {
          conn = DriverManager.getConnection(url, "postgres", "postgres");
        } catch (SQLException e) {
          System.err.println("Waiting for db");
          sleep(1000);
        }
      }

      PreparedStatement st = conn.prepareStatement(
        "CREATE TABLE IF NOT EXISTS votes (id VARCHAR(255) NOT NULL UNIQUE, vote VARCHAR(255) NOT NULL)");
      st.executeUpdate();

    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      System.exit(1);
    }

    System.err.println("Connected to db");
    return conn;
  }

  static void sleep(long duration) {
    try {
      Thread.sleep(duration);
    } catch (InterruptedException e) {
      System.exit(1);
    }
  }
}
