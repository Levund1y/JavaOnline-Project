import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

import java.io.FileReader;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class Program {

    public static void main(String[] args) throws IOException, CsvException, ClassNotFoundException, SQLException {

        var list = new ArrayList<Earthquake>();

        try (CSVReader reader = new CSVReader(new FileReader("Землетрясения.csv"))) {
            reader.readNext();
            var r = reader.readAll();
            r.forEach(x -> {
                list.add(new Earthquake(x));
            });
        }


        var stm = database(list);

        task1(stm);
        task2(stm);
        task3(stm);

        stm.close();
    }

    private static Statement database(ArrayList<Earthquake> list) throws ClassNotFoundException, SQLException {
        Class.forName("org.sqlite.JDBC");

        var cnt = DriverManager.getConnection("jdbc:sqlite:earthquakes.db");
        var stm = cnt.createStatement();

        stm.execute("drop table 'earthquakes';");
        stm.execute(
                "CREATE TABLE 'earthquakes' (" +
                        "'id' varchar PRIMARY KEY, " +
                        "'depth' int, " +
                        "'magnitudeType' varchar," +
                        "'magnitude' real, " +
                        "'state' text, " +
                        "'time' time);");


        var prepareStatement = cnt.prepareStatement(
                "INSERT INTO 'earthquakes' ('id','depth','magnitudeType','magnitude','state','time') VALUES (?,?,?,?,?,?);");


        list.forEach(x -> {
            try {
                prepareStatement.setString(1, x.ID);
                prepareStatement.setInt(2, x.depth);
                prepareStatement.setString(3, x.magnitudeType);
                prepareStatement.setDouble(4, x.magnitude);
                prepareStatement.setString(5, x.state);
                prepareStatement.setString(6, x.time);
                prepareStatement.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        return stm;
    }

    private static void task3(Statement stm) throws SQLException {
        System.out.println("Выведите в консоль название штата, в котором произошло самое глубокое землетрясение в 2013 году");
        System.out.println(stm.executeQuery("select state,MAX(depth),strftime('%Y',time) as year from earthquakes where year ='2013';").getString("state"));
    }

    private static void task2(Statement stm) throws SQLException {
        System.out.println("Выведите в консоль среднюю магнитуду для штата \"West Virginia\"");
        System.out.println(stm.executeQuery("select AVG(magnitude) as avg from earthquakes where state='West Virginia'").getDouble("avg"));
    }

    private static void task1(Statement stm) throws SQLException {
        System.out.println("Постройте график по среднему количеству землетрясений для каждого года");
        var r = stm.executeQuery("select COUNT(*) AS count,strftime('%Y',time) as year from earthquakes where (year IS NOT NULL)  GROUP BY year;");

        while (r.next()) {
            System.out.println(r.getString("year")+" "+r.getString("count"));
        }
    }
}
