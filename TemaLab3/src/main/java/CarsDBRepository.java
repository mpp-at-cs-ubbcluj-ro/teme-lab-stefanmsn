import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.*;

public class CarsDBRepository implements CarRepository {

    private JdbcUtils dbUtils;


    private static final Logger logger = LogManager.getLogger();

    public CarsDBRepository(Properties props) {
        logger.info("Initializing CarsDBRepository with properties: {} ", props);
        dbUtils = new JdbcUtils(props);
    }

    @Override
    public List<Car> findByManufacturer(String manufacturer) {
        List<Car> cars = new ArrayList<>();
        Connection conn = dbUtils.getConnection();
        try (PreparedStatement ps = conn.prepareStatement("SELECT * FROM Cars WHERE manufacturer=?")) {
            ps.setString(1, manufacturer);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                cars.add(new Car(rs.getString("manufacturer"), rs.getString("model"), rs.getInt("year")));
            }

        } catch (SQLException e) {
            logger.error(e);
            System.err.println("Error DB " + e);
        }
        return cars;
    }

    @Override
    public List<Car> findBetweenYears(int min, int max) {
        List<Car> cars = new ArrayList<>();
        Connection conn = dbUtils.getConnection();
        try (PreparedStatement ps = conn.prepareStatement("SELECT * FROM Cars WHERE year BETWEEN ? AND ?")) {
            ps.setInt(1, min);
            ps.setInt(2, max);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                cars.add(new Car(rs.getString("manufacturer"), rs.getString("model"), rs.getInt("year")));
            }
        } catch (SQLException e) {
            logger.error(e);
            System.err.println("Error DB " + e);
        }
        return cars;
    }

    @Override
    public void add(Car elem) {
        logger.traceEntry("saving task {} ", elem);
        Connection conn = dbUtils.getConnection();
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO Cars(manufacturer, model, year) values (?,?,?)")) {
            ps.setString(1, elem.getManufacturer());
            ps.setString(2, elem.getModel());
            ps.setInt(3, elem.getYear());
            int result = ps.executeUpdate();
        } catch (SQLException e) {
            logger.error(e);
            System.err.println("Error DB " + e);
        }

    }

    @Override
    public void update(Integer integer, Car elem) {
        logger.traceEntry();
        Connection conn = dbUtils.getConnection();
        try (PreparedStatement ps = conn.prepareStatement("UPDATE Cars SET manufacturer=?, model=?, year=? WHERE id=?")) {
            ps.setString(1, elem.getManufacturer());
            ps.setString(2, elem.getModel());
            ps.setInt(3, elem.getYear());
            ps.setInt(4, elem.getId());
            int result = ps.executeUpdate();
        } catch (SQLException e) {
            logger.error(e);
        }
    }

    @Override
    public Iterable<Car> findAll() {
        logger.traceEntry();
        Connection conn = dbUtils.getConnection();
        List<Car> cars = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement("SELECT * FROM Cars")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    int id = rs.getInt("id");
                    String manufacturer = rs.getString("manufacturer");
                    String model = rs.getString("model");
                    int year = rs.getInt("year");
                    Car car = new Car(manufacturer, model, year);
                    car.setId(id);
                    cars.add(car);
                }
            }
        } catch (SQLException e) {
            logger.error(e);
            System.err.println("Error DB " + e);
        }
        logger.traceExit(cars);
        return cars;
    }
}