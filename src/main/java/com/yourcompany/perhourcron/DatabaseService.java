package com.yourcompany.perhourcron;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DatabaseService {
    private static final Logger logger = Logger.getLogger(DatabaseService.class.getName());
    private static final String DB_URL = "jdbc:mysql://10.55.130.5:3306/_dashboard";
    private static final String DB_USER = "read_only";
    private static final String DB_PASSWORD = "Rqejkal#142";

    private static final String OVERALL_QUERY = """
            SELECT HOUR(tx_date) AS hour,
                   SUM(request) AS request, SUM(response) AS response,
                   SUM(win) AS win, SUM(impression) AS impression,
                   SUM(revenue) AS revenue, SUM(cost) AS cost
            FROM _tx_hourly_transactions
            WHERE DATE(tx_date) = ?
            GROUP BY HOUR(tx_date)
            ORDER BY hour
            """;

    private static final String PUBLISHER_QUERY = """
            SELECT HOUR(tx_date) AS hour, pub_id,
                   SUM(request) AS request, SUM(response) AS response,
                   SUM(win) AS win, SUM(impression) AS impression,
                   SUM(revenue) AS revenue, SUM(cost) AS cost
            FROM _tx_hourly_transactions
            WHERE DATE(tx_date) = ?
            GROUP BY HOUR(tx_date), pub_id
            ORDER BY hour, pub_id
            """;

    private static final String ADVERTISER_QUERY = """
            SELECT HOUR(tx_date) AS hour, adv_id,
                   SUM(request) AS request, SUM(response) AS response,
                   SUM(win) AS win, SUM(impression) AS impression,
                   SUM(revenue) AS revenue, SUM(cost) AS cost
            FROM _tx_hourly_transactions
            WHERE DATE(tx_date) = ? AND adv_id != 0
            GROUP BY HOUR(tx_date), adv_id
            ORDER BY hour, adv_id
            """;

             public String generateComprehensiveHourlyReports() throws IOException {
        String reportPath = String.format("reports/ComprehensiveHourlyReport_%s.xlsx", LocalDate.now());
        
        try (Workbook workbook = new XSSFWorkbook()) {
            LocalDate today = LocalDate.now(ZoneOffset.UTC);
            LocalDate yesterday = today.minusDays(1);
            int currentHour = LocalDateTime.now(ZoneOffset.UTC).getHour();

            // Create all three report types
            createHourlyComparisonSheet(workbook, "Overall Hourly Comparison",
                    OVERALL_QUERY, today, yesterday, currentHour, false, false);
            createHourlyComparisonSheet(workbook, "Publisher Hourly Comparison",
                    PUBLISHER_QUERY, today, yesterday, currentHour, true, false);
            createHourlyComparisonSheet(workbook, "Advertiser Hourly Comparison",
                    ADVERTISER_QUERY, today, yesterday, currentHour, false, true);

            writeWorkbookToFile(workbook, reportPath);
            return reportPath;

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to generate report", e);
            throw new IOException("Failed to generate report: " + e.getMessage(), e);
        }
    }
        
    private void writeWorkbookToFile(Workbook workbook, String fileName) throws IOException {
        File file = new File(fileName);
        File parentDir = file.getParentFile();

        if (parentDir != null && !parentDir.exists()) {
            if (!parentDir.mkdirs()) {
                throw new IOException("Failed to create directories: " + parentDir);
            }
        }

        try (FileOutputStream outputStream = new FileOutputStream(file)) {
            workbook.write(outputStream);
        }
    }
    

    private Connection getDatabaseConnection() throws SQLException {
        try {
            return DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Database connection failed", e);
            throw new SQLException("Failed to establish database connection", e);
        }
    }

    public Map<String, Double> getOverallMetricsSummary(LocalDate date) throws SQLException {
        String query = """
                SELECT SUM(request) AS total_requests,
                       SUM(response) AS total_responses,
                       SUM(win) AS total_wins,
                       SUM(impression) AS total_impressions,
                       SUM(revenue) AS total_revenue,
                       SUM(cost) AS total_cost
                FROM _tx_hourly_transactions
                WHERE DATE(tx_date) = ?
                """;

        return executeMetricsSummaryQuery(query, date);
    }

    public Map<String, Double> getOverallMetricsSummaryUntilHour(LocalDate date, int currentHour) throws SQLException {
        String query = """
                SELECT SUM(request) AS total_requests,
                       SUM(response) AS total_responses,
                       SUM(win) AS total_wins,
                       SUM(impression) AS total_impressions,
                       SUM(revenue) AS total_revenue,
                       SUM(cost) AS total_cost
                FROM _tx_hourly_transactions
                WHERE DATE(tx_date) = ? AND HOUR(tx_date) < ?
                """;

        try (Connection connection = getDatabaseConnection();
                PreparedStatement statement = connection.prepareStatement(query)) {

            statement.setDate(1, java.sql.Date.valueOf(date));
            statement.setInt(2, currentHour);

            try (ResultSet resultSet = statement.executeQuery()) {
                Map<String, Double> summary = new HashMap<>();
                if (resultSet.next()) {
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    for (int i = 1; i <= metaData.getColumnCount(); i++) {
                        String columnName = metaData.getColumnName(i).toLowerCase();
                        summary.put(columnName, resultSet.getDouble(i));
                    }
                }
                return summary;
            }
        }
    }

    public Map<String, Map<String, Double>> getPublisherMetricsSummary(LocalDate date) throws SQLException {
        String query = """
                SELECT pub_id,
                       SUM(request) AS total_requests,
                       SUM(response) AS total_responses,
                       SUM(win) AS total_wins,
                       SUM(impression) AS total_impressions,
                       SUM(revenue) AS total_revenue,
                       SUM(cost) AS total_cost
                FROM _tx_hourly_transactions
                WHERE DATE(tx_date) = ?
                GROUP BY pub_id
                """;

        return executeDetailedMetricsSummaryQuery(query, date, "pub_id");
    }

    public Map<String, Map<String, Double>> getAdvertiserMetricsSummary(LocalDate date) throws SQLException {
        String query = """
                SELECT adv_id,
                       SUM(request) AS total_requests,
                       SUM(response) AS total_responses,
                       SUM(win) AS total_wins,
                       SUM(impression) AS total_impressions,
                       SUM(revenue) AS total_revenue,
                       SUM(cost) AS total_cost
                FROM _tx_hourly_transactions
                WHERE DATE(tx_date) = ? AND adv_id != 0
                GROUP BY adv_id
                """;

        return executeDetailedMetricsSummaryQuery(query, date, "adv_id");
    }

    private Map<String, Double> executeMetricsSummaryQuery(String query, LocalDate date) throws SQLException {
        Map<String, Double> summary = new HashMap<>();

        try (Connection connection = getDatabaseConnection();
                PreparedStatement statement = connection.prepareStatement(query)) {

            statement.setDate(1, java.sql.Date.valueOf(date));

            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    for (int i = 1; i <= metaData.getColumnCount(); i++) {
                        String columnName = metaData.getColumnName(i).toLowerCase();
                        summary.put(columnName, resultSet.getDouble(i));
                    }
                }
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Error executing metrics summary query", e);
            throw e;
        }

        return summary;
    }

    private Map<String, Map<String, Double>> executeDetailedMetricsSummaryQuery(
            String query, LocalDate date, String idColumnName) throws SQLException {
        Map<String, Map<String, Double>> summary = new HashMap<>();

        try (Connection connection = getDatabaseConnection();
                PreparedStatement statement = connection.prepareStatement(query)) {

            statement.setDate(1, java.sql.Date.valueOf(date));

            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    String id = String.valueOf(resultSet.getObject(idColumnName));
                    Map<String, Double> metrics = new HashMap<>();

                    ResultSetMetaData metaData = resultSet.getMetaData();
                    for (int i = 2; i <= metaData.getColumnCount(); i++) {
                        String columnName = metaData.getColumnName(i).toLowerCase();
                        metrics.put(columnName, resultSet.getDouble(i));
                    }

                    summary.put(id, metrics);
                }
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Error executing detailed metrics summary query", e);
            throw e;
        }

        return summary;
    }

    private void createHourlyComparisonSheet(Workbook workbook, String sheetName,
            String query, LocalDate today, LocalDate yesterday, int currentHour,
            boolean includePublisher, boolean includeAdvertiser) throws SQLException {

        // Create sheet and set up headers
        Sheet sheet = workbook.createSheet(sheetName);
        Row headerRow = sheet.createRow(0);

        // Use ArrayList instead of Arrays.asList
        List<String> columns = new ArrayList<>();
        columns.add("Date");
        columns.add("Hour");
        columns.add("Request");
        columns.add("Response");
        columns.add("Win");
        columns.add("Impression");
        columns.add("Revenue");
        columns.add("Cost");
        columns.add("Change (%)");

        if (includePublisher)
            columns.add("Publisher");
        if (includeAdvertiser)
            columns.add("Advertiser");

        // Add headers to sheet
        for (int i = 0; i < columns.size(); i++) {
            headerRow.createCell(i).setCellValue(columns.get(i));
        }

        // Fetch data for the last hour of today and yesterday
        int lastHour = currentHour - 1; // Previous hour
        Map<String, Map<String, Double>> todayData = fetchLastHourData(query, today, lastHour);
        Map<String, Map<String, Double>> yesterdayData = fetchLastHourData(query, yesterday, lastHour);

        int rowIdx = 1;
        // Add today's data
        if (!todayData.isEmpty()) {
            Row row = sheet.createRow(rowIdx++);
            Map<String, Double> todayMetrics = todayData.get(String.valueOf(lastHour));

            row.createCell(0).setCellValue(today.toString());
            row.createCell(1).setCellValue(lastHour);
            row.createCell(2).setCellValue(todayMetrics.getOrDefault("request", 0.0));
            row.createCell(3).setCellValue(todayMetrics.getOrDefault("response", 0.0));
            row.createCell(4).setCellValue(todayMetrics.getOrDefault("win", 0.0));
            row.createCell(5).setCellValue(todayMetrics.getOrDefault("impression", 0.0));
            row.createCell(6).setCellValue(todayMetrics.getOrDefault("revenue", 0.0));
            row.createCell(7).setCellValue(todayMetrics.getOrDefault("cost", 0.0));

            // Calculate percentage change
            double todayRevenue = todayMetrics.getOrDefault("revenue", 0.0);
            double yesterdayRevenue = yesterdayData.isEmpty() ? 0.0
                    : yesterdayData.get(String.valueOf(lastHour)).getOrDefault("revenue", 0.0);
            double change = calculatePercentageChange(todayRevenue, yesterdayRevenue);
            row.createCell(8).setCellValue(change);

            // Optionally add publisher or advertiser metrics
            int changeColumnIndex = 9;
            if (includePublisher && todayMetrics.containsKey("pub_id")) {
                row.createCell(changeColumnIndex++).setCellValue(todayMetrics.getOrDefault("pub_id", 0.0));
            }
            if (includeAdvertiser && todayMetrics.containsKey("adv_id")) {
                row.createCell(changeColumnIndex).setCellValue(todayMetrics.getOrDefault("adv_id", 0.0));
            }
        }

        // Add yesterday's data (for comparison)
        if (!yesterdayData.isEmpty()) {
            Row row = sheet.createRow(rowIdx);
            Map<String, Double> yesterdayMetrics = yesterdayData.get(String.valueOf(lastHour));

            row.createCell(0).setCellValue(yesterday.toString());
            row.createCell(1).setCellValue(lastHour);
            row.createCell(2).setCellValue(yesterdayMetrics.getOrDefault("request", 0.0));
            row.createCell(3).setCellValue(yesterdayMetrics.getOrDefault("response", 0.0));
            row.createCell(4).setCellValue(yesterdayMetrics.getOrDefault("win", 0.0));
            row.createCell(5).setCellValue(yesterdayMetrics.getOrDefault("impression", 0.0));
            row.createCell(6).setCellValue(yesterdayMetrics.getOrDefault("revenue", 0.0));
            row.createCell(7).setCellValue(yesterdayMetrics.getOrDefault("cost", 0.0));
        }
    }

    private Map<String, Map<String, Double>> fetchLastHourData(String query, LocalDate date, int hour)
            throws SQLException {
        String modifiedQuery = query.replace(
                "WHERE DATE(tx_date) = ?",
                "WHERE DATE(tx_date) = ? AND HOUR(tx_date) = ?");

        Map<String, Map<String, Double>> data = new HashMap<>();

        try (Connection connection = getDatabaseConnection();
                PreparedStatement statement = connection.prepareStatement(modifiedQuery)) {
            statement.setDate(1, java.sql.Date.valueOf(date));
            statement.setInt(2, hour);

            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    String hourStr = String.valueOf(resultSet.getInt("hour"));
                    Map<String, Double> metrics = new HashMap<>();

                    ResultSetMetaData metaData = resultSet.getMetaData();
                    for (int i = 2; i <= metaData.getColumnCount(); i++) {
                        String columnName = metaData.getColumnName(i).toLowerCase();
                        metrics.put(columnName, resultSet.getDouble(i));
                    }
                    data.put(hourStr, metrics);
                }
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Error fetching last hour data", e);
            throw e;
        }

        return data;
    }

    private double calculatePercentageChange(Double todayValue, Double yesterdayValue) {
        if (yesterdayValue == 0)
            return todayValue == 0 ? 0 : 100;
        return ((todayValue - yesterdayValue) / yesterdayValue) * 100;
    }

    public Map<String, Double> getMetricsForTimeRange(LocalDateTime start, LocalDateTime end) {
        String query = """
            SELECT 
                SUM(request) AS total_requests,
                SUM(response) AS total_responses,
                SUM(win) AS total_wins,
                SUM(impression) AS total_impressions,
                SUM(revenue) AS total_revenue,
                SUM(cost) AS total_cost
            FROM _tx_hourly_transactions
            WHERE tx_date BETWEEN ? AND ?
        """;
    
        Map<String, Double> metrics = new HashMap<>();
    
        try (Connection connection = getDatabaseConnection();
             PreparedStatement statement = connection.prepareStatement(query)) {
    
            statement.setTimestamp(1, Timestamp.valueOf(start));
            statement.setTimestamp(2, Timestamp.valueOf(end));
    
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    for (int i = 1; i <= metaData.getColumnCount(); i++) {
                        String columnName = metaData.getColumnName(i).toLowerCase();
                        metrics.put(columnName, resultSet.getDouble(i));
                    }
                }
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Error retrieving metrics for time range", e);
            throw new RuntimeException("Failed to retrieve metrics", e);
        }
    
        return metrics;
    }
}
