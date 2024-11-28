package com.yourcompany.perhourcron;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.poi.ss.usermodel.BorderStyle;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

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
        sheet.setDefaultColumnWidth(15);

        // Create cell styles
        CellStyle headerStyle = createHeaderStyle(workbook);
        CellStyle dataStyle = createDataStyle(workbook);
        CellStyle positiveChangeStyle = createChangeStyle(workbook, true);
        CellStyle negativeChangeStyle = createChangeStyle(workbook, false);
        CellStyle neutralChangeStyle = createNeutralChangeStyle(workbook);

        // Define metrics columns
        String[] metricColumns = {
                "Request", "Response", "Win", "Impression", "Revenue", "Cost"
        };

        // Add headers to sheet
        Row headerRow = sheet.createRow(0);
        int colIdx = 0;

        // Optional additional columns at the start if needed
        if (includePublisher) {
            Cell pubHeaderCell = headerRow.createCell(colIdx++);
            pubHeaderCell.setCellValue("Publisher");
            pubHeaderCell.setCellStyle(headerStyle);
        }
        if (includeAdvertiser) {
            Cell advHeaderCell = headerRow.createCell(colIdx++);
            advHeaderCell.setCellValue("Advertiser");
            advHeaderCell.setCellStyle(headerStyle);
        }

        // Metrics headers with reordered columns
        for (String metric : metricColumns) {
            String[] subHeaders = {
                    metric + " (Yesterday)",
                    metric + " (Today)",
                    metric + " Change (%)"
            };

            for (String subHeader : subHeaders) {
                Cell metricHeaderCell = headerRow.createCell(colIdx++);
                metricHeaderCell.setCellValue(subHeader);
                metricHeaderCell.setCellStyle(headerStyle);
            }
        }

        // Fetch data for the last hour of today and yesterday
        int lastHour = currentHour - 1; // Previous hour
        Map<String, Map<String, Double>> todayData = fetchLastHourData(query, today, lastHour);
        Map<String, Map<String, Double>> yesterdayData = fetchLastHourData(query, yesterday, lastHour);

        // Ensure data exists with zero values if empty
        todayData = todayData.isEmpty() ? createEmptyDataMap() : todayData;
        yesterdayData = yesterdayData.isEmpty() ? createEmptyDataMap() : yesterdayData;

        // Add data row
        Row row = sheet.createRow(1);
        colIdx = 0;

        // Optional additional columns at the start
        if (includePublisher) {
            Cell pubCell = row.createCell(colIdx++);
            pubCell.setCellValue(todayData.get("0").getOrDefault("pub_id", 0.0));
            pubCell.setCellStyle(dataStyle);
        }
        if (includeAdvertiser) {
            Cell advCell = row.createCell(colIdx++);
            advCell.setCellValue(todayData.get("0").getOrDefault("adv_id", 0.0));
            advCell.setCellStyle(dataStyle);
        }

        // Process metrics
        String[] metricKeys = { "request", "response", "win", "impression", "revenue", "cost" };
        for (int i = 0; i < metricKeys.length; i++) {
            // Get today's and yesterday's values
            double yesterdayValue = yesterdayData.get("0").getOrDefault(metricKeys[i], 0.0);
            double todayValue = todayData.get("0").getOrDefault(metricKeys[i], 0.0);

            // Yesterday's value cell
            Cell yesterdayValueCell = row.createCell(colIdx++);
            yesterdayValueCell.setCellValue(yesterdayValue);
            yesterdayValueCell.setCellStyle(dataStyle);

            // Today's value cell
            Cell todayValueCell = row.createCell(colIdx++);
            todayValueCell.setCellValue(todayValue);
            todayValueCell.setCellStyle(dataStyle);

            // Percentage change cell
            double change = calculatePercentageChange(todayValue, yesterdayValue);
            Cell changeCell = row.createCell(colIdx++);
            changeCell.setCellValue(change);

            // Style change cell based on value
            CellStyle changeStyle = change > 0 ? positiveChangeStyle
                    : change < 0 ? negativeChangeStyle : neutralChangeStyle;
            changeCell.setCellStyle(changeStyle);
        }
    }

    // Assumed helper method for percentage change calculation

    private Map<String, Map<String, Double>> createEmptyDataMap() {
        Map<String, Map<String, Double>> emptyMap = new HashMap<>();
        Map<String, Double> zeroMetrics = new HashMap<>();
        String[] metricKeys = { "request", "response", "win", "impression", "revenue", "cost", "pub_id", "adv_id" };
        for (String key : metricKeys) {
            zeroMetrics.put(key, 0.0);
        }
        emptyMap.put("0", zeroMetrics);
        return emptyMap;
    }

    private CellStyle createHeaderStyle(Workbook workbook) {
        CellStyle style = workbook.createCellStyle();
        Font font = workbook.createFont();
        font.setBold(true);
        font.setColor(IndexedColors.WHITE.getIndex());
        style.setFont(font);
        style.setFillForegroundColor(IndexedColors.DARK_BLUE.getIndex());
        style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        style.setAlignment(HorizontalAlignment.CENTER);
        style.setBorderBottom(BorderStyle.THIN);
        style.setBorderTop(BorderStyle.THIN);
        style.setBorderLeft(BorderStyle.THIN);
        style.setBorderRight(BorderStyle.THIN);
        return style;
    }

    private CellStyle createDataStyle(Workbook workbook) {
        CellStyle style = workbook.createCellStyle();
        style.setAlignment(HorizontalAlignment.RIGHT);
        style.setBorderBottom(BorderStyle.THIN);
        style.setBorderTop(BorderStyle.THIN);
        style.setBorderLeft(BorderStyle.THIN);
        style.setBorderRight(BorderStyle.THIN);
        return style;
    }

    private CellStyle createChangeStyle(Workbook workbook, boolean positive) {
        CellStyle style = workbook.createCellStyle();
        Font font = workbook.createFont();
        font.setColor(positive ? IndexedColors.GREEN.getIndex() : IndexedColors.RED.getIndex());
        style.setFont(font);
        style.setAlignment(HorizontalAlignment.RIGHT);
        style.setBorderBottom(BorderStyle.THIN);
        style.setBorderTop(BorderStyle.THIN);
        style.setBorderLeft(BorderStyle.THIN);
        style.setBorderRight(BorderStyle.THIN);
        return style;
    }

    private CellStyle createNeutralChangeStyle(Workbook workbook) {
        CellStyle style = workbook.createCellStyle();
        Font font = workbook.createFont();
        font.setColor(IndexedColors.GREY_50_PERCENT.getIndex());
        style.setFont(font);
        style.setAlignment(HorizontalAlignment.RIGHT);
        style.setBorderBottom(BorderStyle.THIN);
        style.setBorderTop(BorderStyle.THIN);
        style.setBorderLeft(BorderStyle.THIN);
        style.setBorderRight(BorderStyle.THIN);
        return style;
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
                    Map<String, Double> metrics = new HashMap<>();
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    for (int i = 1; i <= metaData.getColumnCount(); i++) {
                        String columnName = metaData.getColumnName(i).toLowerCase();
                        double value = resultSet.getDouble(i);
                        metrics.put(columnName, value);
                    }
                    data.put("0", metrics); // Use a fixed key to ensure data is retrieved
                }
            }

            // If no data found, return an empty map with zero values
            if (data.isEmpty()) {
                Map<String, Double> zeroMetrics = new HashMap<>();
                String[] metricKeys = { "request", "response", "win", "impression", "revenue", "cost" };
                for (String key : metricKeys) {
                    zeroMetrics.put(key, 0.0);
                }
                data.put("0", zeroMetrics);
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Error fetching last hour data for date: " + date + ", hour: " + hour, e);
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
