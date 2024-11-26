package com.yourcompany.perhourcron;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

public class EmailService {

    private static final Logger LOGGER = Logger.getLogger(EmailService.class.getName());

    // Hardcoded SMTP and user credentials
    private static final String SMTP_HOST = "smtp.gmail.com";
    private static final String SMTP_PORT = "587";
    private static final String USERNAME = "dipankar.tripathi@verse.in";
    private static final String PASSWORD = "yaozlkosznvcltiv";
    private static final String[] RECIPIENTS = { "dipankar.tripathi@verse.in" };

    public void sendHourlyReport(DatabaseService databaseService) {
        LOGGER.info("Starting email process for last hour report...");

        try {
            // Generate report file
            String reportPath = databaseService.generateComprehensiveHourlyReports();
            if (reportPath == null || reportPath.isEmpty()) {
                LOGGER.warning("Report generation failed. Skipping email.");
                return;
            }

            File reportFile = new File(reportPath);
            if (!reportFile.exists() || !reportFile.isFile()) {
                LOGGER.warning("Report file does not exist: " + reportPath);
                return;
            }

            // Define time ranges for the last hour and the same hour yesterday
            LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
            LocalDateTime lastHourStart = now.minusHours(1).withMinute(0).withSecond(0).withNano(0);
            LocalDateTime lastHourEnd = now.withMinute(0).withSecond(0).withNano(0);
            LocalDateTime previousDayLastHourStart = lastHourStart.minusDays(1);
            LocalDateTime previousDayLastHourEnd = lastHourEnd.minusDays(1);

            // Fetch metrics safely
            Map<String, Double> todayLastHourMetrics = safeGetMetrics(databaseService, lastHourStart, lastHourEnd);
            Map<String, Double> yesterdayLastHourMetrics = safeGetMetrics(databaseService, previousDayLastHourStart,
                    previousDayLastHourEnd);

            // Set up email properties
            Properties props = configureMailProperties();
            Session session = createMailSession(props);

            // Build and send the email
            Message message = createMessageWithSummary(
                    session,
                    reportFile,
                    todayLastHourMetrics,
                    yesterdayLastHourMetrics,
                    lastHourStart);

            try (Transport transport = session.getTransport("smtp")) {
                transport.connect(SMTP_HOST, USERNAME, PASSWORD);
                transport.sendMessage(message, message.getAllRecipients());
                LOGGER.info("Email sent successfully with the last hour report.");
            }

        } catch (MessagingException e) {
            LOGGER.log(Level.SEVERE, "SMTP Error: " + e.getMessage(), e);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "File Error: " + e.getMessage(), e);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Unexpected error during email process", e);
        }
    }

    private Map<String, Double> safeGetMetrics(DatabaseService databaseService, LocalDateTime start,
            LocalDateTime end) {
        try {
            return databaseService.getMetricsForTimeRange(start, end);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error retrieving metrics for time range: " + start + " to " + end, e);
            return Collections.emptyMap();
        }
    }

    private Properties configureMailProperties() {
        Properties props = new Properties();
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.smtp.host", SMTP_HOST);
        props.put("mail.smtp.port", SMTP_PORT);
        return props;
    }

    private Session createMailSession(Properties props) {
        return Session.getInstance(props, new Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(USERNAME, PASSWORD);
            }
        });
    }

    private Message createMessageWithSummary(
            Session session,
            File reportFile,
            Map<String, Double> todayMetrics,
            Map<String, Double> yesterdayMetrics,
            LocalDateTime lastHourStart) throws MessagingException, IOException {

        Message message = new MimeMessage(session);
        message.setFrom(new InternetAddress(USERNAME));
        message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(String.join(",", RECIPIENTS)));
        message.setSubject("Hourly Metrics Report - Last Hour Ending "
                + lastHourStart.plusHours(1).format(DateTimeFormatter.ofPattern("HH:mm 'UTC'")));

        Multipart multipart = new MimeMultipart();

        // Email content
        MimeBodyPart htmlPart = new MimeBodyPart();
        htmlPart.setContent(
                createHtmlSummary(todayMetrics, yesterdayMetrics, lastHourStart),
                "text/html; charset=utf-8");
        multipart.addBodyPart(htmlPart);

        // Attach the report
        MimeBodyPart attachmentPart = new MimeBodyPart();
        attachmentPart.attachFile(reportFile);
        attachmentPart.setFileName(reportFile.getName());
        multipart.addBodyPart(attachmentPart);

        message.setContent(multipart);
        return message;
    }

    private String createHtmlSummary(
            Map<String, Double> todayMetrics,
            Map<String, Double> yesterdayMetrics,
            LocalDateTime lastHourStart) {

        String formattedHour = lastHourStart.format(DateTimeFormatter.ofPattern("HH:mm 'to' HH:mm 'UTC'"));
        StringBuilder html = new StringBuilder();
        html.append("<html><head><style>")
                .append("body { font-family: Arial, sans-serif; }")
                .append("table { border-collapse: collapse; width: 100%; }")
                .append("th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }")
                .append("</style></head><body>");
        html.append("<h1>Metrics Summary for Last Hour (" + formattedHour + ")</h1>");

        // Metrics for one hour
        double todayRequests = todayMetrics.getOrDefault("total_requests", 0.0);
        double yesterdayRequests = yesterdayMetrics.getOrDefault("total_requests", 0.0);
        double requestsChange = calculatePercentage(todayRequests, yesterdayRequests);

        double todayResponse = todayMetrics.getOrDefault("total_response", 0.0);
        double yesterdayResponse = yesterdayMetrics.getOrDefault("total_response", 0.0);
        double responseChange = calculatePercentage(todayResponse, yesterdayResponse);

        double todayWin = todayMetrics.getOrDefault("total_win", 0.0);
        double yesterdayWin = yesterdayMetrics.getOrDefault("total_win", 0.0);
        double winChange = calculatePercentage(todayWin, yesterdayWin);

        double todayImpressions = todayMetrics.getOrDefault("total_impressions", 0.0);
        double yesterdayImpressions = yesterdayMetrics.getOrDefault("total_impressions", 0.0);
        double impressionsChange = calculatePercentage(todayImpressions, yesterdayImpressions);

        double todayRevenue = todayMetrics.getOrDefault("total_revenue", 0.0);
        double yesterdayRevenue = yesterdayMetrics.getOrDefault("total_revenue", 0.0);
        double revenueChange = calculatePercentage(todayRevenue, yesterdayRevenue);

        double todayCost = todayMetrics.getOrDefault("total_cost", 0.0);
        double yesterdayCost = yesterdayMetrics.getOrDefault("total_cost", 0.0);
        double costChange = calculatePercentage(todayCost, yesterdayCost);

        // Start of the table for the current hour
        html.append("<h2>Metrics for One Hour</h2>");

        // Requests and Response table
        html.append("<h3>Requests and Response</h3><table>");
        html.append(
                "<tr><th>Hours</th><th>Yesterday</th><th>Today</th><th>Delta</th><th>Yesterday</th><th>Today</th><th>Delta</th></tr>");
        html.append(String.format(
                "<tr><td>1</td><td>%s</td><td>%s</td><td>%+.2f%%</td><td>%s</td><td>%s</td><td>%+.2f%%</td></tr>",
                yesterdayRequests, todayRequests, requestsChange, yesterdayResponse, todayResponse, responseChange));
        html.append("</table>");

        // Win and Impressions table
        html.append("<h3>Win and Impressions</h3><table>");
        html.append(
                "<tr><th>Hours</th><th>Yesterday</th><th>Today</th><th>Delta</th><th>Yesterday</th><th>Today</th><th>Delta</th></tr>");
        html.append(String.format(
                "<tr><td>1</td><td>%s</td><td>%s</td><td>%+.2f%%</td><td>%s</td><td>%s</td><td>%+.2f%%</td></tr>",
                yesterdayWin, todayWin, winChange, yesterdayImpressions, todayImpressions, impressionsChange));
        html.append("</table>");

        // Revenue and Cost table
        html.append("<h3>Revenue and Cost</h3><table>");
        html.append(
                "<tr><th>Hours</th><th>Yesterday</th><th>Today</th><th>Delta</th><th>Yesterday</th><th>Today</th><th>Delta</th></tr>");
        html.append(String.format(
                "<tr><td>1</td><td>%s</td><td>%s</td><td>%+.2f%%</td><td>$%.2f</td><td>$%.2f</td><td>%+.2f%%</td></tr>",
                yesterdayRevenue, todayRevenue, revenueChange, yesterdayCost, todayCost, costChange));
        html.append("</table>");

        html.append("</body></html>");
        return html.toString();
    }

    private double calculatePercentage(double currentValue, double previousValue) {
        if (previousValue == 0) {
            return currentValue == 0 ? 0 : 100;
        }
        return ((currentValue - previousValue) / previousValue) * 100;
    }
}
