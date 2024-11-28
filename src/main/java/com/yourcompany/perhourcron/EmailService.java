package com.yourcompany.perhourcron;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.function.BiFunction;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.*;

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
        private static final String[] RECIPIENTS = { "dipankar.tripathi@verse.in" };// ,temp-manish.rajawat@verse.in,anand.kumar@nexverse.ai

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
                        Map<String, Double> todayLastHourMetrics = safeGetMetrics(databaseService, lastHourStart,
                                        lastHourEnd);
                        Map<String, Double> yesterdayLastHourMetrics = safeGetMetrics(databaseService,
                                        previousDayLastHourStart,
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
                                        todayLastHourMetrics, // Pass dataMetrics as another map if needed
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
                        LOGGER.log(Level.WARNING, "Error retrieving metrics for time range: " + start + " to " + end,
                                        e);
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
                        Map<String, Double> dataMetrics,
                        LocalDateTime lastHourStart) throws MessagingException, IOException {

                Message message = new MimeMessage(session);
                message.setFrom(new InternetAddress(USERNAME));
                message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(String.join(",", RECIPIENTS)));
                message.setSubject("Comprehensive Metrics Report - Last Hour Ending "
                                + lastHourStart.plusHours(1).format(DateTimeFormatter.ofPattern("HH:mm 'UTC'")));

                Multipart multipart = new MimeMultipart();

                // Email content
                MimeBodyPart htmlPart = new MimeBodyPart();
                htmlPart.setContent(
                                createHtmlSummary(todayMetrics, yesterdayMetrics, dataMetrics, lastHourStart),
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

        private String createHtmlSummary(Map<String, Double> todayMetrics, Map<String, Double> yesterdayMetrics,
                        Map<String, Double> dataMetrics, LocalDateTime lastHourStart) {
                String formattedHour = lastHourStart.format(DateTimeFormatter.ofPattern("HH:mm 'to' HH:mm 'UTC'"));
                String formattedDate = lastHourStart.format(DateTimeFormatter.ofPattern("dd MMM yyyy"));

                StringBuilder html = new StringBuilder();
                html.append("<!DOCTYPE html>")
                                .append("<html lang='en'>")
                                .append("<head>")
                                .append("<meta charset='UTF-8'>")
                                .append("<meta name='viewport' content='width=device-width, initial-scale=1.0'>")
                                .append("<title>Hourly Metrics Report</title>")
                                .append("<style>")
                                .append("body { font-family: 'Arial', 'Helvetica', sans-serif; line-height: 1.6; color: #333; ")
                                .append("      max-width: 800px; margin: 0 auto; padding: 20px; background-color: #f4f4f4; }")
                                .append(".container { background-color: white; border-radius: 8px; ")
                                .append("            box-shadow: 0 4px 6px rgba(0,0,0,0.1); padding: 30px; }")
                                .append("h1 { color: #1a73e8; text-align: center; border-bottom: 3px solid #1a73e8; padding-bottom: 10px; ")
                                .append("     font-weight: bold; }")
                                .append("h2 { color: #34a853; font-weight: bold; }")
                                .append(".summary { background-color: #f0f0f0; border-radius: 5px; padding: 15px; margin-bottom: 20px; }")
                                .append("table { width: 100%; border-collapse: separate; border-spacing: 0; margin-top: 20px; ")
                                .append("        box-shadow: 0 2px 3px rgba(0,0,0,0.1); }")
                                .append("th, td { border: 1px solid #e0e0e0; padding: 12px; text-align: right; font-weight: bold; }")
                                .append("th { background-color: #f8f9fa; color: #495057; }")
                                .append("tr:nth-child(even) { background-color: #f2f2f2; }")
                                .append("tr:hover { background-color: #e9ecef; transition: background-color 0.3s ease; }")
                                .append(".positive { color: #28a745; }")
                                .append(".negative { color: #dc3545; }")
                                .append(".neutral { color: #6c757d; }")
                                .append(".metric-trend { font-size: 0.9em; margin-left: 10px; }")
                                .append(".trend-up { color: green; }")
                                .append(".trend-down { color: red; }")
                                .append(".trend-neutral { color: gray; }")
                                .append("</style>")
                                .append("</head>")
                                .append("<body>")
                                .append("<div class='container'>")
                                .append("<h1>Hourly Metrics Report</h1>")
                                .append("<h2>Date: ").append(formattedDate).append(" | Time: ").append(formattedHour)
                                .append("</h2>");

                // Add a summary section with observations
                html.append("<div class='summary'>");
                List<String> observations = generateObservations(todayMetrics, yesterdayMetrics);
                html.append("<h3>Key Observations</h3>");
                html.append("<ul>");
                for (String observation : observations) {
                        html.append("<li>").append(observation).append("</li>");
                }
                html.append("</ul>");
                html.append("</div>");

                // Metrics table
                html.append("<table>");
                html.append("<tr><th>Metric</th><th>Yesterday</th><th>Today</th><th>Change %</th><th>Trend</th></tr>");

                String[][] metricsToTrack = {
                                { "total_requests", "Requests" },
                                { "total_responses", "Responses" },
                                { "total_wins", "Wins" },
                                { "total_impressions", "Impressions" },
                                { "total_revenue", "Revenue" },
                                { "total_cost", "Cost" }
                };

                BiFunction<Map<String, Double>, String, Double> getMetric = (map, key) -> {
                        Double value = map.get(key);
                        return value != null ? value : 0.0;
                };

                for (String[] metricInfo : metricsToTrack) {
                        String metricKey = metricInfo[0];
                        String metricName = metricInfo[1];
                        double yesterdayValue = getMetric.apply(yesterdayMetrics, metricKey);
                        double todayValue = getMetric.apply(todayMetrics, metricKey);
                        double percentChange = calculatePercentage(todayValue, yesterdayValue);

                        String trendIcon = percentChange > 0 ? "▲" : percentChange < 0 ? "▼" : "—";
                        String trendClass = percentChange > 0 ? "trend-up"
                                        : percentChange < 0 ? "trend-down" : "trend-neutral";
                        String percentageClass = percentChange > 0 ? "positive"
                                        : percentChange < 0 ? "negative" : "neutral";

                        html.append(String.format(
                                        "<tr>" +
                                                        "<td><strong>%s</strong></td>" +
                                                        "<td>%.2f</td>" +
                                                        "<td>%.2f</td>" +
                                                        "<td class='%s'><strong>%+.2f%%</strong></td>" +
                                                        "<td class='metric-trend %s'>%s</td>" +
                                                        "</tr>",
                                        metricName,
                                        yesterdayValue,
                                        todayValue,
                                        percentageClass,
                                        percentChange,
                                        trendClass,
                                        trendIcon));
                }

                html.append("</table>")
                                .append("</div>")
                                .append("</body>")
                                .append("</html>");

                return html.toString();
        }

        private List<String> generateObservations(Map<String, Double> todayMetrics,
                        Map<String, Double> yesterdayMetrics) {
                List<String> observations = new ArrayList<>();

                // Helper function to get metric values safely
                BiFunction<Map<String, Double>, String, Double> getMetric = (map, key) -> {
                        Double value = map.get(key);
                        return value != null ? value : 0.0;
                };

                // Fetch metric values
                double todayRequests = getMetric.apply(todayMetrics, "total_requests");
                double yesterdayRequests = getMetric.apply(yesterdayMetrics, "total_requests");

                double todayResponses = getMetric.apply(todayMetrics, "total_responses");
                double yesterdayResponses = getMetric.apply(yesterdayMetrics, "total_responses");

                double todayWins = getMetric.apply(todayMetrics, "total_wins");
                double yesterdayWins = getMetric.apply(yesterdayMetrics, "total_wins");

                double todayImpressions = getMetric.apply(todayMetrics, "total_impressions");
                double yesterdayImpressions = getMetric.apply(yesterdayMetrics, "total_impressions");

                double todayRevenue = getMetric.apply(todayMetrics, "total_revenue");
                double yesterdayRevenue = getMetric.apply(yesterdayMetrics, "total_revenue");

                double todayCost = getMetric.apply(todayMetrics, "total_cost");
                double yesterdayCost = getMetric.apply(yesterdayMetrics, "total_cost");

                // Request observations
                double requestChange = calculatePercentage(todayRequests, yesterdayRequests);
                if (Math.abs(requestChange) > 10) {
                        observations.add(String.format("Requests %s by %.2f%% compared to yesterday",
                                        requestChange > 0 ? "increased" : "decreased", Math.abs(requestChange)));
                }

                // Response rate observations
                double responseRate = todayRequests > 0 ? (todayResponses / todayRequests) * 100 : 0;
                double yesterdayResponseRate = yesterdayRequests > 0 ? (yesterdayResponses / yesterdayRequests) * 100
                                : 0;
                double responseRateChange = responseRate - yesterdayResponseRate;
                if (Math.abs(responseRateChange) > 5) {
                        observations.add(String.format("Response rate %s by %.2f percentage points",
                                        responseRateChange > 0 ? "improved" : "declined",
                                        Math.abs(responseRateChange)));
                }

                // Win rate observations
                double winRate = todayResponses > 0 ? (todayWins / todayResponses) * 100 : 0;
                double yesterdayWinRate = yesterdayResponses > 0 ? (yesterdayWins / yesterdayResponses) * 100 : 0;
                double winRateChange = winRate - yesterdayWinRate;
                if (Math.abs(winRateChange) > 5) {
                        observations.add(String.format("Win rate %s by %.2f percentage points",
                                        winRateChange > 0 ? "increased" : "decreased", Math.abs(winRateChange)));
                }

                // Revenue observations
                double revenueChange = calculatePercentage(todayRevenue, yesterdayRevenue);
                if (Math.abs(revenueChange) > 10) {
                        observations.add(String.format("Revenue %s by %.2f%% compared to yesterday",
                                        revenueChange > 0 ? "increased" : "decreased", Math.abs(revenueChange)));
                }

                // Cost efficiency observations
                double yesterdayCPC = yesterdayImpressions > 0 ? yesterdayCost / yesterdayImpressions : 0;
                double todayCPC = todayImpressions > 0 ? todayCost / todayImpressions : 0;
                double cpcChange = calculatePercentage(todayCPC, yesterdayCPC);
                if (Math.abs(cpcChange) > 10) {
                        observations.add(String.format("Cost per impression %s by %.2f%% compared to yesterday",
                                        cpcChange > 0 ? "increased" : "decreased", Math.abs(cpcChange)));
                }

                // Advanced observations
                if (todayRequests > 0 && yesterdayRequests > 0) {
                        double requestChangeRate = calculatePercentage(todayRequests, yesterdayRequests);
                        observations.add(requestChangeRate > 0
                                        ? String.format("🚀 Traffic surge: Requests increased by %.2f%% compared to yesterday",
                                                        requestChangeRate)
                                        : String.format("📉 Traffic dip: Requests decreased by %.2f%% compared to yesterday",
                                                        Math.abs(requestChangeRate)));
                }

                double profitMargin = (todayRevenue - todayCost) / todayRevenue * 100;
                double yesterdayProfitMargin = (yesterdayRevenue - yesterdayCost) / yesterdayRevenue * 100;

                if (!Double.isNaN(profitMargin) && !Double.isNaN(yesterdayProfitMargin)) {
                        double profitMarginChange = profitMargin - yesterdayProfitMargin;
                        observations.add(profitMarginChange > 0
                                        ? String.format("💰 Profitability boost: Profit margin improved by %.2f percentage points",
                                                        profitMarginChange)
                                        : String.format("💸 Profitability concern: Profit margin decreased by %.2f percentage points",
                                                        Math.abs(profitMarginChange)));
                }

                // Fallback if no significant observations
                if (observations.isEmpty()) {
                        observations.add("No significant changes observed in key metrics compared to yesterday.");
                }

                return observations;
        }

        private double calculatePercentage(double currentValue, double previousValue) {
                if (previousValue == 0) {
                        return currentValue == 0 ? 0 : 100;
                }
                return ((currentValue - previousValue) / previousValue) * 100;
        }
}
