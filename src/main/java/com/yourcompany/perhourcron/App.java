package com.yourcompany.perhourcron;

import java.util.logging.Level;
import java.util.logging.Logger;

public class App {
    private static final Logger LOGGER = Logger.getLogger(App.class.getName());

    public static void main(String[] args) {
        try {
            // Initialize services
            DatabaseService databaseService = new DatabaseService();
            EmailService emailService = new EmailService();

            // Generate and send hourly report
            emailService.sendHourlyReport(databaseService);

            LOGGER.info("Hourly reporting process completed successfully.");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to execute hourly reporting process", e);
            System.exit(1);
        }
    }
}