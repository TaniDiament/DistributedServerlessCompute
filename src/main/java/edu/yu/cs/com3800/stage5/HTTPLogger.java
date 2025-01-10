package edu.yu.cs.com3800.stage5;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * Logger class that returns the path of the logger output with initialization
 */
public class HTTPLogger {

    public LoggerWithPath initializeLogging(String fileNamePreface) throws IOException {
        return initializeLogging(fileNamePreface,false);
    }
    public LoggerWithPath initializeLogging(String fileNamePreface, boolean disableParentHandlers) throws IOException {
        if(fileNamePreface == null){
            fileNamePreface = this.getClass().getCanonicalName() + "-" + this.hashCode();
        }
        String loggerName = this.getClass().getCanonicalName() + fileNamePreface;
        return createLogger(loggerName,fileNamePreface,disableParentHandlers);
    }

    public static LoggerWithPath createLogger(String loggerName, String fileNamePreface, boolean disableParentHandlers) throws IOException {
        Logger logger = Logger.getLogger(loggerName);
        LocalDateTime date = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-kk_mm");
        String suffix = date.format(formatter);
        String dirName = "logs-" + suffix;
        new File(".", dirName + File.separator).mkdirs();
        String logFilePath = dirName + File.separator + fileNamePreface + "-Log.txt";
        FileHandler fh = new FileHandler(logFilePath);
        fh.setFormatter(new SimpleFormatter());
        logger.addHandler(fh);
        logger.setLevel(Level.ALL);
        if (disableParentHandlers) {
            logger.setUseParentHandlers(false);
        }
        return new LoggerWithPath(logger, logFilePath);
    }

    public record LoggerWithPath(Logger logger, String logFilePath) {
    }
}
