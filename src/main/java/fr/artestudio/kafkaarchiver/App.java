package fr.artestudio.kafkaarchiver;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

import fi.iki.elonen.NanoHTTPD;

public class App extends NanoHTTPD {

    private static final Logger log = LoggerFactory.getLogger(App.class);
    private static String currentDateString = null;
    private static Date lastDate = null;
    private static long numberOfMessages = 0;
    private static final Configuration conf = new Configuration();
    private static final DateFormat fileDateFormatter = new SimpleDateFormat("yyyy-MM-dd", Locale.US);

    public App() throws IOException {
	super(8080);
        start(NanoHTTPD.SOCKET_READ_TIMEOUT, false);
    }

    public static void main(String[] argv) throws Exception {
	if (argv.length != 9) {
	    System.err.printf(
		    "Usage: %s <server> <topicName> <groupId> <workDirectory> <awsAccessKey> <awsSecret> <awsBucket> <awsPath> <earliest>\n",
		    App.class.getSimpleName());
	    System.exit(-1);
	}
	log.info("Starting ...");
	new App();
	log.info("Starting http server...");

	conf.server = argv[0];
	conf.topicName = argv[1];
	conf.groupName = argv[2];
	conf.workDirectory = argv[3];
	conf.awsAccessKey = argv[4];
	conf.awsSecret = argv[5];
	conf.awsBucket = argv[6];
	conf.awsPath = argv[7];
	conf.earliest = argv[8].equals("earliest");

	ConsumerThread consumerRunnable = new ConsumerThread(conf);
	consumerRunnable.start();

	new Thread() {
	    public void run() {
		while (true) {
		    try {
			Thread.sleep(10000);
		    } catch (InterruptedException e) {
		    }
		    if (currentDateString != null)
			log.info("Current progress: " + currentDateString + " with " + numberOfMessages
				+ " messages readed");
		}
	    }
	}.start();

	consumerRunnable.join();
    }

    private static class Configuration {
	private String server;
	private String topicName;
	private String groupName;
	private String awsAccessKey;
	private String awsSecret;
	private String awsBucket;
	private String awsPath;
	private String workDirectory;
	private boolean earliest;
    }

    private static class ConsumerThread extends Thread {

	private KafkaConsumer<String, String> kafkaConsumer;
	private Configuration conf;

	public ConsumerThread(Configuration conf) {
	    this.conf = conf;
	}

	public void run() {
	    log.info("Connecting...");
	    Properties configProperties = new Properties();
	    configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, conf.server);
	    configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
		    "org.apache.kafka.common.serialization.StringDeserializer");
	    configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
		    "org.apache.kafka.common.serialization.StringDeserializer");
	    configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, conf.groupName);
	    configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "simple");
	    if (conf.earliest)
		configProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

	    kafkaConsumer = new KafkaConsumer<String, String>(configProperties);
	    kafkaConsumer.subscribe(Arrays.asList(conf.topicName));
	    DateFormat dateFormatter = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss.S", Locale.US);
	    Calendar cal = new GregorianCalendar();
	    // log.info(dateFormatter.format(new Date()));
	    // Pattern p = Pattern.compile("\\[\\]");
	    // Matcher m = p.matcher(s);

	    try {
		while (true) {
		    ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
		    for (ConsumerRecord<String, String> record : records) {
			String value = record.value();
			value = value.replaceAll("  ", " ");
			String[] values = value.split(" ");
			String dateString = values[6].substring(1, values[6].length() - 1);
			Date date = null;
			String filename = null;
			try {
			    date = dateFormatter.parse(dateString);
			    filename = formatFilename(date);
			    cal.setTime(date);
			    if (lastDate != null
				    && !fileDateFormatter.format(lastDate).equals(fileDateFormatter.format(date))
				    // needed to wait a little for partitioned data that may not come ordered from kafka
				    && cal.get(Calendar.HOUR_OF_DAY) > 2
				    && lastDate.before(date)) {
				// we pack and send the file for yesterday datas
				log.info("packing file since lastDate is {} and currentDate is {}", dateFormatter.format(lastDate), dateFormatter.format(date));
				cal.add(Calendar.DAY_OF_YEAR, -1);
				packAndSendCurrentFile(cal.getTime());
				
				Thread.sleep(1000);
				lastDate = date;
				Thread.sleep(1000);
				
				// check if an older file may not have been sent by
				// a previous instance of the process
				cal.add(Calendar.DAY_OF_YEAR, -1);
				String oldFilename = formatFilename(cal.getTime());
				if (new File(oldFilename).exists()) {
				    log.info("older file {} has not been sent by a previous instance of the process, sending it");
				    packAndSendCurrentFile(cal.getTime());
				}
			    }
			    
			    currentDateString = dateString;
			    if (lastDate == null)
				lastDate = date;
			} catch (Exception e) {
			    continue;
			}
			try (FileWriter fw = new FileWriter(new File(filename), true)) {
			    fw.write(record.value());
			}
			numberOfMessages++;
		    }
		}
	    } catch (Exception ex) {
		throw new RuntimeException(ex);
	    } finally {
		kafkaConsumer.close();
		log.info("After closing KafkaConsumer");
	    }
	}
    }

    public synchronized static void packAndSendCurrentFile(Date date) {
	String filename = formatFilename(date);
	log.info("packAndSendCurrentFile(" + filename + ")");
	File zipFile = new File(filename + ".zip");
	File logFile = new File(filename);
	try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(zipFile));
		FileInputStream fis = new FileInputStream(logFile)) {
	    ZipEntry entry = new ZipEntry(StringUtils.substringAfterLast(filename, File.separator));
	    zos.putNextEntry(entry);
	    IOUtils.copy(fis, zos);
	    zos.closeEntry();
	    zos.flush();
	    zos.close();
	} catch (IOException e) {
	    log.error("cannot pack and send current file " + filename, e);
	}
	AWSCredentials credentials = new BasicAWSCredentials(conf.awsAccessKey, conf.awsSecret);
	AmazonS3 s3client = new AmazonS3Client(credentials);
	s3client.putObject(conf.awsBucket, conf.awsPath + zipFile.getName(), zipFile);
	zipFile.delete();
	logFile.delete();
    }

    private static String formatFilename(Date date) {
	return conf.workDirectory + File.separator + "data-" + fileDateFormatter.format(date) + ".log";
    }

    @Override
    public Response serve(IHTTPSession session) {
	String msg = "<html><body><h1>Ok</h1></body></html>";
	return newFixedLengthResponse(msg);
    }
}
