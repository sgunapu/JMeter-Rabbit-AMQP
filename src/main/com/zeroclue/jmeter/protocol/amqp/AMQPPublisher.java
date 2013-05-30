package com.zeroclue.jmeter.protocol.amqp;

import com.rabbitmq.client.AMQP;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.rabbitmq.client.MessageProperties;
import com.zeroclue.jmeter.protocol.amqp.gui.AMQPPublisherGui;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.Interruptible;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.services.FileServer;
import org.apache.jmeter.util.JMeterUtils;
import org.apache.jorphan.io.TextFile;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import com.rabbitmq.client.Channel;

/**
 * JMeter creates an instance of a sampler class for every occurrence of the
 * element in every thread. [some additional copies may be created before the
 * test run starts]
 *
 * Thus each sampler is guaranteed to be called by a single thread - there is no
 * need to synchronize access to instance variables.
 *
 * However, access to class fields must be synchronized.
 */
public class AMQPPublisher extends AMQPSampler implements Interruptible {

    private static final long serialVersionUID = -8420658040465788497L;

    private static final Logger log = LoggingManager.getLoggerForClass();

    //++ These are JMX names, and must not be changed
    private final static String MESSAGE = "AMQPPublisher.Message";
    private final static String MESSAGE_ROUTING_KEY = "AMQPPublisher.MessageRoutingKey";
    private final static String MESSAGE_TYPE = "AMQPPublisher.MessageType";
    private final static String REPLY_TO_QUEUE = "AMQPPublisher.ReplyToQueue";
    private final static String CORRELATION_ID = "AMQPPublisher.CorrelationId";

    public static boolean DEFAULT_PERSISTENT = false;
    private final static String PERSISTENT = "AMQPConsumer.Persistent";

    public static boolean DEFAULT_USE_TX = false;
    private final static String USE_TX = "AMQPConsumer.UseTx";

    private static final FileServer FSERVER = FileServer.getFileServer();
    private static final String MESSAGE_SOURCE = "AMQPConsumer.MessageSource";
    private static final String INPUT_FILE = "AMQPConsumer.InputFile";
    private static final String RANDOM_PATH = "AMQPConsumer.RandomPath";
    public static final String DEFAULT_MESSAGE_SOURCE = AMQPPublisherGui.USE_TEXT_RSC;

    // Cache for bytes-message, only used when parsing from a file
    private Map<String, String> fileCache = new HashMap<String, String>();

    private transient Channel channel;

    public AMQPPublisher() {
        super();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SampleResult sample(Entry e) {
        SampleResult result = new SampleResult();
        result.setSampleLabel(getName());
        result.setSuccessful(false);
        result.setResponseCode("500");

        try {
            initChannel();
        } catch (Exception ex) {
            log.error("Failed to initialize channel : ", ex);
            result.setResponseMessage(ex.toString());
            return result;
        }

        String data = getMessage(); // Sampler data

        result.setSampleLabel(getTitle());
        /*
         * Perform the sampling
         */

        // aggregate samples.
        int loop = getIterationsAsInt();
        result.sampleStart(); // Start timing
        try {
            AMQP.BasicProperties messageProperties = getProperties();
            byte[] messageBytes = getMessageBytes();

            for (int idx = 0; idx < loop; idx++) {
                // try to force jms semantics.
                // but this does not work since RabbitMQ does not sync to disk if consumers are connected as
                // seen by iostat -cd 1. TPS value remains at 0.

                channel.basicPublish(getExchange(), getMessageRoutingKey(), messageProperties, messageBytes);

            }

            // commit the sample.
            if (getUseTx()) {
                channel.txCommit();
            }

            /*
             * Set up the sample result details
             */
            result.setSamplerData(data);
            result.setResponseData("OK", null);
            result.setDataType(SampleResult.TEXT);

            result.setResponseCodeOK();
            result.setResponseMessage("OK");
            result.setSuccessful(true);
        } catch (Exception ex) {
            log.debug(ex.getMessage(), ex);
            result.setResponseCode("000");
            result.setResponseMessage(ex.toString());
        }
        finally {
            result.sampleEnd(); // End timimg
        }

        return result;
    }


    private byte[] getMessageBytes() {
        return getMessageContent().getBytes();
    }

    /**
     * @return the message routing key for the sample
     */
    public String getMessageRoutingKey() {
        return getPropertyAsString(MESSAGE_ROUTING_KEY);
    }

    public void setMessageRoutingKey(String content) {
        setProperty(MESSAGE_ROUTING_KEY, content);
    }

    /**
     * @return the message for the sample
     */
    public String getMessage() {
        return getPropertyAsString(MESSAGE);
    }

    public void setMessage(String content) {
        setProperty(MESSAGE, content);
    }

    /**
     * @return the message type for the sample
     */
    public String getMessageType() {
        return getPropertyAsString(MESSAGE_TYPE);
    }

    public void setMessageType(String content) {
        setProperty(MESSAGE_TYPE, content);
    }

    /**
     * @return the reply-to queue for the sample
     */
    public String getReplyToQueue() {
        return getPropertyAsString(REPLY_TO_QUEUE);
    }

    public void setReplyToQueue(String content) {
        setProperty(REPLY_TO_QUEUE, content);
    }

    /**
     * @return the correlation identifier for the sample
     */
    public String getCorrelationId() {
        return getPropertyAsString(CORRELATION_ID);
    }

    public void setCorrelationId(String content) {
        setProperty(CORRELATION_ID, content);
    }


    public Boolean getPersistent() {
        return getPropertyAsBoolean(PERSISTENT, DEFAULT_PERSISTENT);
    }

    public void setPersistent(Boolean persistent) {
       setProperty(PERSISTENT, persistent);
    }

    public Boolean getUseTx() {
        return getPropertyAsBoolean(USE_TX, DEFAULT_USE_TX);
    }

    public void setUseTx(Boolean tx) {
       setProperty(USE_TX, tx);
    }

    /**
     * set the input file for the publisher
     *
     * @param file
     */
    public void setInputFile(String file) {
        setProperty(INPUT_FILE, file);
    }

    /**
     * return the path of the input file
     *
     */
    public String getInputFile() {
        return getPropertyAsString(INPUT_FILE);
    }

    /**
     * set the random path for the messages
     *
     * @param path
     */
    public void setRandomPath(String path) {
        setProperty(RANDOM_PATH, path);
    }

    /**
     * return the random path for messages
     *
     */
    public String getRandomPath() {
        return getPropertyAsString(RANDOM_PATH);
    }


    @Override
    public boolean interrupt() {
        cleanup();
        return true;
    }

    @Override
    protected Channel getChannel() {
        return channel;
    }

    @Override
    protected void setChannel(Channel channel) {
        this.channel = channel;
    }

    @Override
    protected AMQP.BasicProperties getProperties() {
        AMQP.BasicProperties parentProps = super.getProperties();

        int deliveryMode = getPersistent() ? 2 : 1;

        AMQP.BasicProperties publishProperties =
                new AMQP.BasicProperties(parentProps.getContentType(), parentProps.getContentEncoding(),
                parentProps.getHeaders(), deliveryMode, parentProps.getPriority(),
                getCorrelationId(), getReplyToQueue(), parentProps.getExpiration(),
                parentProps.getMessageId(), parentProps.getTimestamp(), getMessageType(),
                parentProps.getUserId(), parentProps.getAppId(), parentProps.getClusterId());

        return publishProperties;
    }

    protected boolean initChannel() throws IOException{
        boolean ret = super.initChannel();
        if (getUseTx()) {
            channel.txSelect();
        }
        return ret;
    }

    /**
     * Method will check the setting and get the contents for the message.
     *
     * @return the contents for the message
     */
    private String getMessageContent() {
        if (getMessageSource().equals(AMQPPublisherGui.USE_FILE_RSC)) {
            return getFileContent(getInputFile());
        } else if (getMessageSource().equals(AMQPPublisherGui.USE_RANDOM_RSC)) {
            // Maybe we should consider creating a global cache for the
            // random files to make JMeter more efficient.
            String fname = FSERVER.getRandomFile(getRandomPath(), new String[] { ".txt" })
                    .getAbsolutePath();
            return getFileContent(fname);
        } else {
            return getMessage();
        }
    }

    /**
     * The implementation uses TextFile to load the contents of the file and
     * returns a string.
     *
     * @param path
     * @return the contents of the file
     */
    protected String getFileContent(String path) {
        String fileContent = fileCache.get(path);
        if (fileContent == null) {
            TextFile tf = new TextFile(path);
            fileContent = tf.getText();
        }
        return fileContent;
    }

    /**
     * set the source of the message
     *
     * @param messageSource
     */
    public void setMessageSource(String messageSource) {
        setProperty(MESSAGE_SOURCE, messageSource);
    }

    /**
     * return the source of the message
     * Converts from old JMX files which used the local language string
     */
    public String getMessageSource() {
        return getPropertyAsString(MESSAGE_SOURCE, DEFAULT_MESSAGE_SOURCE);
    }


}