package com.zeroclue.jmeter.protocol.amqp.gui;

import java.awt.Dimension;

import javax.swing.*;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.apache.jmeter.gui.util.FilePanel;
import org.apache.jmeter.gui.util.JLabeledRadioI18N;
import org.apache.jmeter.testelement.TestElement;
import org.apache.jorphan.gui.JLabeledTextArea;
import org.apache.jorphan.gui.JLabeledTextField;

import com.zeroclue.jmeter.protocol.amqp.AMQPPublisher;

/**
 * AMQP Sampler
 *
 * This class is responsible for ensuring that the Sampler data is kept in step
 * with the GUI.
 *
 * The GUI class is not invoked in non-GUI mode, so it should not perform any
 * additional setup that a test would need at run-time
 *
 */
public class AMQPPublisherGui extends AMQPSamplerGui implements ChangeListener {

    private static final long serialVersionUID = 1L;

    private static final String ALL_FILES = "*.*"; //$NON-NLS-1$
    /** Take source from the named file */
    public static final String USE_FILE_RSC   = "jms_use_file"; //$NON-NLS-1$
    /** Take source from a random file */
    public static final String USE_RANDOM_RSC = "jms_use_random_file"; //$NON-NLS-1$
    /** Take source from the text area */
    public static final String USE_TEXT_RSC   = "jms_use_text"; //$NON-NLS-1$

    private JPanel mainPanel;

    private JLabeledTextField messageRoutingKey = new JLabeledTextField("Routing Key");
    private JLabeledTextField messageType = new JLabeledTextField("Message Type");
    private JLabeledTextField replyToQueue = new JLabeledTextField("Reply-To Queue");
    private JLabeledTextField correlationId = new JLabeledTextField("Correlation Id");

    private JCheckBox persistent = new JCheckBox("Persistent?", AMQPPublisher.DEFAULT_PERSISTENT);
    private JCheckBox useTx = new JCheckBox("Use Transactions?", AMQPPublisher.DEFAULT_USE_TX);

    private final FilePanel messageFile = new FilePanel("Message File", ALL_FILES);
    private final FilePanel randomFile = new FilePanel("Random File", ALL_FILES);
    private final JLabeledTextArea textMessage = new JLabeledTextArea("Text Message");

 // Button group resources
    private static final String[] MESSAGE_SOURCE_ITEMS = { USE_FILE_RSC, USE_RANDOM_RSC, USE_TEXT_RSC };
    private final JLabeledRadioI18N messageSource = new JLabeledRadioI18N("jms_config", MESSAGE_SOURCE_ITEMS, USE_TEXT_RSC); //$NON-NLS-1$

    public AMQPPublisherGui(){
        init();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getLabelResource() {
        return this.getClass().getSimpleName();
    }

    @Override
    public String getStaticLabel() {
        return "AMQP Publisher";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(TestElement element) {
        super.configure(element);
        if (!(element instanceof AMQPPublisher)) return;
        AMQPPublisher sampler = (AMQPPublisher) element;

        persistent.setSelected(sampler.getPersistent());
        useTx.setSelected(sampler.getUseTx());

        messageRoutingKey.setText(sampler.getMessageRoutingKey());
        messageType.setText(sampler.getMessageType());
        replyToQueue.setText(sampler.getReplyToQueue());
        correlationId.setText(sampler.getCorrelationId());

        updateMessageSource(sampler.getMessageSource());
        messageFile.setFilename(sampler.getInputFile());
        randomFile.setFilename(sampler.getRandomPath());
        textMessage.setText(sampler.getMessage());

        updateMessageSource(sampler.getMessageSource());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TestElement createTestElement() {
        AMQPPublisher sampler = new AMQPPublisher();
        modifyTestElement(sampler);
        return sampler;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void modifyTestElement(TestElement te) {
        AMQPPublisher sampler = (AMQPPublisher) te;
        sampler.clear();
        configureTestElement(sampler);

        super.modifyTestElement(sampler);

        sampler.setPersistent(persistent.isSelected());
        sampler.setUseTx(useTx.isSelected());

        sampler.setMessageRoutingKey(messageRoutingKey.getText());
        sampler.setMessageType(messageType.getText());
        sampler.setReplyToQueue(replyToQueue.getText());
        sampler.setCorrelationId(correlationId.getText());

        sampler.setMessage(textMessage.getText());
        sampler.setInputFile(messageFile.getFilename());
        sampler.setRandomPath(randomFile.getFilename());

        sampler.setMessageSource(messageSource.getText());
    }

    @Override
    protected void setMainPanel(JPanel panel){
        mainPanel = panel;
    }

    /*
     * Helper method to set up the GUI screen
     */
    @Override
    protected final void init() {
        super.init();
        persistent.setPreferredSize(new Dimension(100, 25));
        useTx.setPreferredSize(new Dimension(100, 25));
        messageRoutingKey.setPreferredSize(new Dimension(100, 25));
        messageType.setPreferredSize(new Dimension(100, 25));
        replyToQueue.setPreferredSize(new Dimension(100, 25));
        correlationId.setPreferredSize(new Dimension(100, 25));

        mainPanel.add(persistent);
        mainPanel.add(useTx);
        mainPanel.add(messageRoutingKey);
        mainPanel.add(messageType);
        mainPanel.add(replyToQueue);
        mainPanel.add(correlationId);

        messageSource.setLayout(new BoxLayout(messageSource, BoxLayout.X_AXIS));
        messageSource.setText(AMQPPublisher.DEFAULT_MESSAGE_SOURCE);
        mainPanel.add(messageSource);
        messageSource.addChangeListener(this);

        mainPanel.add(messageFile);
        mainPanel.add(randomFile);
        mainPanel.add(textMessage);
        Dimension pref = new Dimension(400, 150);
        textMessage.setPreferredSize(pref);

        updateMessageSource(messageSource.getText());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clearGui() {
        super.clearGui();
        persistent.setSelected(AMQPPublisher.DEFAULT_PERSISTENT);
        useTx.setSelected(AMQPPublisher.DEFAULT_USE_TX);
        messageRoutingKey.setText("");
        messageType.setText("");
        replyToQueue.setText("");
        correlationId.setText("");

        textMessage.setText("");
        messageFile.setFilename("");
        randomFile.setFilename("");
        textMessage.setText("");

        updateMessageSource(AMQPPublisher.DEFAULT_MESSAGE_SOURCE);
    }

    /**
     * Update config contains the actual logic for enabling or disabling text
     * message, file or random path.
     *
     * @param command
     */
    private void updateMessageSource(String command) {
        messageSource.setText(command);
        if (command.equals(USE_TEXT_RSC)) {
            textMessage.setVisible(true);
            messageFile.setVisible(false);
            randomFile.setVisible(false);
        } else if (command.equals(USE_RANDOM_RSC)) {
            textMessage.setVisible(false);
            messageFile.setVisible(false);
            randomFile.setVisible(true);
        } else if (command.equals(USE_FILE_RSC)) {
            textMessage.setVisible(false);
            messageFile.setVisible(true);
            randomFile.setVisible(false);
        }
    }


    @Override
    public void stateChanged(ChangeEvent event) {
        if (event.getSource() == messageSource) {
            updateMessageSource(messageSource.getText());
        }
    }


}