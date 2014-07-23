package icecube.daq.util;

import icecube.daq.juggler.alert.AlertException;
import icecube.daq.juggler.alert.Alerter;
import icecube.daq.payload.impl.UTCTime;

import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class StringHubAlert
{
    /** Logging object */
    private static final Log LOG = LogFactory.getLog(StringHubAlert.class);

    /**
     * Send a DOM alert.
     */
    public static final void sendDOMAlert(Alerter alerter, String condition,
                                          int card, int pair, char dom,
                                          String mbid, String name, int string,
                                          int position)
    {
        sendDOMAlert(alerter, condition, card, pair, dom, mbid, name, string,
                     position, Integer.MIN_VALUE);
    }

    /**
     * Send a DOM alert.
     */
    public static final void sendDOMAlert(Alerter alerter, String condition,
                                          int card, int pair, char dom,
                                          String mbid, String name, int string,
                                          int position, int runNumber)
    {
        sendDOMAlert(alerter, condition, card, pair, dom, mbid, name, string,
                     position, runNumber, Long.MIN_VALUE);
    }

    /**
     * Send a DOM alert.
     */
    public static final void sendDOMAlert(Alerter alerter, String condition,
                                          int card, int pair, char dom,
                                          String mbid, String name, int string,
                                          int position, int runNumber,
                                          long utcTime)
    {
        if (alerter == null || !alerter.isActive()) {
            return;
        }

        HashMap<String, Object> vars = new HashMap<String, Object>();
        if (dom != (char) 0) {
            vars.put("card", card);
            vars.put("pair", pair);
            vars.put("dom", dom);
        }
        if (mbid != null) {
            vars.put("mbid", mbid);
        }
        if (name != null) {
            vars.put("name", name);
        }
        vars.put("string", string);
        vars.put("position", position);
        if (runNumber > 0) {
            vars.put("runNumber", runNumber);
        }
        if (utcTime >= 0L) {
            vars.put("exact-time", UTCTime.toDateString(utcTime));
        }

        try {
            alerter.sendAlert(Alerter.Priority.SCP, condition, vars);
        } catch (AlertException ae) {
            LOG.error("Cannot send " + condition + " alert", ae);
        }
    }
}
