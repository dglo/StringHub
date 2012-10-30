package icecube.daq.util;

import icecube.daq.juggler.alert.AlertException;
import icecube.daq.juggler.alert.Alerter;
import icecube.daq.payload.impl.UTCTime;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

class MockAlerter
    implements Alerter
{
    private Alerter.Priority expPrio;
    private String expCond;
    private Map<String, Object> expVars;

    public MockAlerter()
    {
    }

    private static void addErrors(StringBuilder buf, String name,
                                  List<String> vars)
    {
        Collections.sort(vars);

        String front;
        if (buf.length() == 0) {
            front = "Found ";
        } else {
            front = "; ";
        }
        buf.append(front).append(name).append(" variable");
        if (vars.size() > 1) {
            buf.append("s");
        }

        boolean first = true;
        for (String key : vars) {
            if (first) {
                buf.append(": ").append(key);
                first = false;
            } else {
                buf.append(", ").append(key);
            }
        }
    }

    public void close()
    {
        // do nothing
    }

    public String getService()
    {
        return DEFAULT_SERVICE;
    }

    public boolean isActive()
    {
        return true;
    }

    public void send(Alerter.Priority priority, String condition,
                     Map<String, Object> vars)
        throws AlertException
    {
        send(priority, condition, null, vars);
    }

    public void send(Alerter.Priority priority, String condition,
                     String notify, Map<String, Object> vars)
        throws AlertException
    {
        send(null, priority, condition, notify, vars);
    }

    public void send(Calendar dateTime, Alerter.Priority priority,
                     String condition, String notify, Map<String, Object> vars)
        throws AlertException
    {
        if (priority != expPrio) {
            throw new Error("Expected alert priority " + expPrio + ", got " +
                            priority);
        }

        if (!condition.equals(expCond)) {
            throw new Error("Expected alert condition \"" + expCond +
                            "\", got \"" + condition);
        }

        if ((vars == null || vars.size() == 0) &&
            (expVars != null && expVars.size() > 0))
        {
            throw new Error("Expected " + expVars.size() +
                            " variables, not 0");
        } else if ((vars != null && vars.size() > 0) &&
            (expVars == null || expVars.size() == 0))
        {
            throw new Error("Didn't expect any variables, but found " +
                            vars.size());
        } else if (vars != null && vars.size() > 0 && expVars != null &&
                   expVars.size() > 0)
        {
            List<String> unexpected = new ArrayList<String>();
            List<String> extra = new ArrayList<String>(expVars.keySet());
            List<String> badVal = new ArrayList<String>();

            for (String key : vars.keySet()) {
                if (!expVars.containsKey(key)) {
                    unexpected.add(key);
                } else {
                    extra.remove(key);

                    Object expVal = expVars.get(key);
                    Object actVal = vars.get(key);

                    if (!expVal.equals(actVal)) {
                        badVal.add(key);
                    }
                }
            }

            StringBuilder buf = new StringBuilder();
            if (unexpected.size() > 0) {
                addErrors(buf, "unexpected", unexpected);
            }

            if (extra.size() > 0) {
                addErrors(buf, "extra", extra);
            }

            if (badVal.size() > 0) {
                addErrors(buf, "bad", badVal);
            }

            if (buf.length() > 0) {
                throw new Error(buf.toString());
            }
        }
    }

    void setExpected(Alerter.Priority priority, String condition,
                     Map<String, Object> vars)
    {
        expPrio = priority;
        expCond = condition;
        expVars = vars;
    }

    public void setAddress(String host, int port)
        throws AlertException
    {
        throw new Error("Unimplemented");
    }
}

public class StringHubAlertTest
{
    @BeforeClass
    public static void setupLogging()
    {
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.INFO);
    }

    @Test
    public void testAlert()
        throws Exception
    {
        final String condition = "Test DOM alert";
        final int card = 1;
        final int pair = 23;
        final char dom = 'A';
        final String mbid = "123456789ABC";
        final String name = "TestDOM";
        final int major = 12;
        final int minor = 34;

        HashMap<String, Object> vars = new HashMap<String, Object>();
        vars.put("card", new Integer(card));
        vars.put("pair", new Integer(pair));
        vars.put("dom", dom);
        vars.put("mbid", mbid);
        vars.put("name", name);
        vars.put("major", major);
        vars.put("minor", minor);

        MockAlerter alerter = new MockAlerter();
        alerter.setExpected(Alerter.Priority.SCP, condition, vars);

        StringHubAlert.sendDOMAlert(alerter, condition, card, pair, dom,
                                    mbid, name, major, minor);
    }

    @Test
    public void testAlertPlusTime()
        throws Exception
    {
        final String condition = "Test DOM alert";
        final int card = 1;
        final int pair = 23;
        final char dom = 'A';
        final String mbid = "123456789ABC";
        final String name = "TestDOM";
        final int major = 12;
        final int minor = 34;
        final long utcTime = 123456789L;

        HashMap<String, Object> vars = new HashMap<String, Object>();
        vars.put("card", new Integer(card));
        vars.put("pair", new Integer(pair));
        vars.put("dom", dom);
        vars.put("mbid", mbid);
        vars.put("name", name);
        vars.put("major", major);
        vars.put("minor", minor);
        vars.put("exact-time", UTCTime.toDateString(utcTime));

        MockAlerter alerter = new MockAlerter();
        alerter.setExpected(Alerter.Priority.SCP, condition, vars);

        StringHubAlert.sendDOMAlert(alerter, condition, card, pair, dom,
                                    mbid, name, major, minor, utcTime);
    }
}
