package io.iron.ironmq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import io.iron.ironmq.*;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class IronMQTest {
    private String projectId;
    private String token;

    @Before public void setup() {
        projectId = System.getenv("IRON_PROJECT_ID");
        token = System.getenv("IRON_TOKEN");

        Assume.assumeTrue(projectId != null && token != null);
    }

    @Test public void testClient() throws IOException {
        Client c = new Client(projectId, token, Cloud.ironAWSUSEast);
        Queue q = c.queue("test-queue");

        // clear out the queue
        try {
            q.clear();
        } catch (IOException e) {
            // might be that it doesnt exist?
        }


        List<String> bodies = new ArrayList<String>();
        List<String> ids = new ArrayList<String>();
        for (int i = 0; i < 10; i++) {
            bodies.add("Hello, I am message #" + i);
            ids.add(q.push(bodies.get(i)));
        }

        Message message = q.getOne();
        checkMessage(bodies.get(0), ids.get(0), message);
        q.deleteMessage(message);


        List<Message> messages = q.get(3);
        assertEquals(3, messages.size());

        checkMessage(bodies.get(1), ids.get(1), messages.get(0));
        checkMessage(bodies.get(2), ids.get(2), messages.get(1));
        checkMessage(bodies.get(3), ids.get(3), messages.get(2));
        q.deleteMessage(messages.get(0));
        q.deleteMessage(messages.get(1));
        q.deleteMessage(messages.get(2));

        messages = q.get();
        assertEquals(6, messages.size());

        // just check the first one
        checkMessage(bodies.get(4), ids.get(4), messages.get(0));

        for (Message msg : messages) {
            q.deleteMessage(msg);
        }

    }

    private void checkMessage(String expectedBody, String expectedId, Message message) {
        Assert.assertEquals(expectedBody, message.getBody());
        Assert.assertEquals(expectedId, message.getId());
    }

    @Test(expected=HTTPException.class) public void testErrorResponse() throws IOException {
        // intentionally invalid project/token combination
        Client c = new Client("4444444444444", "aaaaaa", Cloud.ironAWSUSEast);
        Queue q = c.queue("test-queue");

        q.push("test");
    }
}
