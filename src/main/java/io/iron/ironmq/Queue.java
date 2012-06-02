package io.iron.ironmq;

import java.io.IOException;
import java.io.Reader;
import java.util.Collections;
import java.util.List;

import com.google.gson.Gson;

/**
 * The Queue class represents a specific IronMQ queue bound to a client.
 */
public class Queue {
    public static final int DEFAULT_MAX_PER_GET = 100;

    final private Client client;
    final private String name;

    private int maxMessagesPerGet = DEFAULT_MAX_PER_GET;

    Queue(Client client, String name) {
        this.client = client;
        this.name = name;
    }

    /**
    * Retrieves a single Message from the queue. If there are no items on the queue, null
    * is returned.
    *
    * @throws HTTPException If the IronMQ service returns a status other than 200 OK.
    * @throws IOException If there is an error accessing the IronMQ server.
    */
    public Message getOne() throws IOException {
        List<Message> messages = get(1);

        if (messages.isEmpty()) {
            return null;
        }

        return messages.get(0);

    }

    /**
     * Retrieves up to maxMessagesPerGet messages from the queue.  If there
     * are no messages on the queue, an empty list is returned.
     *
     * @return
     * @throws IOException
     */
    public List<Message> get() throws IOException {
        return get(maxMessagesPerGet);
    }

    /**
     * Retrieves up to a given number of Messages from the queue.  If there are
     * no messages on the queue, an empty list is returned.
     *
     * @return
     * @throws IOException
     */
    public List<Message> get(int limit) throws IOException {

        Reader reader = client.get("queues/" + name + "/messages?n=" + limit);
        return parseMessages(reader);
    }

    List<Message> parseMessages(Reader reader) {
        Gson gson = new Gson();
        Messages msgs = gson.fromJson(reader, Messages.class);
        if (msgs == null || msgs.getMessages() == null) {
            return Collections.emptyList();
        }
        return msgs.getMessages();
    }

    /**
    * Deletes a Message from the queue.
    *
    * @param id The ID of the message to delete.
    *
    * @throws HTTPException If the IronMQ service returns a status other than 200 OK.
    * @throws IOException If there is an error accessing the IronMQ server.
    */
    public void deleteMessage(String id) throws IOException {
        client.delete("queues/" + name + "/messages/" + id);
    }

    /**
    * Deletes a Message from the queue.
    *
    * @param msg The message to delete.
    *
    * @throws HTTPException If the IronMQ service returns a status other than 200 OK.
    * @throws IOException If there is an error accessing the IronMQ server.
    */
    public void deleteMessage(Message msg) throws IOException {
        deleteMessage(msg.getId());
    }

    /**
    * Pushes a message onto the queue.
    *
    * @param msg The body of the message to push.
    * @return The new message's ID
    *
    * @throws HTTPException If the IronMQ service returns a status other than 200 OK.
    * @throws IOException If there is an error accessing the IronMQ server.
    */
    public String push(String msg) throws IOException {
        return push(msg, 0);
    }

    /**
    * Pushes a message onto the queue.
    *
    * @param msg The body of the message to push.
    * @param timeout The message's timeout in seconds.
    * @return The new message's ID
    *
    * @throws HTTPException If the IronMQ service returns a status other than 200 OK.
    * @throws IOException If there is an error accessing the IronMQ server.
    */
    public String push(String msg, long timeout) throws IOException {
        return push(msg, 0, 0);
    }

    /**
    * Pushes a message onto the queue.
    *
    * @param msg The body of the message to push.
    * @param timeout The message's timeout in seconds.
    * @param delay The message's delay in seconds.
    * @return The new message's ID
    *
    * @throws HTTPException If the IronMQ service returns a status other than 200 OK.
    * @throws IOException If there is an error accessing the IronMQ server.
    */
    public String push(String msg, long timeout, long delay) throws IOException {
        return push(msg, 0, 0, 0);
    }

    /**
    * Pushes a message onto the queue.
    *
    * @param msg The body of the message to push.
    * @param timeout The message's timeout in seconds.
    * @param delay The message's delay in seconds.
    * @param expiresIn The message's expiration offset in seconds.
    * @return The new message's ID
    *
    * @throws HTTPException If the IronMQ service returns a status other than 200 OK.
    * @throws IOException If there is an error accessing the IronMQ server.
    */
    public String push(String msg, long timeout, long delay, long expiresIn) throws IOException {
        Message message = new Message();
        message.setBody(msg);
        message.setTimeout(timeout);
        message.setDelay(delay);
        message.setExpiresIn(expiresIn);

        List<String> ids = push(Collections.singletonList(message));
        return ids.get(0);
    }

    /**
     * Pushes a batch of messages.  Care should be taken to ensure these will not exceed the
     * maximum post size, there is no checking currently being done.
     *
     * @param messages
     * @return
     * @throws IOException
     */
    public List<String> push(List<Message> messages) throws IOException {
        Messages msgs = new Messages(messages);
        Gson gson = new Gson();
        String body = gson.toJson(msgs);

        Reader reader = client.post("queues/" + name + "/messages", body);
        Ids ids = gson.fromJson(reader, Ids.class);
        return ids.getIds();
    }

    /**
     * Clears all messages from the queue.
     *
     * @throws IOException
     */
    public void clear() throws IOException {
        client.post("queues/" + name + "/clear", null);
    }

    /**
     * The maximum number of messages that will be fetched per get() call.
     *
     * @return
     */
    public int getMaxMessagesPerGet() {
        return maxMessagesPerGet;
    }

    /**
     * Set the maximum number of messages that will be fetched per get() call.
     *
     * @param maxMessagesPerGet
     */
    public void setMaxMessagesPerGet(int maxMessagesPerGet) {
        this.maxMessagesPerGet = maxMessagesPerGet;
    }
}
