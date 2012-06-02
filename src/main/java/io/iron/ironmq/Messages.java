package io.iron.ironmq;

import java.util.Collections;
import java.util.List;

class Messages {
    private List<Message> messages;

    Messages(Message message) {
        messages = Collections.singletonList(message);
    }

    Messages(List<Message> msgs) {
        messages = msgs;
    }

    List<Message> getMessages() {
        return messages;
    }
}
