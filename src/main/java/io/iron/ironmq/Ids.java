package io.iron.ironmq;

import java.util.List;

class Ids {
    private List<String> ids;

    String getId(int i) {
        return ids.get(i);
    }

    public List<String> getIds() {
        return ids;
    }
}
