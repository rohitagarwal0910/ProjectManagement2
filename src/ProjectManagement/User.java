package ProjectManagement;

import java.util.ArrayList;

public class User implements Comparable<User> {
    String name;
    int consumed;
    int lct;
    int sno;

    ArrayList<Job> user_jobs = new ArrayList<Job>();

    User(String name, int sno) {
        this.name = name;
        this.consumed = 0;
        this.sno = sno;
    }

    @Override
    public int compareTo(User user) {
        return (this.consumed - user.consumed != 0) ? this.consumed - user.consumed
                : (this.lct - user.lct == 0) ? this.lct - user.lct : user.sno - this.sno;
    }
}
