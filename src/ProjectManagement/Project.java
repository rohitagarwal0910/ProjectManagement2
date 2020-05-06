package ProjectManagement;

import java.util.ArrayList;

public class Project {
    public String name;
    public int priority;
    public int budget;
    public ArrayList<Job> project_jobs = new ArrayList<Job>();
    public int sno;

    Project(String name, int priority, int budget, int sno) {
        this.name = name;
        this.priority = priority;
        this.budget = budget;
        this.sno = sno;
    }
}
