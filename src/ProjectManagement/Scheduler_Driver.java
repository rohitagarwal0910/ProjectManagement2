package ProjectManagement;

import Trie.*;
import RedBlack.*;
import PriorityQueue.*;
import java.util.List;
import java.util.Queue;
import java.util.LinkedList;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;

public class Scheduler_Driver extends Thread implements SchedulerInterface {
    public Trie<Project> projects = new Trie<Project>(); // all projects
    public MaxHeap<Job> jobs = new MaxHeap<Job>(); // ready jobs
    public Trie<User> users = new Trie<User>(); // all users by user
    public Trie<Job> jobTrie = new Trie<Job>(); // all jobs by job
    public ArrayList<Job> allJobs = new ArrayList<Job>();
    public RBTree<String, Job> jobsLeft = new RBTree<String, Job>(); // unsuffienct jobs by project
    public ArrayList<Job> completedJobs = new ArrayList<Job>(); // completed jobs
    public ArrayList<User> allUsers = new ArrayList<User>(); // storing all users
    int time = 0;
    int sno = 0;
    int psno = 0;
    int usno = 0;

    public static void main(String[] args) throws IOException {
        //

        Scheduler_Driver scheduler_driver = new Scheduler_Driver();
        File file;
        if (args.length == 0) {
            URL url = Scheduler_Driver.class.getResource("INP");
            file = new File(url.getPath());
        } else {
            file = new File(args[0]);
        }

        scheduler_driver.execute(file);
    }

    public void execute(File commandFile) throws IOException {

        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(commandFile));

            String st;
            while ((st = br.readLine()) != null) {
                String[] cmd = st.split(" ");
                if (cmd.length == 0) {
                    System.err.println("Error parsing: " + st);
                    return;
                }
                String project_name, user_name;
                Integer start_time, end_time;

                long qstart_time, qend_time;

                switch (cmd[0]) {
                case "PROJECT":
                    handle_project(cmd);
                    break;
                case "JOB":
                    handle_job(cmd);
                    break;
                case "USER":
                    handle_user(cmd[1]);
                    break;
                case "QUERY":
                    handle_query(cmd[1]);
                    break;
                case "": // HANDLE EMPTY LINE
                    handle_empty_line();
                    break;
                case "ADD":
                    handle_add(cmd);
                    break;
                // --------- New Queries
                case "NEW_PROJECT":
                case "NEW_USER":
                case "NEW_PROJECTUSER":
                case "NEW_PRIORITY":
                    timed_report(cmd);
                    break;
                case "NEW_TOP":
                    qstart_time = System.nanoTime();
                    timed_top_consumer(Integer.parseInt(cmd[1]));
                    qend_time = System.nanoTime();
                    // System.out.println("Time elapsed (ns): " + (qend_time - qstart_time));
                    break;
                case "NEW_FLUSH":
                    qstart_time = System.nanoTime();
                    timed_flush(Integer.parseInt(cmd[1]));
                    qend_time = System.nanoTime();
                    // System.out.println("Time elapsed (ns): " + (qend_time - qstart_time));
                    break;
                default:
                    System.err.println("Unknown command: " + cmd[0]);
                }

            }

            run_to_completion();
            print_stats();

        } catch (FileNotFoundException e) {
            System.err.println("Input file Not found. " + commandFile.getAbsolutePath());
        } catch (NullPointerException ne) {
            ne.printStackTrace();

        }
    }

    @Override
    public ArrayList<JobReport_> timed_report(String[] cmd) {
        long qstart_time, qend_time;
        ArrayList<JobReport_> res = null;
        switch (cmd[0]) {
        case "NEW_PROJECT":
            qstart_time = System.nanoTime();
            res = handle_new_project(cmd);
            qend_time = System.nanoTime();
            // System.out.println("Time elapsed (ns): " + (qend_time - qstart_time));
            break;
        case "NEW_USER":
            qstart_time = System.nanoTime();
            res = handle_new_user(cmd);
            qend_time = System.nanoTime();
            // System.out.println("Time elapsed (ns): " + (qend_time - qstart_time));

            break;
        case "NEW_PROJECTUSER":
            qstart_time = System.nanoTime();
            res = handle_new_projectuser(cmd);
            qend_time = System.nanoTime();
            // System.out.println("Time elapsed (ns): " + (qend_time - qstart_time));
            break;
        case "NEW_PRIORITY":
            qstart_time = System.nanoTime();
            res = handle_new_priority(cmd[1]);
            qend_time = System.nanoTime();
            // System.out.println("Time elapsed (ns): " + (qend_time - qstart_time));
            break;
        }
        return res;
    }

    @Override
    public ArrayList<UserReport_> timed_top_consumer(int top) {
        allUsers.sort((a, b) -> {
            if (a.consumed > b.consumed)
                return -1;
            else if (a.consumed < b.consumed)
                return 1;
            else if (a.lct > b.lct)
                return -1;
            else if (a.lct < b.lct)
                return 1;
            else
                return a.sno - b.sno;
        });
        ArrayList<UserReport_> tr = new ArrayList<UserReport_>();
        for (int i = 0; i < top && i < allUsers.size(); i++) {
            tr.add(new UserReport(allUsers.get(i)));
        }
        return tr;
    }

    @Override
    public void timed_flush(int waittime) {
        int testtime = time - waittime;
        MaxHeap<Job> te = new MaxHeap<Job>();
        for (int i = 0; i < jobs.list.size(); i++) {
            Job tj = jobs.list.get(i).value;
            if (tj.arrivaltime <= testtime && tj.runtime <= tj.project.budget) {
                jobs.swap(i--, jobs.list.size() - 1);
                te.list.add(jobs.list.remove(jobs.list.size() - 1));
            }
        }
        te.makeHeap();
        while (te.list.size() > 0) {
            Job job = te.extractMax();
            if (job.project.budget >= job.runtime) {
                job.completed = true; // marking job as completed
                time += job.runtime; // updating global time
                job.completedTime = time; // setting job completion time
                job.project.budget -= job.runtime; // updating project budget
                job.user.lct = time; // setting last completion time of user
                job.user.consumed += job.runtime; // updating user consumption
                completedJobs.add(job); // adding job to list of completed jobs
            } else {
                jobs.list.add(new pqNode<Job>(0, job)); // adding job back to heap
            }
        }
        jobs.makeHeap();

        // int testtime = time - waittime;
        // MaxHeap<Job> te = new MaxHeap<Job>();
        // MaxHeap<Job> ts = new MaxHeap<Job>();
        // while (jobs.list.size() > 0) {
        // Job tj = jobs.extractMax();
        // if (tj.arrivaltime <= testtime && tj.runtime <= tj.project.budget)
        // te.insert(tj);
        // else
        // ts.insert(tj);
        // }
        // while (te.list.size() > 0) {
        // Job job = te.extractMax();
        // if (job.project.budget >= job.runtime) {
        // job.completed = true; // marking job as completed
        // time += job.runtime; // updating global time
        // job.completedTime = time; // setting job completion time
        // job.project.budget -= job.runtime; // updating project budget
        // job.user.lct = time; // setting last completion time of user
        // job.user.consumed += job.runtime; // updating user consumption
        // completedJobs.add(job); // adding job to list of completed jobs
        // } else {
        // ts.insert(job); // adding job to ts
        // }
        // }
        // jobs = ts;
    }

    private ArrayList<JobReport_> handle_new_priority(String s) {
        allJobs.sort(
                // null
                (a, b) -> {
                    // // if (a.project.priority > b.project.priority)
                    // // return -1;
                    // // else if (a.project.priority < b.project.priority)
                    // // return 1;
                    // // else if (a.project.sno < b.project.sno)
                    // // return -1;
                    // // else if (a.project.sno > b.project.sno)
                    // // return 1;
                    // // else
                    // // return a.sno - b.sno;
                    return (a.project.priority - b.project.priority != 0) ? b.project.priority - a.project.priority
                            : (a.project.sno - b.project.sno != 0) ? a.project.sno - b.project.sno : a.sno - b.sno;
                });
        ArrayList<JobReport_> tr = new ArrayList<JobReport_>();
        for (int i = 0; i < allJobs.size(); i++) {
            Job t = allJobs.get(i);
            if (t.project.priority < Integer.parseInt(s))
                break;
            if (!t.completed)
                tr.add(new JobReport(t));
        }
        return tr;
    }

    int find(ArrayList<Job> pf, int tf, int s, int e) {
        if (pf.size() == 0)
            return 0;
        if (s == e) {
            if (tf <= pf.get(s).arrivaltime)
                return 0;
            if (tf > pf.get(e).arrivaltime)
                return pf.size();
        }
        if (tf > pf.get((s + e) / 2).arrivaltime) {
            if (tf <= pf.get(((s + e) / 2) + 1).arrivaltime)
                return (((s + e) / 2) + 1);
            if (tf > pf.get(((s + e) / 2) + 1).arrivaltime)
                return find(pf, tf, (s + e) / 2 + 1, e);
        }
        if (tf <= pf.get((s + e) / 2).arrivaltime) {
            return find(pf, tf, s, (s + e) / 2);
        }
        return 0;
    }

    private ArrayList<JobReport_> handle_new_projectuser(String[] cmd) {
        TrieNode project = projects.search(cmd[1]);
        if (project == null)
            return new ArrayList<JobReport_>();
        Project tu = (Project) project.value;
        int t1 = Integer.parseInt(cmd[3]);
        int t2 = Integer.parseInt(cmd[4]);
        int start = find(tu.project_jobs, t1, 0, tu.project_jobs.size() - 1);
        ArrayList<JobReport_> tr = new ArrayList<JobReport_>();
        for (int i = start; i < tu.project_jobs.size(); i++) {
            Job tj = tu.project_jobs.get(i);
            if (tj.arrivaltime >= t1 && tj.arrivaltime <= t2 && tj.user.name.equals(cmd[2])) {
                tr.add(new JobReport(tj));
            }
            if (tj.arrivaltime > t2)
                break;
        }

        // TrieNode user = users.search(cmd[2]);
        // if (user == null)
        // return new ArrayList<JobReport_>();
        // User tu = (User) user.value;
        // int t1 = Integer.parseInt(cmd[3]);
        // int t2 = Integer.parseInt(cmd[4]);
        // int start = find(tu.user_jobs, t1, 0, tu.user_jobs.size() - 1);
        // ArrayList<JobReport_> tr = new ArrayList<JobReport_>();
        // for (int i = start; i < tu.user_jobs.size(); i++) {
        // Job tj = tu.user_jobs.get(i);
        // if (tj.arrivaltime >= t1 && tj.arrivaltime <= t2 &&
        // tj.project.name.equals(cmd[1])) {
        // tr.add(new JobReport(tj));
        // }
        // if (tj.arrivaltime > t2)
        // break;
        // }

        tr.sort((b, a) -> {
            if (a.completion_time() == 0 && b.completion_time() == 0)
                return 0;
            else if (a.completion_time() == 0 && b.completion_time() != 0)
                return -1;
            else if (a.completion_time() != 0 && b.completion_time() == 0)
                return 1;
            else
                return b.completion_time() - a.completion_time();
        });
        return tr;
    }

    private ArrayList<JobReport_> handle_new_user(String[] cmd) {
        TrieNode user = users.search(cmd[1]);
        if (user == null)
            return new ArrayList<JobReport_>();
        User tu = (User) user.value;
        int t1 = Integer.parseInt(cmd[2]);
        int t2 = Integer.parseInt(cmd[3]);
        int start = find(tu.user_jobs, t1, 0, tu.user_jobs.size() - 1);
        ArrayList<JobReport_> tr = new ArrayList<JobReport_>();
        for (int i = start; i < tu.user_jobs.size(); i++) {
            Job tj = tu.user_jobs.get(i);
            if (tj.arrivaltime >= t1 && tj.arrivaltime <= t2) {
                tr.add(new JobReport(tj));
            }
            if (tj.arrivaltime > t2)
                break;
        }
        return tr;
    }

    private ArrayList<JobReport_> handle_new_project(String[] cmd) {
        TrieNode project = projects.search(cmd[1]);
        if (project == null)
            return new ArrayList<JobReport_>();
        Project tp = (Project) project.value;
        int t1 = Integer.parseInt(cmd[2]);
        int t2 = Integer.parseInt(cmd[3]);
        int start = find(tp.project_jobs, t1, 0, tp.project_jobs.size() - 1);
        ArrayList<JobReport_> tr = new ArrayList<JobReport_>();
        for (int i = start; i < tp.project_jobs.size(); i++) {
            Job tj = tp.project_jobs.get(i);
            if (tj.arrivaltime >= t1 && tj.arrivaltime <= t2) {
                tr.add(new JobReport(tj));
            }
            if (tj.arrivaltime > t2)
                break;
        }
        return tr;
    }

    public void schedule() {
        System.out.println("Running code");
        System.out.println("Remaining jobs: " + jobs.list.size());
        execute_a_job();
    }

    public void run_to_completion() {
        while (jobs.list.size() > 0) {
            schedule();
            System.out.println("System execution completed");
        }
    }

    @Override
    public void timed_run_to_completion() {
        while (jobs.list.size() > 0) {
            timed_execute_a_job();
        }
    }

    // ArrayList<Job> searchUnfinished(int p) {
    // ArrayList<Job> unfinishedJobs = new ArrayList<Job>();
    // RedBlackNode cn = jobsLeft.root;
    // if (cn == null)
    // return unfinishedJobs;
    // Queue<RedBlackNode> queue = new LinkedList<RedBlackNode>();
    // queue.add(cn);
    // while (queue.size() > 0) {
    // RedBlackNode t = queue.remove();
    // List<Job> j = t.getValues();
    // for (int i = 0; i < j.size(); i++) {
    // Job tj = j.get(i);
    // if (tj.project.priority >= p)
    // unfinishedJobs.add(tj);
    // }
    // if (t.left.key != null) {
    // queue.add(t.left);
    // }
    // if (t.right.key != null) {
    // queue.add(t.right);
    // }
    // }
    // return unfinishedJobs;
    // }

    ArrayList<Job> getUnfinishedJobs() {
        ArrayList<Job> unfinishedJobs = new ArrayList<Job>();
        RedBlackNode cn = jobsLeft.root;
        if (cn == null)
            return unfinishedJobs;
        Queue<RedBlackNode> queue = new LinkedList<RedBlackNode>();
        queue.add(cn);
        while (queue.size() > 0) {
            RedBlackNode t = queue.remove();
            List<Job> j = t.getValues();
            for (int i = 0; i < j.size(); i++) {
                unfinishedJobs.add(j.get(i));
            }
            if (t.left.key != null) {
                queue.add(t.left);
            }
            if (t.right.key != null) {
                queue.add(t.right);
            }
        }
        unfinishedJobs.sort((a, b) -> {
            return (a.project.priority - b.project.priority != 0) ? b.project.priority - a.project.priority
                    : (a.project.sno - b.project.sno != 0) ? a.project.sno - b.project.sno : a.sno - b.sno;
        });
        return unfinishedJobs;
    }

    public void print_stats() {
        ArrayList<Job> unfinishedJobs = getUnfinishedJobs();
        int no = unfinishedJobs.size();
        System.out.println("--------------STATS---------------");
        System.out.println("Total jobs done: " + completedJobs.size());
        for (int i = 0; i < completedJobs.size(); i++) {
            System.out.println(completedJobs.get(i).toString());
        }
        System.out.println("------------------------");
        System.out.println("Unfinished jobs: ");
        for (int i = 0; i < unfinishedJobs.size(); i++) {
            System.out.println(unfinishedJobs.get(i).toString());
        }
        System.out.println("Total unfinished jobs: " + no);
        System.out.println("--------------STATS DONE---------------");
    }

    public void handle_add(String[] cmd) {
        System.out.println("ADDING Budget");
        TrieNode project = projects.search(cmd[1]);
        if (project == null) {
            System.out.println("No such project exists. " + cmd[1]);
            return;
        }
        ((Project) project.getValue()).budget += Integer.parseInt(cmd[2]);
        RedBlackNode r = jobsLeft.search(cmd[1]);
        if (r.key == null)
            return;
        List<Job> toAdd = r.getValues();
        for (int i = 0; i < toAdd.size(); i++) {
            jobs.insert(toAdd.get(i));
        }
        r.values.clear();
    }

    public void handle_empty_line() {
        schedule();
        System.out.println("Execution cycle completed");
    }

    public void handle_query(String key) {
        System.out.println("Querying");
        TrieNode job = jobTrie.search(key);
        if (job == null) {
            System.out.println(key + ": NO SUCH JOB");
            return;
        } else {
            if (((Job) job.getValue()).completed) {
                System.out.println(key + ": COMPLETED");
            } else
                System.out.println(key + ": NOT FINISHED");
        }
    }

    public void handle_user(String name) {
        System.out.println("Creating user");
        User t = new User(name, usno++);
        users.insert(name, t);
        allUsers.add(t);
    }

    @Override
    public void timed_handle_user(String name) {
        User t = new User(name, usno++);
        users.insert(name, t);
        allUsers.add(t);
    }

    public void handle_job(String[] cmd) {
        System.out.println("Creating job");
        TrieNode project = projects.search(cmd[2]);
        if (project == null) {
            System.out.println("No such project exists. " + cmd[2]);
            return;
        }
        TrieNode user = users.search(cmd[3]);
        if (user == null) {
            System.out.println("No such user exists: " + cmd[3]);
            return;
        }
        User tuser = (User) user.value;
        Project tproject = (Project) project.value;
        Job job = new Job(cmd[1], tproject, tuser, Integer.parseInt(cmd[4]), sno++);
        job.arrivaltime = time; // setting job arrival time
        jobs.insert(job); // adding job to heap
        jobTrie.insert(job.name, job); // adding job to all job trie
        tuser.user_jobs.add(job); // adding job to its users job list
        tproject.project_jobs.add(job); // adding job to its project job list
        allJobs.add(job); // add to all jobs list
    }

    @Override
    public void timed_handle_job(String[] cmd) {
        TrieNode project = projects.search(cmd[2]);
        if (project == null) {
            return;
        }
        TrieNode user = users.search(cmd[3]);
        if (user == null) {
            return;
        }
        User tuser = (User) user.value;
        Project tproject = (Project) project.value;
        Job job = new Job(cmd[1], tproject, tuser, Integer.parseInt(cmd[4]), sno++);
        job.arrivaltime = time; // setting job arrival time
        jobs.insert(job); // adding job to heap
        jobTrie.insert(job.name, job); // adding job to all job trie
        tuser.user_jobs.add(job); // adding job to its users job list
        tproject.project_jobs.add(job); // adding job to its project job list
        allJobs.add(job); // add to all jobs list
    }

    public void handle_project(String[] cmd) {
        System.out.println("Creating project");
        projects.insert(cmd[1], new Project(cmd[1], Integer.parseInt(cmd[2]), Integer.parseInt(cmd[3]), psno++));

    }

    @Override
    public void timed_handle_project(String[] cmd) {
        projects.insert(cmd[1], new Project(cmd[1], Integer.parseInt(cmd[2]), Integer.parseInt(cmd[3]), psno++));
    }

    public void execute_a_job() {
        while (true) {
            Job job = jobs.extractMax();
            if (job == null) {
                break;
            }
            System.out.println("Executing: " + job.name + " from: " + job.project.name);
            if (job.project.budget >= job.runtime) {
                job.completed = true; // marking job as completed
                time += job.runtime; // updating global time
                job.completedTime = time; // setting job completion time
                job.project.budget -= job.runtime; // updating project budget
                job.user.lct = time; // setting last completion time of user
                job.user.consumed += job.runtime; // updating user consumption
                completedJobs.add(job); // adding job to list of completed jobs
                System.out.println("Project: " + job.project.name + " budget remaining: " + job.project.budget);
                break;
            } else {
                System.out.println("Un-sufficient budget.");
                jobsLeft.insert(job.project.name, job); // adding job to unsufficient rbtree
            }
        }
    }

    public void timed_execute_a_job() {
        while (true) {
            Job job = jobs.extractMax();
            if (job == null) {
                break;
            }
            if (job.project.budget >= job.runtime) {
                job.completed = true; // marking job as completed
                time += job.runtime; // updating global time
                job.completedTime = time; // setting job completion time
                job.project.budget -= job.runtime; // updating project budget
                job.user.lct = time; // setting last completion time of user
                job.user.consumed += job.runtime; // updating user consumption
                completedJobs.add(job); // adding job to list of completed jobs
                break;
            } else {
                jobsLeft.insert(job.project.name, job); // adding job to unsufficient rbtree
            }
        }
    }
}
