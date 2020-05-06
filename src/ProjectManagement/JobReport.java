package ProjectManagement;

public class JobReport implements JobReport_ {
    public Job job;

    JobReport(Job job) {
        this.job = job;
    }

    @Override
    public String user() {
        return job.user.name;
    }

    @Override
    public String project_name() {
        return job.project.name;
    }

    @Override
    public int budget() {
        return job.runtime;
    }

    @Override
    public int arrival_time() {
        return job.arrivaltime;
    }

    @Override
    public int completion_time() {
        return job.completedTime;
    }
}