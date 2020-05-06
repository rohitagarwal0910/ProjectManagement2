package ProjectManagement;

public class UserReport implements UserReport_ {
    User user;

    UserReport(User user){
        this.user = user;
    }

    @Override
    public String user() {
        return user.name;
    }

    @Override
    public int consumed() {
        return user.consumed;
    }

    
}