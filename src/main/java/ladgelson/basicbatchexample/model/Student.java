package ladgelson.basicbatchexample.model;

public class Student {
    private int id;
    private String firstname;
    private String lastname;
    private String email;

    public static String[] fields() {
        return new String[] {"id", "firstname", "lastname", "email"};
    }

    public Student() {
    }

    public Student(int id, String firstname, String lastname, String email) {
        this.id = id;
        this.firstname = firstname;
        this.lastname = lastname;
        this.email = email;
    }

    public int getId() {
        return id;
    }

    public Student setId(int id) {
        this.id = id;
        return this;
    }

    public String getFirstname() {
        return firstname;
    }

    public Student setFirstname(String firstname) {
        this.firstname = firstname;
        return this;
    }

    public String getLastname() {
        return lastname;
    }

    public Student setLastname(String lastname) {
        this.lastname = lastname;
        return this;
    }

    public String getEmail() {
        return email;
    }

    public Student setEmail(String email) {
        this.email = email;
        return this;
    }
}
