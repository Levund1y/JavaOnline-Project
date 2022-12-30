import org.apache.commons.lang3.time.DateUtils;

import java.time.LocalDateTime;

public class Earthquake {

    public String ID;
    public int depth;
    public String magnitudeType;
    public double magnitude;
    public String state;
    public String time;



    public Earthquake(String[] str){
        this.ID = str[0];
        this.depth = Integer.parseInt(str[1]);
        this.magnitudeType = str[2];
        this.magnitude = Double.parseDouble(str[3]);
        this.state = str[4];
        this.time = str[5];
    }

}
