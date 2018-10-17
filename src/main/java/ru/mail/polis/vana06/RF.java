package ru.mail.polis.vana06;

import java.util.StringTokenizer;

public class RF {
    private final String errorMsg = "The replicas parameter must contain 2 non-negative integer separated by a symbol \"/\" -> \"ack/from\"\n" +
            "The ack value must be less than or equal to the value of from";
    private final int ack, from;

    public RF(String str) throws IllegalArgumentException{
        StringTokenizer st = new StringTokenizer(str, "/");
        if (st.countTokens() != 2){
            throw new IllegalArgumentException(errorMsg);
        }
        try {
            ack = Integer.parseInt(st.nextToken());
            from = Integer.parseInt(st.nextToken());
        } catch (NumberFormatException e){
            throw new IllegalArgumentException(errorMsg);
        }
        validate();
    }

    public RF(int ack, int from) throws IllegalArgumentException {
        this.ack = ack;
        this.from = from;
        validate();
    }

    private void validate() throws IllegalArgumentException{
        if(ack > from){
            throw new IllegalArgumentException(errorMsg);
        }
        if(ack <= 0){
            throw new IllegalArgumentException(errorMsg);
        }
    }

}
