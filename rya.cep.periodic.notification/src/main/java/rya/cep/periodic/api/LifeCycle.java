package rya.cep.periodic.api;

public interface LifeCycle {

    public void start();

    public void stop();
    
    public boolean currentlyRunning();

}
