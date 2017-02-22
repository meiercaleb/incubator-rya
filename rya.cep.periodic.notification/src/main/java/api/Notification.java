package api;

import java.util.Optional;

public interface Notification {

	public String getId();
	
	public Optional<String> getMessage();
}
