package puma.piputils;

public class Tuple<S, D> {
	private S data;
	private D type;
	
	public Tuple(S data, D type) {
		this.data = data;
		this.type = type;
	}
	
	public S getData() {
		return this.data;
	}
	
	public D getType() {
		return this.type;
	}
	
	public boolean hasType() {
		return this.getType() != null;
	}
}
