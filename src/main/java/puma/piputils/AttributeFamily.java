package puma.piputils;

public class AttributeFamily {
	
	public final Multiplicity multiplicity;
	public final DataType dataType;
	public final String simpleName;
	public final String xacmlName;
	
	public AttributeFamily(Multiplicity multiplicity, DataType dataType, String xacmlName) {
		this.multiplicity = multiplicity;
		this.dataType = dataType;
		this.xacmlName = xacmlName;
		this.simpleName = xacmlName.substring(8, xacmlName.length()); // TODO subject to change
	}

}
