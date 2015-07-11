package org.cataractsoftware.datasponge;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * structure to hold data extracted from sites. Any two records that share the same identifier are considered to be equivalent (regardless of the fields they possess).
 *
 * @author Christopher Fagiani
 */
public class DataRecord implements Serializable {
    private String identifier;
    private String type;

    private Map<String, Object> fields;

    public DataRecord(String identifier, String type) {
        if (identifier == null) {
            throw new IllegalArgumentException("Cannot have null identifier");
        }
        if (type == null) {
            throw new IllegalArgumentException("Cannot have null type");
        }
        this.identifier = identifier;
        this.type = type;
        fields = new HashMap<String, Object>();
    }

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public void setField(String name, Object val) {
        fields.put(name, val);
    }

    public Object getFieldValue(String name) {
        return fields.get(name);
    }

    public int getFieldCount() {
        return fields.size();
    }

    public Set<Map.Entry<String, Object>> getFields() {
        return fields.entrySet();
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DataRecord that = (DataRecord) o;

        if (!identifier.equals(that.identifier)) return false;
        if (!type.equals(that.type)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = identifier.hashCode();
        result = 31 * result + type.hashCode();
        return result;
    }
}
