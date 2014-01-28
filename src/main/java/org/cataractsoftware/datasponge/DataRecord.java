package org.cataractsoftware.datasponge;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * structure to hold data extracted from sites. Any two records that share the same identifier are considered to be equivalent (regardless of the fields they possess).
 *
 * @author Christopher Fagiani
 */
public class DataRecord<T> implements Comparable<DataRecord<T>> {
    private String identifier;

    private Map<String, T> fields;

    public DataRecord(String identifier) {
        if (identifier == null) {
            throw new IllegalArgumentException("Cannot have null identifier");
        }
        this.identifier = identifier;
        fields = new HashMap<String, T>();
    }

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public void setField(String name, T val) {
        fields.put(name, val);
    }

    public T getFieldValue(String name) {
        return fields.get(name);
    }

    public int getFieldCount() {
        return fields.size();
    }

    public Set<Map.Entry<String, T>> getFields() {
        return fields.entrySet();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DataRecord that = (DataRecord) o;

        if (!identifier.equals(that.identifier)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return identifier.hashCode();
    }

    @Override
    public int compareTo(DataRecord<T> o) {
        if (o == this) {
            return 0;
        } else {
            return getIdentifier().compareTo(o.getIdentifier());
        }
    }
}
