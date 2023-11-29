package cn.hashdata.dlagent.service.profile;

import org.apache.commons.lang.StringUtils;

import javax.xml.bind.annotation.*;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Represents a dlagent {@link Profile}. A profile is a way
 * to simplify access to external data sources. The
 * {@link Profile} consists of {@link Plugins} a protocol
 * and a list of option mappings
 */
@XmlRootElement(name = "profile")
public class Profile {

    @XmlElement(name = "name", required = true)
    private String name;

    @XmlTransient
    private Plugins plugins;

    @XmlElementWrapper(name = "optionMappings")
    @XmlElement(name = "mapping")
    private List<Mapping> mappingList;

    @XmlTransient
    private Map<String, String> optionsMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    @XmlTransient
    private Map<String, String> pluginsMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    /**
     * Returns the name of the profile
     *
     * @return the name of the profile
     */
    String getName() {
        return StringUtils.trim(name);
    }

    /**
     * Returns the options map for this profile. (optional)
     *
     * @return (optional) the options map for this profile
     */
    Map<String, String> getOptionsMap() {
        return optionsMap;
    }

    /**
     * Returns the map of plugins configured for this profile
     *
     * @return the map of plugins configured for this profile
     */
    Map<String, String> getPluginsMap() {
        return pluginsMap;
    }

    List<Mapping> getMappingList() {
        return mappingList;
    }

    @XmlElement(name = "plugins")
    private void setPlugins(Plugins plugins) {
        this.plugins = plugins;

        if (plugins != null) {
            String metadata = plugins.getMetadata();
            if (StringUtils.isNotBlank(metadata)) {
                pluginsMap.put(Plugins.METADATA, metadata);
            }
        }
    }

    private Plugins getPlugins() {
        return plugins;
    }

    /**
     * Plugins identify the fully qualified name of Java
     * classes that dlagent uses to parse and access the
     * external data.
     */
    @XmlRootElement(name = "plugins")
    @XmlAccessorType(XmlAccessType.FIELD)
    static class Plugins {

        final static String METADATA = "METADATA";

        @XmlElement(name = "metadata")
        private String metadata;

        /**
         * Returns the fully qualified class name for the Profile's metadata
         *
         * @return the fully qualified class name for the Profile's metadata
         */
        String getMetadata() {
            return metadata;
        }
    }

    /**
     * A mapping defines a whitelisted option that is allowed
     * for the given profile. The option maps to a property that
     * can be interpreted by the Profile Plugins.
     */
    @XmlRootElement(name = "mapping")
    @XmlAccessorType(XmlAccessType.FIELD)
    static class Mapping {

        @XmlAttribute(name = "option")
        private String option;

        @XmlAttribute(name = "property")
        private String property;

        /**
         * Returns the whitelisted option
         *
         * @return the whitelisted option
         */
        String getOption() {
            return option;
        }

        /**
         * Returns the name of the property for the given option
         *
         * @return the name of the property for the given option
         */
        String getProperty() {
            return property;
        }
    }
}
