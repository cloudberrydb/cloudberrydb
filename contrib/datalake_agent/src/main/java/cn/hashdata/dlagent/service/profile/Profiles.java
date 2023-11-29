package cn.hashdata.dlagent.service.profile;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

/**
 * Profiles is the root element for the list of profiles
 * defined in the profiles XML file
 */
@XmlRootElement(name = "profiles")
@XmlAccessorType(XmlAccessType.FIELD)
public class Profiles {

    @XmlElement(name = "profile")
    private List<Profile> profiles;

    /**
     * Returns a list of {@link Profile} objects
     * @return a list of {@link Profile} objects
     */
    public List<Profile> getProfiles() {
        return profiles;
    }
}
