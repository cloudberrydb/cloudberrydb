package cn.hashdata.dlagent.service.utilities;

import org.apache.commons.lang.StringUtils;
import cn.hashdata.dlagent.api.model.Plugin;
import cn.hashdata.dlagent.api.model.RequestContext;
import org.springframework.stereotype.Component;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * The base implementation of the {@code PluginFactory}
 */
@Component
public class BasePluginFactory {

    public <T extends Plugin> T getPlugin(RequestContext context, String pluginClassName) {

        // get the class name of the plugin
        if (StringUtils.isBlank(pluginClassName)) {
            throw new RuntimeException("Could not determine plugin class name");
        }

        // load the class by name
        Class<?> cls;
        try {
            cls = Class.forName(pluginClassName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(String.format("Class %s is not found", pluginClassName), e);
        }

        // check if the class is a plugin
        if (!Plugin.class.isAssignableFrom(cls)) {
            throw new RuntimeException(String.format("Class %s does not implement Plugin interface", pluginClassName));
        }

        // get the empty constructor
        Constructor<?> con;
        try {
            con = cls.getConstructor();
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(String.format("Class %s does not have an empty constructor", pluginClassName));
        }

        // create plugin instance
        Plugin instance;
        try {
            instance = (Plugin) con.newInstance();
        } catch (InvocationTargetException e) {
            throw (e.getCause() != null) ? new RuntimeException(e.getCause()) :
                    new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Class %s could not be instantiated", pluginClassName), e);
        }

        // initialize the instance
        instance.setRequestContext(context);
        instance.afterPropertiesSet();

        // cast into a target type
        @SuppressWarnings("unchecked")
        T castInstance = (T) instance;

        return castInstance;
    }
}
