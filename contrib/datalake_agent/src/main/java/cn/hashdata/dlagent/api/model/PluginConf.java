package cn.hashdata.dlagent.api.model;

import java.util.Map;

public interface PluginConf {

    Map<String, String> getOptionMappings(String key);

    Map<String, String> getPlugins(String key);
}