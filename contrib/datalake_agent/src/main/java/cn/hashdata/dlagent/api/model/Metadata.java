package cn.hashdata.dlagent.api.model;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import org.apache.commons.lang.StringUtils;
import cn.hashdata.dlagent.api.utilities.EnumGpdbType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Metadata holds an item's metadata information.
 */
public class Metadata {

    /**
     * Class representing item name - db/schema/path name and table/file name.
     */
    public static class Item {
        private String path;
        private String name;

        public Item(String path, String itemName) {

            if (StringUtils.isBlank(path) || StringUtils.isBlank(itemName)) {
                throw new IllegalArgumentException("Item or path name cannot be empty");
            }

            this.path = path;
            this.name = itemName;
        }

        public String getPath() {
            return path;
        }

        public String getName() {
            return name;
        }

        /**
         * Returns full item name in the form path.name
         * eg: dbname.tblname
         */
        @Override
        public String toString() {
            return path + "." + name;
        }
    }

    /**
     * Class representing item field - name, type, source type, is complex type?, modifiers.
     * Type - exposed type of field
     * Source type - type of field in underlying source
     * Is complex type - whether source type is complex type
     * Modifiers - additional attributes which describe type or field
     */
    public static class Field {
        private String name;
        private EnumGpdbType type; // field type which dlagent exposes
        private boolean isComplexType; // whether source field's type is complex
        private String sourceType; // field type dlagent reads from
        private String[] modifiers; // type modifiers, optional field

        public Field(String name, EnumGpdbType type, String sourceType) {
            if (StringUtils.isBlank(name)
                    || type == null
                    || StringUtils.isBlank(sourceType)) {
                throw new IllegalArgumentException(
                        "Field name, type and source type cannot be empty");
            }
            this.name = name;
            this.type = type;
            this.sourceType = sourceType;
        }

        public Field(String name, EnumGpdbType type, String sourceType, String[] modifiers) {
            this(name, type, sourceType);
            this.modifiers = modifiers;
        }

        public Field(String name, EnumGpdbType type, boolean isComplexType, String sourceType, String[] modifiers) {
            this(name, type, sourceType);
            this.modifiers = modifiers;
            this.isComplexType = isComplexType;
        }

        public String getName() {
            return name;
        }

        public EnumGpdbType getType() {
            return type;
        }

        public String getSourceType() {
            return sourceType;
        }

        public String[] getModifiers() {
            return modifiers;
        }

        public boolean isComplexType() {
            return isComplexType;
        }

        public void setComplexType(boolean isComplexType) {
            this.isComplexType = isComplexType;
        }
    }

    /**
     * Item name
     */
    private Item item;

    /**
     * Item's fields
     */
    private List<Field> fields;
    private Map<String, String> outputParameters;

    /**
     * Returns an item's output parameters, for example - delimiters etc.
     *
     * @return item's output parameters
     */
    public Map<String, String> getOutputParameters() {
        return outputParameters;
    }

    public void setOutputParameters(Map<String, String> outputParameters) {
        this.outputParameters = outputParameters;
    }

    /**
     * Constructs an item's Metadata.
     *
     * @param itemName the item name
     * @param fields the item's fields
     */
    public Metadata(Item itemName,
            List<Field> fields) {
        this.item = itemName;
        this.fields = fields;
    }

    public Metadata(Item itemName) {
        this(itemName, new ArrayList<Field>());
    }

    public Item getItem() {
        return item;
    }

    public List<Field> getFields() {
        return fields;
    }

    /**
     * Adds a field to metadata fields.
     *
     * @param field field to add
     */
    public void addField(Field field) {
        if (fields == null) {
            fields = new ArrayList<Field>();
        }
        fields.add(field);
    }
}
