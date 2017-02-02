/*
 * Copyright (C) 2009-2017 Lightbend Inc. <https://www.lightbend.com>
 */
package com.lightbend.lagom.sbt;

import io.swagger.codegen.*;
import io.swagger.codegen.languages.AbstractJavaCodegen;
import io.swagger.models.Model;
import io.swagger.models.Operation;
import io.swagger.models.Swagger;
import io.swagger.models.properties.ArrayProperty;
import io.swagger.models.properties.MapProperty;
import io.swagger.models.properties.Property;
import io.swagger.util.Json;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class LagomJavaCodegen extends AbstractJavaCodegen {

    private static final String TEMPLATE_DIRECTORY_NAME = "codegen-java-templates";

    public LagomJavaCodegen() {
        super();
        invokerPackage = "com.example.codegen.lagom";

        sourceFolder = "java";
        dateLibrary = "java8";

        modelTemplateFiles.put("model.mustache", ".java");
        apiTemplateFiles.put("api.mustache", ".java");
        //TODO: add doc templates
        modelDocTemplateFiles.remove("model_doc.mustache");
        apiDocTemplateFiles.remove("api_doc.mustache");


        // TODO: use immutables
        instantiationTypes.put("array", "ArrayList");
        instantiationTypes.put("map", "HashMap");


        apiPackage = "io.swagger.api";
        modelPackage = "io.swagger.model";

        apiTestTemplateFiles.clear(); // TODO: add api test template
        modelTestTemplateFiles.clear(); // TODO: add model test template


        additionalProperties.put("title", "Lagom Descriptor");

        super.embeddedTemplateDir = templateDir = TEMPLATE_DIRECTORY_NAME;

        for (int i = 0; i < cliOptions.size(); i++) {
            if (CodegenConstants.LIBRARY.equals(cliOptions.get(i).getOpt())) {
                cliOptions.remove(i);
                break;
            }
        }

        CliOption library = new CliOption(CodegenConstants.LIBRARY, "library template (sub-template) to use");
        library.setDefault(DEFAULT_LIBRARY);

        Map<String, String> supportedLibraries = new LinkedHashMap<String, String>();

        supportedLibraries.put(DEFAULT_LIBRARY, "lagomJavadslApi");
        library.setEnum(supportedLibraries);

        cliOptions.add(library);

    }

    @Override
    public CodegenOperation fromOperation(String path, String httpMethod, Operation operation, Map<String, Model> definitions, Swagger swagger) {
        CodegenOperation op = super.fromOperation(path, httpMethod, operation, definitions, swagger);

        op.path = op.path.replaceAll("\\{([^}]*)\\}", ":$1");

        return op;
    }


    @Override
    public void processOpts() {
        super.processOpts();

        // Don't need extra files provided by Lagom & Java Codegen
        supportingFiles.clear();

        // Use pcollections
        typeMapping.put("array", "org.pcollections.PSequence");
        typeMapping.put("map", "org.pcollections.PMap");
    }


    @Override
    public CodegenType getTag() {
        return CodegenType.OTHER;
    }

    @Override
    public String getName() {
        return "lagom-service-descriptor";
    }

    @Override
    public void addOperationToGroup(String tag, String resourcePath, Operation operation, CodegenOperation co, Map<String, List<CodegenOperation>> operations) {
        String basePath = resourcePath;
        if (basePath.startsWith("/")) {
            basePath = basePath.substring(1);
        }
        int pos = basePath.indexOf("/");
        if (pos > 0) {
            basePath = basePath.substring(0, pos);
        }

        if (basePath == "") {
            basePath = "default";
        } else {
            if (co.path.startsWith("/" + basePath)) {
                co.path = co.path.substring(("/" + basePath).length());
            }
            co.subresourceOperation = !co.path.isEmpty();
        }
        List<CodegenOperation> opList = operations.get(basePath);
        if (opList == null) {
            opList = new ArrayList<CodegenOperation>();
            operations.put(basePath, opList);
        }
        opList.add(co);
        co.baseName = basePath;
    }

    private boolean safeValue(Boolean value) {
        return value != null && value;
    }

    @Override
    public void postProcessModelProperty(CodegenModel model, CodegenProperty property) {
        super.postProcessModelProperty(model, property);
        model.imports.remove("ApiModelProperty");
        model.imports.remove("ApiModel");
        model.imports.remove("JsonSerialize");
        model.imports.remove("ToStringSerializer");
        model.imports.remove("JsonValue");
        model.imports.remove("JsonProperty");

        if (safeValue(property.required) && !isListOrContainer(property)) {
            property.vendorExtensions.put("requiredByBuilder", true);
        }

        // We've messed things up in the case of optional enums, fix that
        {
            String optionalType = (String) property.vendorExtensions.get("optionalType");
            if (optionalType != null) {
                if (property.isEnum) {
                    // The swagger codegen approach to enums is such an awful hack, rather than creating a descriptor
                    // specifically for the enum, and making the property just the type of the enum, they create this
                    // extra datatypeWithEnum field, which is just stupid. So we change to make an additional property
                    // that we attach to this one that is used to describe how to create the enum.
                    CodegenProperty enumProperty = property.clone();
                    enumProperty.vendorExtensions = new HashMap<>(property.vendorExtensions);
                    enumProperty.vendorExtensions.remove("optionalType");
                    enumProperty.datatype = optionalType;
                    // Some post processing is done on allowableValues that we need to be applied, so share the same
                    // values
                    enumProperty.allowableValues = property.allowableValues;


                    property.vendorExtensions.put("optionalType", property.datatypeWithEnum);
                    property.datatype = optionalType;
                    property.datatypeWithEnum = "java.util.Optional<" + property.datatypeWithEnum + ">";
                    property.vendorExtensions.put("enumProperty", enumProperty);
                }
            } else if (property.isEnum) {
                property.vendorExtensions.put("enumProperty", property);
            }
        }

        // Also fix optional lists up
        if (isListOrContainer(property)) {
            String originalType = (String) property.items.vendorExtensions.get("originalType");
            if (originalType != null) {
                property.items.vendorExtensions.remove("originalType");
                property.items.datatypeWithEnum = originalType;
            }
        }
    }

    private boolean isListOrContainer(CodegenProperty property) {
        return safeValue(property.isListContainer) || safeValue(property.isMapContainer);
    }

    @Override
    public void preprocessSwagger(Swagger swagger) {
        //copy input swagger to output folder 
        try {
            String swaggerJson = Json.pretty(swagger);
            FileUtils.writeStringToFile(new File(outputFolder + File.separator + "swagger.json"), swaggerJson);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e.getCause());
        }
        super.preprocessSwagger(swagger);

    }

    @Override
    public String getHelp() {
        return "Generates a Lagom Service Descriptor.";
    }

    @Override
    public String getTypeDeclaration(Property p) {
        if (p instanceof ArrayProperty) {
            ArrayProperty ap = (ArrayProperty) p;
            Property inner = ap.getItems();
            if (inner == null) {
                LOGGER.warn(ap.getName() + "(array property) does not have a proper inner type defined");
                // TODO maybe better defaulting to StringProperty than returning null
                return null;
            }
            return getSwaggerType(p) + "<" + super.getTypeDeclaration(inner) + ">";
        } else if (p instanceof MapProperty) {
            MapProperty mp = (MapProperty) p;
            Property inner = mp.getAdditionalProperties();
            if (inner == null) {
                LOGGER.warn(mp.getName() + "(map property) does not have a proper inner type defined");
                // TODO maybe better defaulting to StringProperty than returning null
                return null;
            }
            return getSwaggerType(p) + "<String, " + super.getTypeDeclaration(inner) + ">";
        } else if (!p.getRequired()) {
            String typeDeclaration = super.getTypeDeclaration(p);
            p.getVendorExtensions().put("originalType", typeDeclaration);
            p.getVendorExtensions().put("optionalType", convertToPrimitives(typeDeclaration));
            return "java.util.Optional<" + typeDeclaration + ">";
        } else {
            String typeDeclaration = super.getTypeDeclaration(p);
            return convertToPrimitives(typeDeclaration);
        }
    }

    private String convertToPrimitives(String typeDeclaration) {
        switch (typeDeclaration) {
            case "Integer":
                return "int";
            case "Long":
                return "long";
            case "Float":
                return "float";
            case "Double":
                return "double";
            case "Boolean":
                return "boolean";
        }
        return typeDeclaration;
    }

    @Override
    public String toDefaultValue(Property p) {
        if (p instanceof ArrayProperty) {
            return "org.pcollections.TreePVector.empty()";
        } else if (p instanceof MapProperty) {
            return "org.pcollections.HashTreePMap.empty()";
        } else if (!p.getRequired()) {
            return "java.util.Optional.empty()";
        } else {
            return "null";
        }
    }


    @Override
    public CodegenModel fromModel(String name, Model model, Map<String, Model> allDefinitions) {
        CodegenModel codegenModel = super.fromModel(name, model, allDefinitions);

        List<CodegenProperty> builderRequiredVars = new ArrayList<>();
        for (CodegenProperty var: codegenModel.requiredVars) {
            if (!isListOrContainer(var)) {
                builderRequiredVars.add(var);
            }
        }
        codegenModel.vendorExtensions.put("builderRequiredVars", builderRequiredVars);

        return codegenModel;
    }
}
