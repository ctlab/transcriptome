package ru.ifmo.genetics.utils.tool.inputParameterBuilder;

import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.ifmo.genetics.utils.tool.parameters.MultiValuedParameterDescription;
import ru.ifmo.genetics.utils.tool.parameters.ParameterDescription;
import ru.ifmo.genetics.utils.tool.values.InValue;
import ru.ifmo.genetics.utils.tool.values.SimpleInValue;

public class ParameterBuilder<T> {
    Logger logger = Logger.getLogger(ParameterBuilder.class);

    @NotNull protected Class<T> tClass;
    @NotNull protected String name;
    @Nullable protected String shortOpt;
    @Nullable protected InValue<T> defaultValue;
    @Nullable protected String defaultComment;
    @Nullable protected String description;
    protected boolean mandatory = false;


    public ParameterBuilder(@NotNull ParameterDescription<T> other) {
        this.tClass = other.tClass;
        this.name = other.name;
        this.shortOpt = other.shortOpt;
        this.defaultValue = other.defaultValue;
        this.defaultComment = other.defaultComment;
        this.description = other.description;
        this.mandatory = other.mandatory;
    }

    public ParameterBuilder(@NotNull Class<T> tClass, @NotNull String name) {
        this.tClass = tClass;
        this.name = name;
    }


    
    public ParameterBuilder<T> withName(@NotNull String name) {
        this.name = name;
        return this;
    }

    public ParameterBuilder<T> withShortOpt(@Nullable String shortOpt) {
        this.shortOpt = shortOpt;
        return this;
    }


    public ParameterBuilder<T> withDefaultValue(@Nullable InValue<T> defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }
    public ParameterBuilder<T> withDefaultValue(@Nullable T defaultValue) {
        this.defaultValue = new SimpleInValue<T>(defaultValue);
        return this;
    }

    public ParameterBuilder<T> withDefaultComment(@Nullable String defaultComment) {
        this.defaultComment = defaultComment;
        return this;
    }
    

    public ParameterBuilder<T> withDescription(@NotNull String description) {
        this.description = description;
        return this;
    }


    public ParameterBuilder<T> mandatory() {
        this.mandatory = true;
        return this;
    }
    public ParameterBuilder<T> optional() {
        this.mandatory = false;
        return this;
    }


    public ParameterDescription<T> create() {
        boolean hasArg = (tClass != Boolean.class);

        if (description == null) {
            throw new IllegalArgumentException("description shouldn't be null");
        }
        if (defaultValue != null && mandatory) {
            logger.warn("Parameter " + name + " is mandatory but has default value " + defaultValue);
        }

        return new ParameterDescription<T>(tClass, name, shortOpt, hasArg, defaultValue, defaultComment, description, mandatory);
    }


    public static ParameterBuilder createBuilder(ParameterDescription description) {
        if (description instanceof MultiValuedParameterDescription) {
            //noinspection unchecked
            return new MultiValuedParameterBuilder((MultiValuedParameterDescription) description);
        } else {
            //noinspection unchecked
            return new ParameterBuilder(description);
        }
    }
}