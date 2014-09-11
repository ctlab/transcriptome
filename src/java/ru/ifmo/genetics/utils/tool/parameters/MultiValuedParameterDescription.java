package ru.ifmo.genetics.utils.tool.parameters;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.ifmo.genetics.utils.tool.values.InValue;

public class MultiValuedParameterDescription<T> extends ParameterDescription<T> {

    public MultiValuedParameterDescription(@NotNull Class elementClass, @NotNull String name, @Nullable String shortOpt, @Nullable InValue<T> defaultValue, @Nullable String defaultComment, @NotNull String description, boolean mandatory) {
        super(elementClass, name, shortOpt, true, defaultValue, defaultComment, description, mandatory);
    }


    @Override
    public String toString() {
        return "MultiValuedParameterDescription{" +
                "elementClass=" + tClass +
                ", name='" + name + '\'' +
                ", shortOpt='" + shortOpt + '\'' +
                ", defaultValue=" + defaultValue +
                ", description='" + description + '\'' +
                ", mandatory='" + mandatory + '\'' +
                '}';
    }
}
