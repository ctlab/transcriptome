package ru.ifmo.genetics.utils.tool.inputParameterBuilder;

import org.jetbrains.annotations.NotNull;
import ru.ifmo.genetics.utils.tool.parameters.ParameterDescription;

public class BoolParameterBuilder extends ParameterBuilder<Boolean> {

    public BoolParameterBuilder(@NotNull String name) {
        super(Boolean.class, name);
    }

    @Override
    public ParameterDescription<Boolean> create() {
        withDefaultValue(false);
        if (defaultComment == null) {
            withDefaultComment("");
        }
        return super.create();
    }
}
