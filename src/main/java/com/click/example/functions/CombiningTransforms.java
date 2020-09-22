package com.click.example.functions;

import org.apache.beam.sdk.transforms.SerializableFunction;

public class CombiningTransforms implements SerializableFunction<Iterable<String>, String> {
    @Override
    public String apply(Iterable<String> input) {
        String result = "";
        for (String item : input) {
            result += item;
        }
        return result;
    }
}