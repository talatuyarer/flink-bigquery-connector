package com.google.cloud.flink.bigquery.sink.fastserializer;

import com.google.cloud.flink.bigquery.sink.exceptions.BigQuerySerializationException;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.ListIterator;

import java.util.Map;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.dynamic.DynamicType.Builder.MethodDefinition.ReceiverTypeDefinition;
import net.bytebuddy.implementation.FixedValue;
import com.google.protobuf.ByteString;
import org.apache.avro.Schema;
import org.apache.avro.io.parsing.ResolvingGrammarGenerator;
import org.apache.avro.io.parsing.Symbol;

public class ProtoSerializerGenerator {

    private Map<Long, Schema> schemaMap = new HashMap<>();

    public static <T> ProtoSerializer<T> generateSerializer(Class<T> messageType, Schema aliasedWriterSchema,
            Schema readerSchema) {

        ReceiverTypeDefinition<ProtoSerializer> builder = new ByteBuddy()
            .subclass(ProtoSerializer.class)
            .implement(ProtoSerializer.class) // Specify the type parameter
            .defineMethod("serialize", ByteString.class, Visibility.PUBLIC)
            .withParameter(messageType) // Correct parameter type
            .intercept(FixedValue.value(ByteString.EMPTY)); // Placeholder serialization

        try {
            Symbol resolvingGrammar = new ResolvingGrammarGenerator()
                    .generate(aliasedWriterSchema, readerSchema);
            FieldAction fieldAction = FieldAction
                    .fromValues(aliasedWriterSchema.getType(), true, resolvingGrammar);

            return builder.make()
                .load(ProtoSerializer.class.getClassLoader())
                .getLoaded()
                .getDeclaredConstructor()
                .newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Error generating serializer", e);
        }
    }

    private boolean schemaNotRegistered(final Schema schema) {
        return !schemaMap.containsKey(SerializerRegistry.getSchemaFingerprint(schema));
    }

    protected ListIterator<Symbol> actionIterator(FieldAction action)
            throws BigQuerySerializationException {
        ListIterator<Symbol> actionIterator = null;

        if (action.getSymbolIterator() != null) {
            actionIterator = action.getSymbolIterator();
        } else if (action.getSymbol().production != null) {
            actionIterator = Arrays.asList(reverseSymbolArray(action.getSymbol().production))
                    .listIterator();
        } else {
            actionIterator = Collections.emptyListIterator();
        }

        while (actionIterator.hasNext()) {
            Symbol symbol = actionIterator.next();

            if (symbol instanceof Symbol.ErrorAction) {
                throw new BigQuerySerializationException(((Symbol.ErrorAction) symbol).msg);
            }

            if (symbol instanceof Symbol.FieldOrderAction) {
                break;
            }
        }

        return actionIterator;
    }

    protected static Symbol[] reverseSymbolArray(Symbol[] symbols) {
        Symbol[] reversedSymbols = new Symbol[symbols.length];

        for (int i = 0; i < symbols.length; i++) {
            reversedSymbols[symbols.length - i - 1] = symbols[i];
        }

        return reversedSymbols;
    }

    protected void forwardToExpectedDefault(ListIterator<Symbol> symbolIterator)
            throws BigQuerySerializationException {
        Symbol symbol;
        while (symbolIterator.hasNext()) {
            symbol = symbolIterator.next();

            if (symbol instanceof Symbol.ErrorAction) {
                throw new BigQuerySerializationException(((Symbol.ErrorAction) symbol).msg);
            }

            if (symbol instanceof Symbol.DefaultStartAction) {
                return;
            }
        }
        throw new BigQuerySerializationException("DefaultStartAction symbol expected!");
    }

    protected FieldAction seekFieldAction(boolean shouldReadCurrent, Schema.Field field,
            ListIterator<Symbol> symbolIterator) throws BigQuerySerializationException {

        Schema.Type type = field.schema().getType();

        if (!shouldReadCurrent) {
            return FieldAction.fromValues(type, false, EMPTY_SYMBOL);
        }

        boolean shouldRead = true;
        Symbol fieldSymbol = END_SYMBOL;

        if (Schema.Type.RECORD.equals(type)) {
            if (symbolIterator.hasNext()) {
                fieldSymbol = symbolIterator.next();
                if (fieldSymbol instanceof Symbol.SkipAction) {
                    return FieldAction.fromValues(type, false, fieldSymbol);
                } else {
                    symbolIterator.previous();
                }
            }
            return FieldAction.fromValues(type, true, symbolIterator);
        }

        while (symbolIterator.hasNext()) {
            Symbol symbol = symbolIterator.next();

            if (symbol instanceof Symbol.ErrorAction) {
                throw new BigQuerySerializationException(((Symbol.ErrorAction) symbol).msg);
            }

            if (symbol instanceof Symbol.SkipAction) {
                shouldRead = false;
                fieldSymbol = symbol;
                break;
            }

            if (symbol instanceof Symbol.WriterUnionAction) {
                if (symbolIterator.hasNext()) {
                    symbol = symbolIterator.next();

                    if (symbol instanceof Symbol.Alternative) {
                        shouldRead = true;
                        fieldSymbol = symbol;
                        break;
                    }
                }
            }

            if (symbol.kind == Symbol.Kind.TERMINAL) {
                shouldRead = true;
                if (symbolIterator.hasNext()) {
                    symbol = symbolIterator.next();

                    if (symbol instanceof Symbol.Repeater) {
                        fieldSymbol = symbol;
                    } else {
                        fieldSymbol = symbolIterator.previous();
                    }
                } else if (!symbolIterator.hasNext() && getSymbolPrintName(symbol) != null) {
                    fieldSymbol = symbol;
                }
                break;
            }
        }

        return FieldAction.fromValues(type, shouldRead, fieldSymbol);
    }

    protected static String getSymbolPrintName(Symbol symbol)
            throws BigQuerySerializationException {
        String printName;
        try {
            Field field = symbol.getClass().getDeclaredField("printName");

            field.setAccessible(true);
            printName = (String) field.get(symbol);
            field.setAccessible(false);

        } catch (ReflectiveOperationException e) {
            throw new BigQuerySerializationException("getSymbolPrintName",e);
        }

        return printName;
    }

    protected static final Symbol EMPTY_SYMBOL = new Symbol(Symbol.Kind.TERMINAL, new Symbol[]{}) {
    };

    protected static final Symbol END_SYMBOL = new Symbol(Symbol.Kind.TERMINAL, new Symbol[]{}) {
    };

    protected static final class FieldAction {

        private Schema.Type type;
        private boolean shouldRead;
        private Symbol symbol;
        private ListIterator<Symbol> symbolIterator;

        private FieldAction(Schema.Type type, boolean shouldRead, Symbol symbol) {
            this.type = type;
            this.shouldRead = shouldRead;
            this.symbol = symbol;
        }

        private FieldAction(Schema.Type type, boolean shouldRead, ListIterator<Symbol> symbolIterator) {
            this.type = type;
            this.shouldRead = shouldRead;
            this.symbolIterator = symbolIterator;
        }

        public static FieldAction fromValues(Schema.Type type, boolean read, Symbol symbol) {
            return new FieldAction(type, read, symbol);
        }

        public static FieldAction fromValues(Schema.Type type, boolean read,
                ListIterator<Symbol> symbolIterator) {
            return new FieldAction(type, read, symbolIterator);
        }

        public Schema.Type getType() {
            return type;
        }

        public boolean getShouldRead() {
            return shouldRead;
        }

        public Symbol getSymbol() {
            return symbol;
        }

        public ListIterator<Symbol> getSymbolIterator() {
            return symbolIterator;
        }
    }
}
