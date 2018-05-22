/*
 * The MIT License (MIT)
 * Copyright (c) 2018 Microsoft Corporation
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.microsoft.azure.cosmosdb.internal.routing;

import java.io.IOException;
import java.io.OutputStream;

import com.fasterxml.jackson.core.JsonGenerator;

class BoolPartitionKeyComponent implements IPartitionKeyComponent {

    private final boolean value;

    public BoolPartitionKeyComponent(boolean value) {
        this.value = value;
    }

    @Override
    public int compareTo(IPartitionKeyComponent other) {
        if (!(other instanceof BoolPartitionKeyComponent)) {
            throw new IllegalArgumentException("other");
        }

        return (int) Math.signum((this.value ? 1 : 0) - (((BoolPartitionKeyComponent) other).value ? 1 : 0));
    }

    @Override
    public int getTypeOrdinal() {
        return this.value ? PartitionKeyComponentType.TRUE.getValue() : PartitionKeyComponentType.FALSE.getValue();
    }

    @Override
    public void jsonEncode(JsonGenerator writer) {
        try {
            writer.writeBoolean(this.value);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void writeForHashing(OutputStream outputStream) {
        try {
            outputStream.write((byte) (this.value ? PartitionKeyComponentType.TRUE.getValue()
                    : PartitionKeyComponentType.FALSE.getValue()));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void writeForBinaryEncoding(OutputStream outputStream) {
        try {
            outputStream.write((byte) (this.value ? PartitionKeyComponentType.TRUE.getValue()
                    : PartitionKeyComponentType.FALSE.getValue()));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
