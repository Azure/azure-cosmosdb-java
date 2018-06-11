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
import java.io.UnsupportedEncodingException;

import com.fasterxml.jackson.core.JsonGenerator;

class StringPartitionKeyComponent implements IPartitionKeyComponent {

    public static final int MAX_STRING_COMPONENT_LENGTH = 100;
    private final String value;

    public StringPartitionKeyComponent(String value) {
        if (value == null) {
            throw new IllegalArgumentException("value");
        }

        this.value = value.length() < MAX_STRING_COMPONENT_LENGTH ? value : value.substring(0, MAX_STRING_COMPONENT_LENGTH);
    }

    @Override
    public int compareTo(IPartitionKeyComponent other) {
        if (!(other instanceof StringPartitionKeyComponent)) {
            throw new IllegalArgumentException("other");
        }

        StringPartitionKeyComponent otherComponent = (StringPartitionKeyComponent) other;

        byte[] left;
        byte[] right;
        try {
            left = this.value.getBytes("UTF-8");
            right = otherComponent.value.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
        }

        for (int i = 0; i < Math.min(left.length, right.length); i++) {
            if (left[i] != right[i]) {
                return (int) Math.signum(left[i] - right[i]);
            }
        }

        return (int) Math.signum(left.length - right.length);
    }

    @Override
    public int getTypeOrdinal() {
        return PartitionKeyComponentType.STRING.getValue();
    }

    @Override
    public void jsonEncode(JsonGenerator writer) {
        try {
            writer.writeString(this.value);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void writeForHashing(OutputStream outputStream) {
        try {
            outputStream.write(PartitionKeyComponentType.STRING.getValue());
            outputStream.write(this.value.getBytes("UTF-8"));
            outputStream.write((byte) 0);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void writeForBinaryEncoding(OutputStream outputStream) {
        try {
            outputStream.write(PartitionKeyComponentType.STRING.getValue());

            boolean shortString = true;

            for (int index = 0; index < this.value.length(); index++) {
                byte charByte = (byte) this.value.charAt(index);
                if (index <= MAX_STRING_COMPONENT_LENGTH) {
                    if (charByte < 0xFF) charByte++;
                    outputStream.write(charByte);
                } else {
                    shortString = false;
                    break;
                }
            }

            if (shortString) {
                outputStream.write((byte) 0x00);
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

}
