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
package com.microsoft.azure.cosmosdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;

import org.testng.annotations.Test;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static org.assertj.core.api.Assertions.assertThat;

public class JsonSerializableTests {

    public static class Pojo implements Serializable {
        int a;
        int b;

        public Pojo(int a, int b) {
            this.a = a;
            this.b = b;
        }

        @JsonCreator
        public Pojo(@JsonProperty("a") String a, @JsonProperty("b") String b) {
            this.a = Integer.parseInt(a);
            this.b = Integer.parseInt(b);
        }

        public int getA() {
            return a;
        }

        public int getB() {
            return b;
        }

        public void setA(int a) {
            this.a = a;
        }

        public void setB(int b) {
            this.b = b;
        }

    }

    public enum enums {
        first, second, third
    }

    @Test(groups = { "simple" })
    public void getObjectAndCastToClass() {
        Document document = new Document();
        // numeric values
        document.set("intValue", Integer.MAX_VALUE);
        document.set("doubleValue", Double.MAX_VALUE);
        document.set("longValue", Long.MAX_VALUE);

        assertThat(document.getObject("intValue", Integer.class).intValue()).isEqualTo(Integer.MAX_VALUE);
        assertThat(document.getObject("doubleValue", Double.class).doubleValue()).isEqualTo(Double.MAX_VALUE);
        assertThat(document.getObject("longValue", Long.class).longValue()).isEqualTo(Long.MAX_VALUE);

        // string
        document.set("stringValue", "stringField");
        assertThat(document.getObject("stringValue", String.class)).isEqualTo("stringField");

        // boolean
        document.set("boolValue", true);
        assertThat(document.getObject("boolValue", Boolean.class)).isEqualTo(true);

        // enum
        document.set("enumValue", "third");
        assertThat(document.getObject("enumValue", enums.class)).isEqualTo(enums.third);

        // Pojo
        Pojo pojo = new Pojo(1, 2);
        document.set("pojoValue", pojo);
        Pojo readPojo = document.getObject("pojoValue", Pojo.class);
        assertThat(readPojo.getA()).isEqualTo(pojo.getA());
        assertThat(readPojo.getB()).isEqualTo(pojo.getB());

        // JsonSerializable
        Document innerDocument = new Document();
        innerDocument.setId("innerDocument");
        document.set("innerDocument", innerDocument);
        Document readInnerDocument = document.getObject("innerDocument", Document.class);
        assertThat(readInnerDocument.getId()).isEqualTo(innerDocument.getId());
    }
}
