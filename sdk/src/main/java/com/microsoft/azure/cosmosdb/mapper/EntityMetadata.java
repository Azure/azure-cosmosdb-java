/**
 * The MIT License (MIT)
 * Copyright (c) 2018 Microsoft Corporation
 * <p>
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * <p>
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * <p>
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.microsoft.azure.cosmosdb.mapper;

import java.util.Objects;

import static org.apache.commons.lang3.StringUtils.isBlank;

class EntityMetadata {

    private final String databaseName;

    private final String collectionName;

    EntityMetadata(String databaseName, String collectionName) {
        this.databaseName = databaseName;
        this.collectionName = collectionName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public String getCollectionLink() {
        return String.format("/dbs/%s/colls/%s", databaseName, collectionName);
    }

    public String getDocumentId(String id) {
        return String.format("/dbs/%s/colls/%s/docs/%s", databaseName, collectionName, id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof EntityMetadata)) {
            return false;
        }
        EntityMetadata that = (EntityMetadata) o;
        return Objects.equals(databaseName, that.databaseName) &&
                Objects.equals(collectionName, that.collectionName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, collectionName);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("EntityMetadata{");
        sb.append("databaseName='").append(databaseName).append('\'');
        sb.append(", collectionName='").append(collectionName).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public static <T> EntityMetadata of(Class<T> entityClass) {

        Entity entity = entityClass.getAnnotation(Entity.class);

        if(entity == null) {
            throw new IllegalArgumentException("The annotation com.microsoft.azure.cosmosdb.mapper.Entity is required at this class: " + entityClass);
        }

        if(isBlank(entity.databaseName())) {
            throw new IllegalArgumentException("The database field cannot be null at @Entity in the class: " + entityClass);
        }
        String databaseName = entity.databaseName();
        String collectionName = isBlank(entity.collectionName()) ? entityClass.getSimpleName()
                : entity.collectionName();
        return new EntityMetadata(databaseName, collectionName);
    }

}
