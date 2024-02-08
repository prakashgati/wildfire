/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.joins;

import com.google.common.base.Objects;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.jetbrains.annotations.NotNull;
import scala.Function1;
import scala.collection.Iterator;
import scala.collection.JavaConverters;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.catalyst.CatalystTypeConverters;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.bcvar.ArrayWrapper;
import org.apache.spark.sql.catalyst.bcvar.BroadcastedJoinKeysWrapper;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.LongType$;
import org.apache.spark.sql.types.ShortType$;


public class BroadcastedJoinKeysWrapperImpl implements BroadcastedJoinKeysWrapper {
  private Broadcast<HashedRelation> bcVar;

  private DataType[] keyDataTypes;

  private int relativeKeyIndexInArray = 0;

  private int[] indexesOfInterest;

  private transient volatile WeakReference<Object> keysArray = null;

  private int totalJoinKeys = 0;

  private static final LoadingCache<BroadcastedJoinKeysWrapperImpl, Set<Object>>
    idempotentializerForSet = CacheBuilder.newBuilder().expireAfterWrite(
     CACHE_EXPIRY, TimeUnit.SECONDS).maximumSize(CACHE_SIZE).weakValues().build(
          new CacheLoader<BroadcastedJoinKeysWrapperImpl, Set<Object>>() {
            public Set<Object> load(BroadcastedJoinKeysWrapperImpl bcjk) {
                BroadcastJoinKeysReaper.checkInitialized();
                ArrayWrapper<? extends Object> keys = bcjk.getKeysArray();
                int len = keys.getLength();
                Set<Object> set = new HashSet<>();
                for(int i = 0; i < len; ++i) {
                  set.add(keys.get(i));
                }
                return set;
              }
            });

  private static final LoadingCache<KeyIdempotForHashedRelationDeser, Object>
      idempotentializerForHashedRelationDeser =
      CacheBuilder.newBuilder().expireAfterWrite(CACHE_EXPIRY, TimeUnit.SECONDS)
      .maximumSize(CACHE_SIZE).weakValues().build(
          new CacheLoader<KeyIdempotForHashedRelationDeser, Object>() {
            // this will register the Reaper on the driver side as well as executor side to get
            // application life cycle events and removal of broadcast var event
            public Object load(KeyIdempotForHashedRelationDeser key){
              BroadcastJoinKeysReaper.checkInitialized();
              Broadcast<HashedRelation> bcVar = key.bcjk.bcVar;
              if (bcVar.getValue() instanceof LongHashedRelation) {
                LongHashedRelation lhr = (LongHashedRelation) bcVar.getValue();
                if (key.bcjk.totalJoinKeys == 1) {
                  if (key.bcjk.keyDataTypes[0].equals(LongType$.MODULE$)) {
                    return JavaConverters.asJavaCollection(lhr.keys().map(f -> f.get(
                        0, LongType$.MODULE$)).toList()).toArray();
                  } else if (key.bcjk.keyDataTypes[0].equals(IntegerType$.MODULE$)) {
                    return JavaConverters.asJavaCollection(lhr.keys().map(f -> ((Long)f.get(
                        0, LongType$.MODULE$)).intValue()).toList()).toArray();
                  } else if (key.bcjk.keyDataTypes[0].equals(ShortType$.MODULE$)) {
                    return JavaConverters.asJavaCollection(lhr.keys().map(f -> ((Long)f.get(
                        0, LongType$.MODULE$)).shortValue()).toList()).toArray();
                  } else {
                    return JavaConverters.asJavaCollection(lhr.keys().map(f -> ((Long)f.get(
                        0, LongType$.MODULE$)).byteValue()).toList()).toArray();
                  }
                } else {
                  if (key.bcjk.indexesOfInterest.length == 1) {
                    return JavaConverters.asJavaCollection(
                        lhr.keys().map(ir -> {
                          long hashedKey = ir.getLong(0);
                          int actualkey;
                          if (key.bcjk.indexesOfInterest[0] == 0) {
                            actualkey = (int) (hashedKey >> 32);
                          } else {
                            actualkey = (int) (hashedKey & 0xffffffffL);
                          }
                          if (key.bcjk.keyDataTypes[0].equals(IntegerType$.MODULE$)) {
                            return actualkey;
                          } else if (key.bcjk.keyDataTypes[0].equals(ShortType$.MODULE$)) {
                            return (short)actualkey;
                          } else {
                            return (byte)actualkey;
                          }
                        }).toList()).toArray();
                  } else {
                    return getObjects(lhr, key.bcjk);
                  }
                }
              } else {
                Iterator<InternalRow> keysIter = bcVar.getValue().keys();
                if (key.bcjk.indexesOfInterest.length == 1) {
                  int actualIndex = key.bcjk.indexesOfInterest[0];
                  DataType keyDataType = key.bcjk.keyDataTypes[0];
                  Function1<Object, Object> toScalaConverter =
                      CatalystTypeConverters.createToScalaConverter(keyDataType);
                  Iterator<Object> keysAsScala = keysIter.map(f -> {
                    Object x = f.get(actualIndex, keyDataType);
                    return toScalaConverter.apply(x);
                  });
                  return JavaConverters.asJavaCollection(keysAsScala.toList()).toArray();
                } else {
                  Function1<Object, Object>[] toScalaConverters =
                      new Function1[key.bcjk.indexesOfInterest.length];
                  for (int i = 0; i < key.bcjk.indexesOfInterest.length; ++i) {
                    DataType keyDataType = key.bcjk.keyDataTypes[i];
                    toScalaConverters[i] = CatalystTypeConverters.createToScalaConverter(
                        keyDataType);
                  }
                  Iterator<Object[]> keysAsScala = keysIter.map(f -> {
                    Object[] arr = new Object[key.bcjk.indexesOfInterest.length];
                    for (int i = 0; i < key.bcjk.indexesOfInterest.length; ++i) {
                      int actualIndex = key.bcjk.indexesOfInterest[i];
                      DataType keyDataType = key.bcjk.keyDataTypes[i];
                      Object x = f.get(actualIndex, keyDataType);
                      arr[i] = toScalaConverters[i].apply(x);
                    }
                    return arr;
                  });
                  return JavaConverters.asJavaCollection(keysAsScala.toList()).toArray(
                      new Object[0][]);
                }
              }
            }
          });

  private static Object[][] getObjects(LongHashedRelation lhr,
      BroadcastedJoinKeysWrapperImpl bcjk ) {
    int totalKeysPresent = bcjk.totalJoinKeys;
    final UnsafeRow unsafeRow = new UnsafeRow(totalKeysPresent);
    final ByteBuffer buff = ByteBuffer.allocate(8);
    Function1<Object, Object>[] toScalaConverters = new Function1[bcjk.indexesOfInterest.length];
    for (int i = 0; i < bcjk.indexesOfInterest.length; ++i) {
      DataType keyDataType = bcjk.keyDataTypes[i];
      toScalaConverters[i] = CatalystTypeConverters.createToScalaConverter(keyDataType);
    }
    return JavaConverters.asJavaCollection(
      lhr.keys().map(ir -> {
        long hashedKey = Long.reverse(ir.getLong(0));
        buff.putLong(0, hashedKey);
        byte[] arr = buff.array();
        unsafeRow.pointTo(arr, arr.length);
        Object[] actualkeys = new Object[bcjk.indexesOfInterest.length];
        for (int i = 0; i < bcjk.indexesOfInterest.length; ++i) {
          DataType keyDataType = bcjk.keyDataTypes[i];
          Object temp = unsafeRow.get(bcjk.indexesOfInterest[i], keyDataType);
          actualkeys[i] =  toScalaConverters[i].apply(temp);
        }
        return actualkeys;
      }).toList()).toArray(new Object[0][]);
  }

  public BroadcastedJoinKeysWrapperImpl() {}

  public BroadcastedJoinKeysWrapperImpl(Broadcast<HashedRelation> bcVar, DataType[] keyDataTypes,
      int relativeKeyIndexInArray, int[] indexArray, int totalJoinKeys) {
    this.bcVar = bcVar;
    this.keyDataTypes = keyDataTypes;
    this.relativeKeyIndexInArray = relativeKeyIndexInArray;
    this.indexesOfInterest = indexArray;
    this.totalJoinKeys = totalJoinKeys;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeObject(this.bcVar);
    out.writeInt(this.relativeKeyIndexInArray);
    out.writeInt(this.totalJoinKeys);
    out.writeObject(this.indexesOfInterest);
    out.writeObject(this.keyDataTypes);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    this.bcVar = (Broadcast<HashedRelation>) in.readObject();
    this.relativeKeyIndexInArray = in.readInt();
    this.totalJoinKeys = in.readInt();
    this.indexesOfInterest = (int[])in.readObject();
    this.keyDataTypes = (DataType[])in.readObject();
  }

  private Object initKeys() {
    Object actualArray;
    try {
      if (this.keysArray == null || (actualArray = this.keysArray.get()) == null) {
        actualArray =
            idempotentializerForHashedRelationDeser.get(
                new KeyIdempotForHashedRelationDeser(this));
        this.keysArray = new WeakReference<>(actualArray);
      }
      return actualArray;
    } catch (ExecutionException ee) {
      throw new RuntimeException(ee);
    }
  }

  public DataType getSingleKeyDataType() {
    return this.keyDataTypes[this.relativeKeyIndexInArray];
  }

  public ArrayWrapper<? extends Object> getKeysArray() {
    Object array = this.initKeys();
    return ArrayWrapper.wrapArray(array, this.indexesOfInterest.length == 1,
        this.relativeKeyIndexInArray);
  }

  public Set<Object> getKeysAsSet() {
    try {
      Set<Object> keyset = idempotentializerForSet.get(this);
      if (System.getProperty("debug", "false").equals("true")) {
        return new SetWrapper<>(keyset);
      } else {
        return keyset;
      }
    } catch(ExecutionException ee) {
      throw new RuntimeException(ee);
    }
  }

  @Override
  public boolean equals(Object other) {
    if (other != null) {
      return this == other || (other instanceof BroadcastedJoinKeysWrapperImpl
          && this.bcVar.id() == ((BroadcastedJoinKeysWrapperImpl) other).bcVar.id()
          && this.relativeKeyIndexInArray ==
          ((BroadcastedJoinKeysWrapperImpl) other).relativeKeyIndexInArray
      );
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(this.bcVar.id(), this.relativeKeyIndexInArray);
  }

  public long getBroadcastVarId() {
    return this.bcVar.id();
  }

  public int getRelativeKeyIndex() {
    return this.relativeKeyIndexInArray;
  }

  public int getTupleLength() {
    return this.indexesOfInterest.length;
  }

  public int getTotalJoinKeys() {
    return this.totalJoinKeys;
  }

  public HashedRelation getUnderlyingRelation() {
    return this.bcVar.getValue();
  }

  public Broadcast<HashedRelation> getUnderlyingBroadcastVar() {
    return this.bcVar;
  }

  @Override
  public void invalidateSelf() {
    removeBroadcast(this.bcVar.id());
  }

  static void removeBroadcast(long id) {
    idempotentializerForHashedRelationDeser.asMap().keySet().stream()
      .filter( key -> key.bcjk.getBroadcastVarId() == id).forEach(
            idempotentializerForHashedRelationDeser::invalidate);
    idempotentializerForSet.asMap().keySet().stream()
      .filter( bcVar -> bcVar.getBroadcastVarId() == id).forEach(
        idempotentializerForSet::invalidate);
  }

  static void invalidateBroadcastCache() {
    idempotentializerForSet.invalidateAll();
    idempotentializerForHashedRelationDeser.invalidateAll();
  }
}

class SetWrapper<T> implements Set<T> {
  private final Set<T> base;
  public SetWrapper(Set<T> base) {
    this.base = base;
  }
  @Override
  public java.util.Iterator<T> iterator() {
    return this.base.iterator();
  }

  @NotNull
  @Override
  public Object[] toArray() {
    return this.base.toArray();
  }

  @Override
  public boolean add(T o) {
    return this.base.add(o);
  }

  @Override
  public boolean remove(Object o) {
    return this.base.remove(o);
  }

  @Override
  public boolean addAll(@NotNull Collection<? extends T> c) {
    return this.base.addAll(c);
  }

  @Override
  public void clear() {
    this.base.clear();
  }

  @Override
  public boolean equals(Object o) {
    return this.base.equals(o);
  }

  @Override
  public int hashCode() {
    return this.base.hashCode();
  }

  @Override
  public boolean removeAll(@NotNull Collection<?> c) {
    return this.base.removeAll(c);
  }

  @Override
  public boolean retainAll(@NotNull Collection<?> c) {
    return this.base.retainAll(c);
  }

  @Override
  public boolean containsAll(@NotNull Collection<?> c) {
    return this.base.containsAll(c);
  }

  @NotNull
  @Override
  public <T> T[] toArray(T[] a) {
    return this.base.toArray(a);
  }

  @Override
  public int size() {
    return this.base.size();
  }

  @Override
  public boolean isEmpty() {
    return this.base.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return this.base.contains(o);
  }
}

class KeyIdempotForHashedRelationDeser {
  final BroadcastedJoinKeysWrapperImpl bcjk;

  public KeyIdempotForHashedRelationDeser(BroadcastedJoinKeysWrapperImpl bcjk) {
    this.bcjk = bcjk;
  }

  @Override
  public boolean equals(Object other) {
    if (other != null) {
      return this == other || (other instanceof KeyIdempotForHashedRelationDeser
          && this.bcjk.getBroadcastVarId() == ((KeyIdempotForHashedRelationDeser) other).
          bcjk.getBroadcastVarId());
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(this.bcjk.getBroadcastVarId());
  }
}