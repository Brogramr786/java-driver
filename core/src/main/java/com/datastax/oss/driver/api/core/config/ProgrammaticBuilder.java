/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.api.core.config;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * A builder that allows providing typed configuration tied to {@link DriverOption}.
 *
 * <p>Rarely would one ever deal with this type directly. This interface is useful for defining
 * default methods so implementors only have to implement a small set of methods.
 */
public interface ProgrammaticBuilder<SelfT extends ProgrammaticBuilder> {

  @NonNull
  default SelfT withBoolean(@NonNull DriverOption option, boolean value) {
    return with(option, value);
  }

  @NonNull
  default SelfT withBooleanList(@NonNull DriverOption option, @NonNull List<Boolean> value) {
    return with(option, value);
  }

  @NonNull
  default SelfT withInt(@NonNull DriverOption option, int value) {
    return with(option, value);
  }

  @NonNull
  default SelfT withIntList(@NonNull DriverOption option, @NonNull List<Integer> value) {
    return with(option, value);
  }

  @NonNull
  default SelfT withLong(@NonNull DriverOption option, long value) {
    return with(option, value);
  }

  @NonNull
  default SelfT withLongList(@NonNull DriverOption option, @NonNull List<Long> value) {
    return with(option, value);
  }

  @NonNull
  default SelfT withDouble(@NonNull DriverOption option, double value) {
    return with(option, value);
  }

  @NonNull
  default SelfT withDoubleList(@NonNull DriverOption option, @NonNull List<Double> value) {
    return with(option, value);
  }

  @NonNull
  default SelfT withString(@NonNull DriverOption option, @NonNull String value) {
    return with(option, value);
  }

  @NonNull
  default SelfT withStringList(@NonNull DriverOption option, @NonNull List<String> value) {
    return with(option, value);
  }

  @SuppressWarnings("unchecked")
  @NonNull
  default SelfT withStringMap(@NonNull DriverOption option, @NonNull Map<String, String> value) {
    SelfT v = (SelfT) this;
    for (String key : value.keySet()) {
      v = (SelfT) v.with(option.getPath() + "." + key, value.get(key));
    }
    return v;
  }

  /**
   * Specifies a size in bytes. This is separate from {@link #withLong(DriverOption, long)}, in case
   * implementations want to allow users to provide sizes in a more human-readable way, for example
   * "256 MB".
   */
  @NonNull
  default SelfT withBytes(@NonNull DriverOption option, @NonNull String value) {
    return with(option, value);
  }

  @NonNull
  default SelfT withBytesList(@NonNull DriverOption option, @NonNull List<Long> value) {
    return with(option, value);
  }

  @NonNull
  default SelfT withDuration(@NonNull DriverOption option, @NonNull Duration value) {
    return with(option, value);
  }

  @NonNull
  default SelfT withDurationList(@NonNull DriverOption option, @NonNull List<Duration> value) {
    return with(option, value);
  }

  @NonNull
  default SelfT withClass(@NonNull DriverOption option, @NonNull Class<?> value) {
    return with(option, value.getName());
  }

  /** Unsets an option. */
  @NonNull
  default SelfT without(@NonNull DriverOption option) {
    return with(option, null);
  }

  @NonNull
  default SelfT with(@NonNull DriverOption option, @NonNull Object value) {
    return with(option.getPath(), value);
  }

  /**
   * Provides a simple path to value mapping, all default methods invoke this method directly. It is
   * not recommended that it is used directly other than by these defaults.
   */
  @NonNull
  SelfT with(@NonNull String path, @NonNull Object value);
}