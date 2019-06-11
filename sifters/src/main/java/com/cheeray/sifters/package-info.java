/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Create a {@link com.cheeray.sifters.Sifter} to sift any numbers of raw
 * {@link com.cheeray.sifters.Gradable} targets into {@link com.cheeray.sifters.Bucket}s.
 * Each bucket accept a set of {@link com.cheeray.sifters.Grade}s and collect sift results
 * after a given delay set by system property <code>"sifter.collect.delay"</code> and time
 * unit <code>"sifter.collect.delay.unit"</code>.
 * <p>
 * Built-in memory throttle can pause data flow till configured free memory ratio is
 * satisfied. The minimum ratio is configured by system property
 * <code>"sifter.mem.free.percent"</code>, default is 30.
 * </p>
 * @author Chengwei.Yan
 */
package com.cheeray.sifters;